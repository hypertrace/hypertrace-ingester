package org.hypertrace.viewgenerator.generators;

import static org.hypertrace.core.datamodel.shared.SpanAttributeUtils.getStringAttributeWithDefault;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Entity;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.eventfields.http.Http;
import org.hypertrace.core.datamodel.eventfields.http.Request;
import org.hypertrace.core.datamodel.shared.ApiNode;
import org.hypertrace.core.datamodel.shared.HexUtils;
import org.hypertrace.core.datamodel.shared.SpanAttributeUtils;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.entity.constants.v1.BackendAttribute;
import org.hypertrace.entity.service.constants.EntityConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.utils.EnrichedSpanUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.ErrorMetrics;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Protocol;
import org.hypertrace.traceenricher.trace.util.ApiTraceGraph;
import org.hypertrace.viewgenerator.api.ServiceCallView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServiceCallViewGenerator extends BaseViewGenerator<ServiceCallView> {

  private static final Logger LOG = LoggerFactory.getLogger(ServiceCallViewGenerator.class);
  private static final String BACKEND_PROTOCOL_ATTR =
      EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_PROTOCOL);
  private static final String BACKEND_PATH_ATTR =
      EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_PATH);
  private static final String BACKEND_HOST_ATTR =
      EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_HOST);
  private static final String BACKEND_PORT_ATTR =
      EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_PORT);
  private static final Set<String> VALID_PROTOCOLS;

  static {
    VALID_PROTOCOLS = new HashSet<>();
    for (Protocol p : Protocol.values()) {
      if (p != Protocol.UNRECOGNIZED && p != Protocol.PROTOCOL_UNSPECIFIED) {
        VALID_PROTOCOLS.add(EnrichedSpanConstants.getValue(p));
      }
    }
  }

  @Override
  List<ServiceCallView> generateView(
      StructuredTrace structuredTrace,
      Map<String, Entity> entityMap,
      Map<ByteBuffer, Event> eventMap,
      Map<ByteBuffer, List<ByteBuffer>> parentToChildrenEventIds,
      Map<ByteBuffer, ByteBuffer> childToParentEventIds) {
    ApiTraceGraph apiTraceGraph = new ApiTraceGraph(structuredTrace).build();

    // Scenario #1: Go through the apiNode edges and create a record for each edge. Should be easy
    // to get to the events
    // since the ApiNodeEventEdge has both the indices in the ApiNode list and Events list in the
    // trace.
    List<ServiceCallView> serviceCallViewsFromEdges =
        apiTraceGraph.getApiNodeEventEdgeList().stream()
            .map(
                edge ->
                    createGenericRecordFromApiNodeEdge(
                        structuredTrace,
                        structuredTrace.getEventList().get(edge.getSrcEventIndex()),
                        structuredTrace.getEventList().get(edge.getTgtEventIndex())))
            .collect(Collectors.toUnmodifiableList());

    List<ServiceCallView> serviceCallViewRecords = new ArrayList<>(serviceCallViewsFromEdges);

    // Compute the Entry API Boundary spans that are the target of an edge.
    Set<ByteBuffer> calleeEntryEvents =
        apiTraceGraph.getApiNodeEventEdgeList().stream()
            .map(edge -> structuredTrace.getEventList().get(edge.getTgtEventIndex()).getEventId())
            .collect(Collectors.toSet());

    // Compute the Exit API Boundary spans that are the source of an edge
    Set<ByteBuffer> callerExitEvents =
        apiTraceGraph.getApiNodeEventEdgeList().stream()
            .map(edge -> structuredTrace.getEventList().get(edge.getSrcEventIndex()).getEventId())
            .collect(Collectors.toSet());

    // Scenario #2: Handle root entry points. Ideally there should only be one root entry.
    // Root entry spans are those entry boundary spans not in set of entry boundaries that are the
    // target of an edge in the
    // ApiNodeEventEdge list.
    // i.e Root entry spans = All Entry API Boundary Spans set - Set of Entry API Boundary spans
    // that are the target of an edge(calleeEntryEvents computed above)
    List<ServiceCallView> serviceCallViewsForRootEntries =
        apiTraceGraph.getNodeList().stream()
            .map(ApiNode::getEntryApiBoundaryEvent)
            .filter(
                apiBoundaryEvent ->
                    apiBoundaryEvent.isPresent()
                        && !calleeEntryEvents.contains(apiBoundaryEvent.get().getEventId()))
            .map(
                entryEvent -> getServiceCallFromSingleEntryEvent(structuredTrace, entryEvent.get()))
            .collect(Collectors.toUnmodifiableList());

    serviceCallViewRecords.addAll(serviceCallViewsForRootEntries);

    // Log warning if the trace has multiple root entries. A trace should only ever have one root
    // entry.
    if (serviceCallViewsForRootEntries.size() > 1) {
      LOG.warn(
          "Multiple root entries for trace: {}", HexUtils.getHex(structuredTrace.getTraceId()));
    }

    // Scenario #3: Handle backends
    // Backends are exit boundaries that are not in the set of exit boundaries that are the source
    // of an edge in the
    // ApiNodeEventEdge list.
    // i.e Backends spans = All Exit Boundary spans set - Set of Exit API Boundaries that are the
    // source of an edge(callerExitEvents computed above)
    List<ServiceCallView> serviceCallViewsForBackends =
        apiTraceGraph.getNodeList().stream()
            .flatMap(apiNode -> apiNode.getExitApiBoundaryEvents().stream())
            .filter(exitEvent -> !callerExitEvents.contains(exitEvent.getEventId()))
            .map(exitEvent -> getServiceCallFromSingleExitEvent(structuredTrace, exitEvent))
            .collect(Collectors.toUnmodifiableList());

    serviceCallViewRecords.addAll(serviceCallViewsForBackends);

    // Scenario #4: Handle non entry point roots
    // Handle API Nodes without an entry boundary span but with an exit/exits boundaries. For this
    // scenario we want to
    // create a ServiceCallView record for the root ancestors of each of the exits. We will be
    // careful not to double
    // count cases where trace is broken and there are spans with a parent span ID but not
    // corresponding parent span
    // object in the trace. (broken trace.)
    List<ServiceCallView> serviceCallViewsForExitsThatAreRoots =
        apiTraceGraph.getNodeList().stream()
            .filter(
                apiNode -> apiNode.getEntryApiBoundaryEvent().isEmpty()) // No entry api boundary
            .flatMap(apiNode -> apiNode.getExitApiBoundaryEvents().stream())
            .map(exitEvent -> getRootAncestor(exitEvent, childToParentEventIds, eventMap))
            .distinct() // Make sure that we create a record for each root that makes multiple
            // exits. We don't want to double count root span calls.
            .filter(
                rootEvent ->
                    rootEvent
                        .getEventRefList()
                        .isEmpty()) // Real roots and not broken spans with a parent span Id whose
            // corresponding span object is not part of the trace.
            .map(rootEvent -> createViewForNonEntryRootSpan(structuredTrace, rootEvent))
            .collect(Collectors.toUnmodifiableList());

    serviceCallViewRecords.addAll(serviceCallViewsForExitsThatAreRoots);

    if (LOG.isTraceEnabled()) {
      LOG.trace(
          "Generated ServiceCalls for the structuredTrace: {}. serviceCalls: {}",
          structuredTrace,
          serviceCallViewRecords);
    }

    return serviceCallViewRecords;
  }

  @Override
  public String getViewName() {
    return ServiceCallView.class.getName();
  }

  @Override
  public Schema getSchema() {
    return ServiceCallView.getClassSchema();
  }

  @Override
  public Class<ServiceCallView> getViewClass() {
    return ServiceCallView.class;
  }

  private ServiceCallView getServiceCallFromSingleExitEvent(StructuredTrace trace, Event event) {
    ServiceCallView.Builder builder = createAndInitializeBuilder(trace);
    buildCommonServiceCallView(event, builder);

    Protocol protocol = EnrichedSpanUtils.getProtocol(event);
    buildExitSpanView(event, builder);

    // If the call was made into a backend, populate Backend specific data on the view.
    if (event
        .getEnrichedAttributes()
        .getAttributeMap()
        .containsKey(EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_ID))) {
      buildExitSpanViewForBackendEntity(event, builder, protocol);
    }

    return builder.build();
  }

  private ServiceCallView getServiceCallFromSingleEntryEvent(StructuredTrace trace, Event event) {
    ServiceCallView.Builder builder = createAndInitializeBuilder(trace);
    buildCommonServiceCallView(event, builder);

    Protocol protocol = EnrichedSpanUtils.getProtocol(event);
    buildEntrySpanView(event, builder, protocol);

    return builder.build();
  }

  private void buildCommonServiceCallView(Event event, ServiceCallView.Builder builder) {
    int exceptionCount =
        (int)
            getMetricValue(
                event,
                EnrichedSpanConstants.getValue(ErrorMetrics.ERROR_METRICS_EXCEPTION_COUNT),
                0.0d);
    int errorCount =
        getServiceCallViewErrorCount(
            (int)
                getMetricValue(
                    event,
                    EnrichedSpanConstants.getValue(ErrorMetrics.ERROR_METRICS_ERROR_COUNT),
                    0.0d));
    buildCommonServiceCallView(event, builder, exceptionCount, errorCount);
  }

  private void buildCommonServiceCallView(
      Event event, ServiceCallView.Builder builder, int exceptionCount, int errorCount) {
    builder.setExceptionCount(exceptionCount);
    builder.setErrorCount(errorCount);

    builder.setStartTimeMillis(event.getStartTimeMillis());
    builder.setEndTimeMillis(event.getEndTimeMillis());
    builder.setDurationMillis(event.getEndTimeMillis() - event.getStartTimeMillis());
  }

  private void buildExitSpanViewForBackendEntity(
      Event event, ServiceCallView.Builder builder, Protocol protocol) {
    // Fill callee info
    builder.setCalleeBackendId(EnrichedSpanUtils.getBackendId(event));
    builder.setCalleeBackendName(EnrichedSpanUtils.getBackendName(event));

    final Map<String, AttributeValue> enrichedAttributeMap =
        event.getEnrichedAttributes().getAttributeMap();

    if (protocol != null) {
      builder.setProtocolName(EnrichedSpanConstants.getValue(protocol));
    }

    String requestUrl = getBackendRequestUrl(enrichedAttributeMap);
    builder.setRequestUrl(requestUrl);

    String statusValue = EnrichedSpanUtils.getStatusCode(event);
    if (StringUtils.isNumeric(statusValue)) {
      builder.setResponseStatusCode(Integer.parseInt(statusValue));
    }
  }

  private String getBackendRequestUrl(Map<String, AttributeValue> attributeMap) {
    StringBuilder sb = new StringBuilder();
    if (attributeMap.containsKey(BACKEND_PROTOCOL_ATTR)) {
      sb.append(attributeMap.get(BACKEND_PROTOCOL_ATTR).getValue());
      sb.append("://");
    }

    if (attributeMap.containsKey(BACKEND_HOST_ATTR)) {
      sb.append(attributeMap.get(BACKEND_HOST_ATTR).getValue());
    }

    sb.append(':');

    if (attributeMap.containsKey(BACKEND_PORT_ATTR)) {
      sb.append(attributeMap.get(BACKEND_PORT_ATTR).getValue());
    }

    if (attributeMap.containsKey(BACKEND_PATH_ATTR)) {
      sb.append(attributeMap.get(BACKEND_PATH_ATTR).getValue());
    }

    return sb.toString();
  }

  private void buildExitSpanView(Event event, ServiceCallView.Builder builder) {
    builder.setCallerService(EnrichedSpanUtils.getServiceName(event));
    String serviceId = EnrichedSpanUtils.getServiceId(event);
    if (serviceId != null) {
      builder.setCallerServiceIdStr(serviceId);
    }

    builder.setClientEventId(event.getEventId());

    builder.setCallerApi(EnrichedSpanUtils.getApiName(event));
    builder.setCallerApiIdStr(EnrichedSpanUtils.getApiId(event));
  }

  /**
   * Method to cherry-pick the fields that should be coming from an Entry span into the service call
   * view.
   */
  private void buildEntrySpanView(
      Event event, ServiceCallView.Builder builder, @Nullable Protocol protocol) {
    builder.setServerEventId(event.getEventId());
    builder.setCalleeService(EnrichedSpanUtils.getServiceName(event));
    String serviceId = EnrichedSpanUtils.getServiceId(event);
    if (serviceId != null) {
      builder.setCalleeServiceIdStr(serviceId);
    }

    builder.setCalleeApi(EnrichedSpanUtils.getApiName(event));
    builder.setCalleeApiIdStr(EnrichedSpanUtils.getApiId(event));

    if (protocol != null) {
      builder.setProtocolName(EnrichedSpanConstants.getValue(protocol));
    }

    // If this is a HTTP request, parse the http response status code.
    if (protocol == Protocol.PROTOCOL_HTTP || protocol == Protocol.PROTOCOL_HTTPS) {
      Optional<Request> httpRequest = Optional.ofNullable(event.getHttp())
          .map(org.hypertrace.core.datamodel.eventfields.http.Http::getRequest);
      // Set request related attributes.
      builder.setRequestUrl(httpRequest.map(Request::getUrl).orElse(null));
      builder.setRequestMethod(httpRequest.map(Request::getMethod).orElse(null));
    }

    String statusValue = EnrichedSpanUtils.getStatusCode(event);
    if (StringUtils.isNumeric(statusValue)) {
      builder.setResponseStatusCode(Integer.parseInt(statusValue));
    }
  }

  private ServiceCallView createViewForNonEntryRootSpan(StructuredTrace trace, Event event) {
    ServiceCallView.Builder builder = createAndInitializeBuilder(trace);
    Protocol protocol = EnrichedSpanUtils.getProtocol(event);
    buildCommonServiceCallView(event, builder);

    builder.setServerEventId(event.getEventId());
    builder.setCalleeService(EnrichedSpanUtils.getServiceName(event));
    String serviceId = EnrichedSpanUtils.getServiceId(event);
    if (serviceId != null) {
      builder.setCalleeServiceIdStr(serviceId);
    }

    if (protocol != null) {
      builder.setProtocolName(EnrichedSpanConstants.getValue(protocol));
    }

    builder.setErrorCount(
        getServiceCallViewErrorCount(
            getMetricValue(
                event,
                EnrichedSpanConstants.getValue(ErrorMetrics.ERROR_METRICS_ERROR_COUNT),
                0.0d)));

    return builder.build();
  }

  /**
   * This method assumes that "source" event has API_BOUNDARY_TYPE=EXIT and "target" event has
   * API_BOUNDARY_TYPE=ENTRY which is what is expected of the edges in the ApiTraceGraph
   */
  private ServiceCallView createGenericRecordFromApiNodeEdge(
      StructuredTrace trace, Event source, Event target) {
    ServiceCallView.Builder builder = createAndInitializeBuilder(trace);

    // Edge exception count = source exception count + target exception count
    int exceptionCount =
        (int)
            (getMetricValue(
                source,
                EnrichedSpanConstants.getValue(ErrorMetrics.ERROR_METRICS_EXCEPTION_COUNT),
                0.0d)
                + getMetricValue(
                target,
                EnrichedSpanConstants.getValue(ErrorMetrics.ERROR_METRICS_EXCEPTION_COUNT),
                0.0d));

    // Edge error count: 1 if either source or target has an error, 0 otherwise
    int errorCount =
        getServiceCallViewErrorCount(
            getMetricValue(
                source,
                EnrichedSpanConstants.getValue(ErrorMetrics.ERROR_METRICS_ERROR_COUNT),
                0.0d)
                + getMetricValue(
                target,
                EnrichedSpanConstants.getValue(ErrorMetrics.ERROR_METRICS_ERROR_COUNT),
                0.0d));

    Protocol protocol = EnrichedSpanUtils.getProtocol(target);
    // Set the server side attributes.
    buildEntrySpanView(target, builder, protocol);
    buildCommonServiceCallView(target, builder, exceptionCount, errorCount);
    // Set the client side attributes
    buildExitSpanView(source, builder);

    return builder.build();
  }

  private ServiceCallView.Builder createAndInitializeBuilder(StructuredTrace trace) {
    ServiceCallView.Builder builder = ServiceCallView.newBuilder();
    addTraceCommonAttributes(trace, builder);

    return builder;
  }

  private void addTraceCommonAttributes(StructuredTrace trace, ServiceCallView.Builder builder) {
    builder.setTenantId(trace.getCustomerId());
    builder.setTraceId(trace.getTraceId());
    builder.setTransactionName(getTransactionName(trace));
  }

  private Event getRootAncestor(
      Event descendant,
      final Map<ByteBuffer, ByteBuffer> childToParentEventIds,
      final Map<ByteBuffer, Event> eventMap) {
    Event rootAncestor = descendant;
    while (rootAncestor != null) {
      Event parentSpan =
          SpanAttributeUtils.getParentSpan(rootAncestor, childToParentEventIds, eventMap);
      if (parentSpan == null) {
        break;
      }
      rootAncestor = parentSpan;
    }

    return rootAncestor;
  }

  /**
   * For error count, to avoid double counting errors when both source and target has an error, we
   * return 1 if either source or target has an error.
   */
  private int getServiceCallViewErrorCount(double errorCount) {
    return errorCount > 0.0d ? 1 : 0;
  }
}
