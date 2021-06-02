package org.hypertrace.viewgenerator.generators;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Entity;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.HexUtils;
import org.hypertrace.core.datamodel.shared.SpanAttributeUtils;
import org.hypertrace.entity.constants.v1.BackendAttribute;
import org.hypertrace.entity.service.constants.EntityConstants;
import org.hypertrace.semantic.convention.utils.http.HttpMigration;
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

  @Override
  List<ServiceCallView> generateView(
      StructuredTrace structuredTrace,
      Map<String, Entity> entityMap,
      Map<ByteBuffer, Event> eventMap,
      Map<ByteBuffer, List<ByteBuffer>> parentToChildrenEventIds,
      Map<ByteBuffer, ByteBuffer> childToParentEventId) {
    ApiTraceGraph apiTraceGraph = ViewGeneratorState.getApiTraceGraph(structuredTrace);
    List<ServiceCallView> serviceCallViewRecords = new ArrayList<>();

    // Scenario #1 Create edge corresponding to ApiNodeEdge
    createEdgeFromApiNodeEdge(apiTraceGraph, structuredTrace, serviceCallViewRecords);

    // Scenario #2 Handle Root entry events
    createEdgeFromRootEntryEvent(apiTraceGraph, structuredTrace, serviceCallViewRecords);

    // Scenario #3 Handle Backends
    createEdgeFromBackend(apiTraceGraph, structuredTrace, serviceCallViewRecords);

    // Scenario #4: Handle non entry roots
    createEdgeFromNonRootEntryEvent(
        apiTraceGraph, structuredTrace, eventMap, childToParentEventId, serviceCallViewRecords);

    if (LOG.isTraceEnabled()) {
      LOG.trace(
          "Generated ServiceCalls for the structuredTrace: {}. serviceCalls: {}",
          structuredTrace,
          serviceCallViewRecords);
    }

    return serviceCallViewRecords;
  }

  /**
   * Go through the apiNode edges and create a record for each edge. Should be easy to get to the
   * events since the ApiNodeEventEdge has both the indices in the ApiNode list and Events list in
   * the trace
   */
  void createEdgeFromApiNodeEdge(
      ApiTraceGraph apiTraceGraph,
      StructuredTrace structuredTrace,
      List<ServiceCallView> serviceCallViewRecords) {
    apiTraceGraph
        .getApiNodeEventEdgeList()
        .forEach(
            edge ->
                serviceCallViewRecords.add(
                    createGenericRecordFromApiNodeEdge(
                        structuredTrace,
                        structuredTrace.getEventList().get(edge.getSrcEventIndex()),
                        structuredTrace.getEventList().get(edge.getTgtEventIndex()))));
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

  /**
   * Method to cherry-pick the fields that should be coming from an Entry span into the service call
   * view.
   */
  private void buildEntrySpanView(
      Event event, ServiceCallView.Builder builder, @Nullable Protocol protocol) {
    builder.setServerEventId(event.getEventId());
    builder.setCalleeService(EnrichedSpanUtils.getServiceName(event));
    builder.setCalleeSpaceIds(EnrichedSpanUtils.getSpaceIds(event));
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
      //      Optional<Request> httpRequest =
      //          Optional.ofNullable(event.getHttp())
      //              .map(org.hypertrace.core.datamodel.eventfields.http.Http::getRequest);
      // Set request related attributes.
      builder.setRequestUrl(HttpMigration.getHttpUrl(event).orElse(null));
      //      builder.setRequestUrl(httpRequest.map(Request::getUrl).orElse(null));
      builder.setRequestMethod(HttpMigration.getHttpMethod(event).orElse(null));
      //      builder.setRequestMethod(httpRequest.map(Request::getMethod).orElse(null));
    }

    String statusValue = EnrichedSpanUtils.getStatusCode(event);
    if (StringUtils.isNumeric(statusValue)) {
      builder.setResponseStatusCode(Integer.parseInt(statusValue));
    }
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

  private void buildExitSpanView(Event event, ServiceCallView.Builder builder) {
    builder.setCallerService(EnrichedSpanUtils.getServiceName(event));
    builder.setCallerSpaceIds(EnrichedSpanUtils.getSpaceIds(event));
    String serviceId = EnrichedSpanUtils.getServiceId(event);
    if (serviceId != null) {
      builder.setCallerServiceIdStr(serviceId);
    }

    builder.setClientEventId(event.getEventId());

    builder.setCallerApi(EnrichedSpanUtils.getApiName(event));
    builder.setCallerApiIdStr(EnrichedSpanUtils.getApiId(event));
  }

  /**
   * Handle root entry events. Ideally there should only be one root entry. Root entry spans are
   * those entry api boundary spans that are not the target of any {@link
   * org.hypertrace.core.datamodel.ApiNodeEventEdge} i.e Root entry spans = All Entry API Boundary
   * Spans set - Set of Entry API Boundary spans that are the target of an edge({@code
   * calleeEntryEvents} computed below)
   */
  void createEdgeFromRootEntryEvent(
      ApiTraceGraph apiTraceGraph,
      StructuredTrace structuredTrace,
      List<ServiceCallView> serviceCallViewRecords) {
    int serviceCallViewRecordsCount = serviceCallViewRecords.size();

    apiTraceGraph
        .getApiEntryBoundaryEventsWithNoIncomingEdge()
        .forEach(
            entryEvent ->
                serviceCallViewRecords.add(
                    getServiceCallFromSingleEntryEvent(structuredTrace, entryEvent)));

    // Log warning if the trace has multiple root entries. A trace should only ever have one root
    // entry
    if (serviceCallViewRecords.size() - serviceCallViewRecordsCount > 1) {
      LOG.warn(
          "Multiple root entries for trace: {}", HexUtils.getHex(structuredTrace.getTraceId()));
    }
  }

  private ServiceCallView getServiceCallFromSingleEntryEvent(StructuredTrace trace, Event event) {
    ServiceCallView.Builder builder = createAndInitializeBuilder(trace);
    buildCommonServiceCallView(event, builder);

    Protocol protocol = EnrichedSpanUtils.getProtocol(event);
    buildEntrySpanView(event, builder, protocol);

    return builder.build();
  }

  /**
   * Scenario #3:Handle backends Backends are exit api boundaries that are not the source of any
   * {@link org.hypertrace.core.datamodel.ApiNodeEventEdge} i.e Backends spans = All Exit Api
   * Boundary spans set - Set of Exit API Boundaries that are the source of an edge({@code
   * callerExitEvents} computed below)
   */
  void createEdgeFromBackend(
      ApiTraceGraph apiTraceGraph,
      StructuredTrace structuredTrace,
      List<ServiceCallView> serviceCallViewRecords) {
    apiTraceGraph
        .getApiExitBoundaryEventsWithNoOutgoingEdge()
        .forEach(
            exitEvent ->
                serviceCallViewRecords.add(
                    getServiceCallFromSingleExitEvent(structuredTrace, exitEvent)));
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

  /**
   * Handle API Nodes without an entry boundary span but with an exit/exits boundaries. For this
   * scenario we want to create a ServiceCallView record for the root ancestors of each of the
   * exits. We will be careful not to double count cases where trace is broken and there are spans
   * with a parent span ID but not corresponding parent span object in the trace (broken trace)
   */
  void createEdgeFromNonRootEntryEvent(
      ApiTraceGraph apiTraceGraph,
      StructuredTrace structuredTrace,
      Map<ByteBuffer, Event> eventMap,
      Map<ByteBuffer, ByteBuffer> childToParentEventId,
      List<ServiceCallView> serviceCallViewRecords) {
    apiTraceGraph.getApiNodeList().stream()
        .filter(apiNode -> apiNode.getEntryApiBoundaryEvent().isEmpty()) // No entry api boundary
        .flatMap(apiNode -> apiNode.getExitApiBoundaryEvents().stream())
        .map(exitEvent -> getRootAncestor(exitEvent, childToParentEventId, eventMap))
        .distinct() // Make sure that we create a record for each root that makes multiple
        // exits. We don't want to double count root span calls.
        .filter(
            rootEvent ->
                rootEvent
                    .getEventRefList()
                    .isEmpty()) // Real roots and not broken spans with a parent span Id whose
        // corresponding span object is not part of the trace.
        .forEach(
            rootEvent ->
                serviceCallViewRecords.add(
                    createViewForNonEntryRootSpan(structuredTrace, rootEvent)));
  }

  private Event getRootAncestor(
      Event descendant,
      final Map<ByteBuffer, ByteBuffer> childToParentEventId,
      final Map<ByteBuffer, Event> eventMap) {
    Event rootAncestor = descendant;
    while (rootAncestor != null) {
      Event parentSpan =
          SpanAttributeUtils.getParentSpan(rootAncestor, childToParentEventId, eventMap);
      if (parentSpan == null) {
        break;
      }
      rootAncestor = parentSpan;
    }

    return rootAncestor;
  }

  private ServiceCallView createViewForNonEntryRootSpan(StructuredTrace trace, Event event) {
    ServiceCallView.Builder builder = createAndInitializeBuilder(trace);
    Protocol protocol = EnrichedSpanUtils.getProtocol(event);
    buildCommonServiceCallView(event, builder);

    builder.setServerEventId(event.getEventId());
    builder.setCalleeSpaceIds(EnrichedSpanUtils.getSpaceIds(event));
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

  /**
   * For error count, to avoid double counting errors when both source and target has an error, we
   * return 1 if either source or target has an error.
   */
  private int getServiceCallViewErrorCount(double errorCount) {
    return errorCount > 0.0d ? 1 : 0;
  }
}
