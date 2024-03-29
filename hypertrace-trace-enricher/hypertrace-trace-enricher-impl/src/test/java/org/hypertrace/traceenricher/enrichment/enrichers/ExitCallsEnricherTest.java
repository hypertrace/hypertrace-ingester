package org.hypertrace.traceenricher.enrichment.enrichers;

import static org.hypertrace.traceenricher.enrichment.enrichers.TestUtils.assertTraceDoesNotContainAttribute;
import static org.hypertrace.traceenricher.enrichment.enrichers.TestUtils.createEntryEventWithName;
import static org.hypertrace.traceenricher.enrichment.enrichers.TestUtils.createExitEventName;
import static org.hypertrace.traceenricher.enrichment.enrichers.TestUtils.createTraceWithEventsAndEdges;
import static org.hypertrace.traceenricher.enrichment.enrichers.TestUtils.createUnspecifiedTypeEventWithName;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.ApiNode;
import org.hypertrace.core.datamodel.shared.SpanAttributeUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.utils.EnrichedSpanUtils;
import org.hypertrace.traceenricher.enrichment.enrichers.ExitCallsEnricher.ApiExitCallInfo;
import org.hypertrace.traceenricher.trace.util.ApiTraceGraph;
import org.junit.jupiter.api.Test;

public class ExitCallsEnricherTest {
  private static final String API_EXIT_CALLS_COUNT = "api.exit.calls.count";

  @Test
  public void testEnrichTrace_HotrodTrace() throws IOException {
    URL resource =
        Thread.currentThread().getContextClassLoader().getResource("StructuredTrace-Hotrod.avro");

    SpecificDatumReader<StructuredTrace> datumReader =
        new SpecificDatumReader<>(StructuredTrace.getClassSchema());
    DataFileReader<StructuredTrace> dfrStructuredTrace =
        new DataFileReader<>(new File(resource.getPath()), datumReader);
    StructuredTrace trace = dfrStructuredTrace.next();
    dfrStructuredTrace.close();
    ExitCallsEnricher exitCallsEnricher = new ExitCallsEnricher();
    exitCallsEnricher.enrichTrace(trace);
    verifyComputeApiExitInfo_HotrodTrace(trace, exitCallsEnricher);
  }

  private void verifyComputeApiExitInfo_HotrodTrace(
      StructuredTrace trace, ExitCallsEnricher exitCallsEnricher) {
    ApiTraceGraph apiTraceGraph = new ApiTraceGraph(trace);
    // this trace has 12 api nodes
    // api edges
    // 0 -> [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    // backend exit
    // 1 -> to redis 13 exit calls
    // 2 -> to mysql 1 exit call
    // for events parts of api_node 0, there should 12 exit calls
    // for events parts of api_node 1, there should be 13 exit calls
    // for events parts of api_node 2, there should be 1 exit calls
    Map<Integer, Integer> apiNodeToExitCallCount = Map.of(0, 12, 1, 13, 2, 1);
    Map<Integer, Map<String, String>> apiNodeToExitServices =
        Map.of(
            0,
            Map.of(
                "route", "10",
                "driver", "1",
                "customer", "1"),
            1,
            Map.of(
                "redis", "11",
                "unknown-backend", "2"),
            2,
            Map.of("unknown-backend", "1"));
    Map<ByteBuffer, Integer> eventToApiNodeIndex = buildEventIdToApiNode(apiTraceGraph);
    trace
        .getEventList()
        .forEach(
            e -> {
              Integer apiNodeIndex = eventToApiNodeIndex.get(e.getEventId());
              if (null != apiNodeIndex) {
                assertEquals(
                    apiNodeToExitServices.getOrDefault(apiNodeIndex, Maps.newHashMap()),
                    SpanAttributeUtils.getAttributeValue(
                            e, EnrichedSpanConstants.API_CALLEE_NAME_COUNT_ATTRIBUTE)
                        .getValueMap());
                assertEquals(
                    apiNodeToExitCallCount.getOrDefault(apiNodeIndex, 0),
                    Integer.parseInt(
                        SpanAttributeUtils.getStringAttribute(
                            e, EnrichedSpanConstants.API_EXIT_CALLS_ATTRIBUTE)));
              }
            });

    Map<ByteBuffer, ApiExitCallInfo> eventToApiExitInfo =
        exitCallsEnricher.computeApiExitCallCount(trace);
    // verify exit call count per service per api_trace
    // this trace has 4 services
    // frontend service has 1 api_entry span and that api_node has 12 exit calls [drive: 1,
    // customer: 1, route: 10]
    List<Event> events = getApiEntryEventsForService(trace, "frontend");
    assertEquals(1, events.size());
    assertEquals(12, eventToApiExitInfo.get(events.get(0).getEventId()).getExitCallCount());

    // customer service has 1 api_entry span and that api_node has 1 exit call to mysql
    events = getApiEntryEventsForService(trace, "customer");
    assertEquals(1, events.size());
    assertEquals(1, eventToApiExitInfo.get(events.get(0).getEventId()).getExitCallCount());

    // driver service has 1 api_entry span and that api_node has 13 exit call redis
    events = getApiEntryEventsForService(trace, "driver");
    assertEquals(1, events.size());
    assertEquals(13, eventToApiExitInfo.get(events.get(0).getEventId()).getExitCallCount());

    // route service has 10 api_entry span and all of them have 0 exit calls
    events = getApiEntryEventsForService(trace, "route");
    assertEquals(10, events.size());
    events.forEach(v -> assertEquals(0, eventToApiExitInfo.get(v.getEventId()).getExitCallCount()));
  }

  private List<Event> getApiEntryEventsForService(StructuredTrace trace, String serviceName) {
    return trace.getEventList().stream()
        .filter(EnrichedSpanUtils::isEntryApiBoundary)
        .filter(v -> serviceName.equals(v.getServiceName()))
        .collect(Collectors.toList());
  }

  private Map<ByteBuffer, Integer> buildEventIdToApiNode(ApiTraceGraph apiTraceGraph) {
    Map<ByteBuffer, Integer> map = Maps.newHashMap();
    for (int index = 0; index < apiTraceGraph.getApiNodeList().size(); index++) {
      ApiNode<Event> apiNode = apiTraceGraph.getApiNodeList().get(index);
      int finalIndex = index;
      apiNode.getEvents().forEach(v -> map.put(v.getEventId(), finalIndex));
    }
    return map;
  }

  @Test
  void apiExitCallsIsAvailableInTraceAttribute() {
    Event yEvent = createUnspecifiedTypeEventWithName("yEvent"); // 0
    Event zEvent = createUnspecifiedTypeEventWithName("zEvent"); // 1
    Event aEntryHeadSpanEvent = createEntryEventWithName("aEvent"); // 2
    Event aExitEvent = createExitEventName("aExitEvent"); // 3
    Event aExitEvent1 = createExitEventName("aExitEvent1"); // 4
    Event aExitEvent2 = createExitEventName("aExitEvent2"); // 5
    Event bEntryEvent = createEntryEventWithName("bEvent"); // 6

    Event[] allEvents =
        new Event[] {
          yEvent, zEvent, aEntryHeadSpanEvent, aExitEvent, aExitEvent1, aExitEvent2, bEntryEvent
        };
    HashMap<Integer, int[]> eventEdges =
        new HashMap<>() {
          {
            put(0, new int[] {1});
            put(1, new int[] {2});
            put(2, new int[] {3, 4, 5});
            put(3, new int[] {6});
          }
        };

    StructuredTrace trace = createTraceWithEventsAndEdges(allEvents, eventEdges);

    ExitCallsEnricher exitCallsEnricher = new ExitCallsEnricher();
    exitCallsEnricher.enrichTrace(trace);

    String actualTotalNumberOfCalls =
        trace.getAttributes().getAttributeMap().get(API_EXIT_CALLS_COUNT).getValue();

    assertEquals("3", actualTotalNumberOfCalls);
  }

  @Test
  void apiExitCallsCountAttributeNotAddedIfThereIsOnlyOneEventAndNoApiNodes() {
    Event aEntryEvent = createUnspecifiedTypeEventWithName("aEvent"); // 0

    Event[] allEvents = new Event[] {aEntryEvent};

    StructuredTrace trace = createTraceWithEventsAndEdges(allEvents, Collections.emptyMap());

    ExitCallsEnricher exitCallsEnricher = new ExitCallsEnricher();
    exitCallsEnricher.enrichTrace(trace);

    assertTraceDoesNotContainAttribute(trace, API_EXIT_CALLS_COUNT);
  }

  @Test
  void apiExitCallsCountAttributeNotAddedIfThereAreEventsAndNoApiNodes() {
    Event aEntryEvent = createUnspecifiedTypeEventWithName("aEvent"); // 0
    Event bEntryEvent = createUnspecifiedTypeEventWithName("bEvent"); // 1

    Event[] allEvents = new Event[] {aEntryEvent, bEntryEvent};
    HashMap<Integer, int[]> eventEdges =
        new HashMap<>() {
          {
            put(0, new int[] {1});
          }
        };

    StructuredTrace trace = createTraceWithEventsAndEdges(allEvents, eventEdges);

    ExitCallsEnricher exitCallsEnricher = new ExitCallsEnricher();
    exitCallsEnricher.enrichTrace(trace);

    assertTraceDoesNotContainAttribute(trace, API_EXIT_CALLS_COUNT);
  }

  @Test
  void apiExitCallsCountAttributeNotAddedIfApiNodeExistButNoExitCalls() {
    Event yEvent = createUnspecifiedTypeEventWithName("yEvent"); // 0
    Event zEvent = createUnspecifiedTypeEventWithName("zEvent"); // 1
    Event aEntryHeadSpanEvent = createEntryEventWithName("aEvent"); // 2

    Event[] allEvents = new Event[] {yEvent, zEvent, aEntryHeadSpanEvent};
    HashMap<Integer, int[]> eventEdges =
        new HashMap<>() {
          {
            put(0, new int[] {1});
            put(1, new int[] {2});
          }
        };

    StructuredTrace trace = createTraceWithEventsAndEdges(allEvents, eventEdges);

    ExitCallsEnricher exitCallsEnricher = new ExitCallsEnricher();
    exitCallsEnricher.enrichTrace(trace);

    assertTraceDoesNotContainAttribute(trace, API_EXIT_CALLS_COUNT);
  }
}
