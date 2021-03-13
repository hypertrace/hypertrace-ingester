package org.hypertrace.traceenricher.enrichment.enrichers;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.ApiNode;
import org.hypertrace.traceenricher.enrichedspan.constants.utils.EnrichedSpanUtils;
import org.hypertrace.traceenricher.enrichment.enrichers.ExitCallsEnricher.ApiExitCallInfo;
import org.hypertrace.traceenricher.trace.util.ApiTraceGraph;
import org.junit.jupiter.api.Test;

public class ExitCallsEnricherTest {

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
    Map<ByteBuffer, ApiExitCallInfo> eventToApiExitInfo =
        exitCallsEnricher.computeApiExitCallCount(trace);
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
    eventToApiExitInfo.forEach(
        (k, v) -> {
          Integer apiNodeIndex = eventToApiNodeIndex.get(k);
          if (null != apiNodeIndex) {
            assertEquals(
                apiNodeToExitCallCount.getOrDefault(apiNodeIndex, 0), v.getExitCallCount());
            assertEquals(
                apiNodeToExitServices.getOrDefault(apiNodeIndex, Maps.newHashMap()),
                v.getCalleeNameToExitCalls());
          }
        });

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
}
