package org.hypertrace.traceenricher.trace.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.ApiNode;
import org.junit.jupiter.api.Test;

public class ApiTraceGraphTest {

  @Test
  public void testApiTraceGraph_HotrodTrace() throws IOException {
    URL resource = Thread.currentThread().getContextClassLoader().
        getResource("StructuredTrace-Hotrod.avro");

    SpecificDatumReader<StructuredTrace> datumReader = new SpecificDatumReader<>(
        StructuredTrace.getClassSchema());
    DataFileReader<StructuredTrace> dfrStructuredTrace = new DataFileReader<>(new File(resource.getPath()), datumReader);
    StructuredTrace trace = dfrStructuredTrace.next();
    dfrStructuredTrace.close();

    ApiTraceGraph apiTraceGraph = new ApiTraceGraph(trace);
    assertEquals(12, apiTraceGraph.getApiNodeApiNodeEdgeList().size());
    assertEquals(13, apiTraceGraph.getApiNodeList().size());
    assertNotNull(apiTraceGraph.getTrace());
    verifyEveryEventPartOfSingleApiNode_HotrodTrace(trace, apiTraceGraph);
  }

  private void verifyEveryEventPartOfSingleApiNode_HotrodTrace(
      StructuredTrace trace, ApiTraceGraph apiTraceGraph) {
    Map<ByteBuffer, Set<Integer>> eventToApiNodes = Maps.newHashMap();

    for (int index = 0; index < apiTraceGraph.getApiNodeList().size(); index++) {
      ApiNode<Event> apiNode = apiTraceGraph.getApiNodeList().get(index);
      int finalIndex = index;
      apiNode.getEvents().forEach(v -> eventToApiNodes.computeIfAbsent(
          v.getEventId(), s -> new HashSet<>()).add(finalIndex));
    }

    // verify every event belongs to exactly 1 api_node
    trace.getEventList().forEach(e -> {
      assertTrue(eventToApiNodes.containsKey(e.getEventId()));
      assertEquals(1, eventToApiNodes.get(e.getEventId()).size());
    });
  }
}
