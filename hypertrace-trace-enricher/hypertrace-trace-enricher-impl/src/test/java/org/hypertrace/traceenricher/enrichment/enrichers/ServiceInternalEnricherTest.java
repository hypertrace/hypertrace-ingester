package org.hypertrace.traceenricher.enrichment.enrichers;

import static java.util.stream.Collectors.toList;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants;
import org.hypertrace.traceenricher.enrichment.Enricher;
import org.hypertrace.traceenricher.trace.util.ApiTraceGraph;
import org.hypertrace.traceenricher.trace.util.ApiTraceGraphBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ServiceInternalEnricherTest extends AbstractAttributeEnricherTest {

  private final Enricher testCandidate = new ServiceInternalTimeEnricher();
  private StructuredTrace trace;

  @BeforeEach
  public void setup() throws IOException {
    URL resource =
        Thread.currentThread().getContextClassLoader().getResource("StructuredTrace-Hotrod.avro");
    SpecificDatumReader<StructuredTrace> datumReader =
        new SpecificDatumReader<>(StructuredTrace.getClassSchema());
    DataFileReader<StructuredTrace> dfrStructuredTrace =
        new DataFileReader<>(new File(resource.getPath()), datumReader);
    trace = dfrStructuredTrace.next();
    dfrStructuredTrace.close();
  }

  @Test
  public void validateServiceInternalTimeAttributeInEntrySpans() {
    // this trace has 12 api nodes
    // api edges
    // 0 -> [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    // backend exit
    // 1 -> to redis 13 exit calls
    // 2 -> to mysql 1 exit call
    // for events parts of api_node 0, there should 12 exit calls
    // for events parts of api_node 1, there should be 13 exit calls
    // for events parts of api_node 2, there should be 1 exit calls
    // verify exit call count per service per api_trace
    // this trace has 4 services
    // frontend service has 1 api_entry span and that api_node has 12 exit calls [drive: 1,
    // customer: 1, route: 10]
    //setup
    ApiTraceGraph apiTraceGraph = ApiTraceGraphBuilder.buildGraph(trace);
    var apiNodes = apiTraceGraph.getApiNodeList();
    //Assert preconditions
    Assertions.assertEquals(13, apiNodes.size());
    apiNodes.forEach(
        apiNode -> Assertions.assertTrue(apiNode.getEntryApiBoundaryEvent().isPresent()));
    List<String> serviceNames = apiNodes.stream().map(apiNode -> {
      Assertions.assertTrue(apiNode.getEntryApiBoundaryEvent().isPresent());
      return apiNode.getEntryApiBoundaryEvent().get().getServiceName();
    }).collect(toList());
    Assertions.assertTrue(serviceNames.contains("frontend"));
    Assertions.assertTrue(serviceNames.contains("driver"));
    Assertions.assertTrue(serviceNames.contains("customer"));
    Assertions.assertTrue(serviceNames.contains("route"));
    //execute
    testCandidate.enrichTrace(trace);
    //assertions: All entry spans should have this tag
    apiTraceGraph.getApiNodeList().forEach(
        a -> Assertions.assertTrue(
            a.getEntryApiBoundaryEvent().get().getAttributes().getAttributeMap()
                .containsKey(EnrichedSpanConstants.INTERNAL_SVC_LATENCY)));
  }
}
