package org.hypertrace.traceenricher.trace.util;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.junit.jupiter.api.Test;

public class ApiTraceGraphTest {

  @Test
  public void testBuild() throws IOException {
    URL resource = Thread.currentThread().getContextClassLoader().
        getResource("SampleTrace-Hotrod.avro");

    SpecificDatumReader<StructuredTrace> datumReader = new SpecificDatumReader<>(
        StructuredTrace.getClassSchema());
    DataFileReader<StructuredTrace> dfrStructuredTrace = new DataFileReader<>(new File(resource.getPath()), datumReader);

    StructuredTrace trace = dfrStructuredTrace.next();
    ApiTraceGraph apiTraceGraph = new ApiTraceGraph(trace);
    apiTraceGraph.build();
    assertTrue(apiTraceGraph.getApiNodeEventEdgeList().size() > 0);
    assertTrue(apiTraceGraph.getNodeList().size() > 0);
    assertNotNull(apiTraceGraph.getTrace());

    dfrStructuredTrace.close();
  }
}
