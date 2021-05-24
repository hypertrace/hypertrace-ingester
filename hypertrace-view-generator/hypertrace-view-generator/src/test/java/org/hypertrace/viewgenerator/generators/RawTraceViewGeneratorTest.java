package org.hypertrace.viewgenerator.generators;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.viewgenerator.api.RawTraceView;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RawTraceViewGeneratorTest {
  private RawTraceViewGenerator rawTraceViewGenerator;

  @BeforeEach
  public void setup() {
    rawTraceViewGenerator = new RawTraceViewGenerator();
  }

  @Test
  public void testGenerateView_SampleHotrodTrace() throws IOException {
    RawTraceViewGenerator rawTraceViewGenerator = new RawTraceViewGenerator();
    List<RawTraceView> rawTraceViews = rawTraceViewGenerator.process(createSampleTrace());
    assertEquals(1, rawTraceViews.size());
    assertEquals(4, rawTraceViews.get(0).getNumServices());
    assertEquals(50, rawTraceViews.get(0).getNumSpans());
  }

  private static StructuredTrace createSampleTrace() throws IOException {
    URL resource =
        Thread.currentThread().getContextClassLoader().getResource("StructuredTrace-Hotrod.avro");
    SpecificDatumReader<StructuredTrace> datumReader =
        new SpecificDatumReader<>(StructuredTrace.getClassSchema());
    DataFileReader<StructuredTrace> dfrStructuredTrace =
        new DataFileReader<>(new File(resource.getPath()), datumReader);
    StructuredTrace trace = dfrStructuredTrace.next();
    dfrStructuredTrace.close();
    return trace;
  }
}
