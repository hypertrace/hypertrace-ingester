package org.hypertrace.viewgenerator.generators;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.List;
import org.hypertrace.viewgenerator.api.RawTraceView;
import org.hypertrace.viewgenerator.generators.utils.TestUtilities;
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
    List<RawTraceView> rawTraceViews =
        rawTraceViewGenerator.process(TestUtilities.getSampleHotRodTrace());
    assertEquals(1, rawTraceViews.size());
    assertEquals(4, rawTraceViews.get(0).getNumServices());
    assertEquals(50, rawTraceViews.get(0).getNumSpans());
  }
}
