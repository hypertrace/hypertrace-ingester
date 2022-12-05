package org.hypertrace.viewgenerator.generators;

import java.io.FileNotFoundException;
import java.util.List;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.viewgenerator.api.RawServiceView;
import org.hypertrace.viewgenerator.generators.utils.TestUtilities;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RawServiceViewGeneratorTest {
  private RawServiceViewGenerator rawServiceViewGenerator;

  @BeforeEach
  public void setup() {
    rawServiceViewGenerator = new RawServiceViewGenerator();
  }

  @Test
  public void testEntrySpanInternalDuration() throws FileNotFoundException {
    StructuredTrace sampleTrace = TestUtilities.getSampleHotRodTrace();

    List<RawServiceView> rawServiceViews = rawServiceViewGenerator.process(sampleTrace);

    Long actualInternalDurationMillis = rawServiceViews.get(0).getInternalDurationMillis();
    Assertions.assertNull(actualInternalDurationMillis);
  }
}
