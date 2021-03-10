package org.hypertrace.traceenricher.util;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hypertrace.core.datamodel.AttributeValue;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

public class AvroToJsonLoggerTest {
  @Test
  public void testLogAvro() {
    String fmt = "Testing logging {}";
    Logger logger = mock(Logger.class);
    when(logger.isDebugEnabled()).thenReturn(false);
    AttributeValue attributeValue = AttributeValue.newBuilder().setValue("test-val").build();
    AvroToJsonLogger.log(logger, "Testing logging {}", attributeValue);

    verify(logger, never()).debug(eq(fmt), anyString());

    when(logger.isDebugEnabled()).thenReturn(true);
    AvroToJsonLogger.log(logger, "Testing logging {}", attributeValue);
    AvroToJsonLogger.log(logger, "Testing logging {}", attributeValue);

    verify(logger, times(2))
        .debug(
            fmt,
            "{\"value\":{\"string\":\"test-val\"},\"binary_value\":null,\"value_list\":null,\"value_map\":null}");
  }
}
