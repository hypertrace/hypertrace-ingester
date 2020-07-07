package org.hypertrace.traceenricher.util;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hypertrace.core.datamodel.Event;
import org.junit.jupiter.api.Test;

public class EnricherUtilTest {
  @Test
  public void testGrpcEventNames() {
    Event e1 = mock(Event.class);
    when(e1.getEventName()).thenReturn("Sent./products/browse");
    assertTrue(EnricherUtil.isSentGrpcEvent(e1));

    Event e2 = mock(Event.class);
    when(e2.getEventName()).thenReturn("Recv./products/browse");
    assertTrue(EnricherUtil.isReceivedGrpcEvent(e2));

    Event e3 = mock(Event.class);
    when(e3.getEventName()).thenReturn("GET /products/browse");
    assertFalse(EnricherUtil.isSentGrpcEvent(e3));
    assertFalse(EnricherUtil.isReceivedGrpcEvent(e3));
  }
}
