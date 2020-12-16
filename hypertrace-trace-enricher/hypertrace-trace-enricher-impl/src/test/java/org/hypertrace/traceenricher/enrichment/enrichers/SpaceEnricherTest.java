package org.hypertrace.traceenricher.enrichment.enrichers;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
import org.junit.jupiter.api.Test;

class SpaceEnricherTest extends AbstractAttributeEnricherTest {

  private final SpaceEnricher enricher = new SpaceEnricher();

  @Test
  void testEnrichEvent() {
    Event targetEvent = createMockEvent();
    enricher.enrichEvent(createMockStructuredTrace(), targetEvent);

    assertEquals(
        AttributeValueCreator.create(emptyList()),
        targetEvent.getEnrichedAttributes().getAttributeMap().get("SPACE_IDS"));
  }

  @Test
  void testEnrichTrace() {
    StructuredTrace targetTrace = createMockStructuredTrace();
    enricher.enrichTrace(targetTrace);

    assertEquals(
        AttributeValueCreator.create(emptyList()),
        targetTrace.getAttributes().getAttributeMap().get("SPACE_IDS"));
  }
}
