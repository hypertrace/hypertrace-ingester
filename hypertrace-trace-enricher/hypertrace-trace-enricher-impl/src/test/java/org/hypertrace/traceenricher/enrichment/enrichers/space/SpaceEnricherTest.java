package org.hypertrace.traceenricher.enrichment.enrichers.space;

import static java.util.Collections.emptyList;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
import org.hypertrace.spaces.config.service.v1.SpaceConfigRule;
import org.hypertrace.traceenricher.enrichment.enrichers.AbstractAttributeEnricherTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SpaceEnricherTest extends AbstractAttributeEnricherTest {

  @Mock SpaceRulesCachingClient ruleClient;

  @Mock SpaceRuleEvaluator ruleEvaluator;

  private SpaceEnricher enricher;

  @BeforeEach
  void beforeEach() {
    this.enricher = new SpaceEnricher();
    this.enricher.init(this.ruleClient, this.ruleEvaluator);
  }

  @Test
  void testEnrichEventZeroRules() {
    Event targetEvent = mock(Event.class, RETURNS_DEEP_STUBS);
    when(targetEvent.getCustomerId()).thenReturn(TENANT_ID);

    when(this.ruleClient.getRulesForTenant(TENANT_ID)).thenReturn(emptyList());
    this.enricher.enrichEvent(mock(StructuredTrace.class), targetEvent);

    verify(targetEvent.getEnrichedAttributes().getAttributeMap())
        .put("SPACE_IDS", AttributeValueCreator.create(emptyList()));
  }

  @Test
  void testEnrichEventOneRuleToRuleThemAll() {
    Event targetEvent = mock(Event.class, RETURNS_DEEP_STUBS);
    StructuredTrace mockTrace = mock(StructuredTrace.class);
    when(targetEvent.getCustomerId()).thenReturn(TENANT_ID);
    SpaceConfigRule mockRule = SpaceConfigRule.getDefaultInstance(); // Protos can't be mocked

    when(this.ruleClient.getRulesForTenant(TENANT_ID)).thenReturn(List.of(mockRule));
    when(this.ruleEvaluator.calculateSpacesForRule(mockTrace, targetEvent, mockRule))
        .thenReturn(List.of("my-space"));

    enricher.enrichEvent(mockTrace, targetEvent);

    verify(targetEvent.getEnrichedAttributes().getAttributeMap())
        .put("SPACE_IDS", AttributeValueCreator.create(List.of("my-space")));
  }

  @Test
  void testEnrichEventMultiRules() {
    Event targetEvent = mock(Event.class, RETURNS_DEEP_STUBS);
    StructuredTrace mockTrace = mock(StructuredTrace.class);
    when(targetEvent.getCustomerId()).thenReturn(TENANT_ID);
    SpaceConfigRule mockRule1 = SpaceConfigRule.newBuilder().setId("1").build();
    SpaceConfigRule mockRule2 = SpaceConfigRule.newBuilder().setId("2").build();
    SpaceConfigRule mockRule3 = SpaceConfigRule.newBuilder().setId("3").build();

    when(this.ruleClient.getRulesForTenant(TENANT_ID))
        .thenReturn(List.of(mockRule1, mockRule2, mockRule3));
    when(this.ruleEvaluator.calculateSpacesForRule(mockTrace, targetEvent, mockRule1))
        .thenReturn(List.of("my-space"));
    when(this.ruleEvaluator.calculateSpacesForRule(mockTrace, targetEvent, mockRule2))
        .thenReturn(List.of("my-other-space"));
    when(this.ruleEvaluator.calculateSpacesForRule(mockTrace, targetEvent, mockRule3))
        .thenReturn(List.of("my-space")); // Expect to dedupe

    enricher.enrichEvent(mockTrace, targetEvent);

    verify(targetEvent.getEnrichedAttributes().getAttributeMap())
        .put("SPACE_IDS", AttributeValueCreator.create(List.of("my-space", "my-other-space")));
  }

  @Test
  void testEnrichTrace() {
    Event targetEvent1 = mock(Event.class, RETURNS_DEEP_STUBS);
    Event targetEvent2 = mock(Event.class, RETURNS_DEEP_STUBS);
    StructuredTrace mockTrace = mock(StructuredTrace.class, RETURNS_DEEP_STUBS);

    when(targetEvent1.getEnrichedAttributes().getAttributeMap().get("SPACE_IDS"))
        .thenReturn(AttributeValueCreator.create(List.of("first-space")));
    when(targetEvent2.getEnrichedAttributes().getAttributeMap().get("SPACE_IDS"))
        .thenReturn(AttributeValueCreator.create(List.of("second-space")));
    when(mockTrace.getEventList()).thenReturn(List.of(targetEvent1, targetEvent2));

    enricher.enrichTrace(mockTrace);

    verify(mockTrace.getAttributes().getAttributeMap())
        .put("SPACE_IDS", AttributeValueCreator.create(List.of("first-space", "second-space")));
  }
}
