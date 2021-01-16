package org.hypertrace.traceenricher.enrichment.enrichers.space;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import io.reactivex.rxjava3.core.Single;
import java.util.List;
import java.util.NoSuchElementException;
import org.hypertrace.core.attribute.service.v1.LiteralValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.spaces.config.service.v1.AttributeValueRuleData;
import org.hypertrace.spaces.config.service.v1.SpaceConfigRule;
import org.hypertrace.trace.reader.attributes.TraceAttributeReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SpaceRuleEvaluatorTest {

  @Mock TraceAttributeReader mockAttributeReader;

  @Mock StructuredTrace mockTrace;

  @Mock Event mockSpan;

  SpaceRuleEvaluator ruleEvaluator;

  private final String MOCK_SCOPE = "SCOPE";
  private final String MOCK_KEY = "key";

  private final SpaceConfigRule rule =
      SpaceConfigRule.newBuilder()
          .setAttributeValueRuleData(
              AttributeValueRuleData.newBuilder()
                  .setAttributeScope(MOCK_SCOPE)
                  .setAttributeKey(MOCK_KEY))
          .build();

  @BeforeEach
  void beforeEach() {
    this.ruleEvaluator = new SpaceRuleEvaluator(this.mockAttributeReader);
  }

  @Test
  void testConvertsStringValue() {
    when(this.mockAttributeReader.getSpanValue(this.mockTrace, this.mockSpan, MOCK_SCOPE, MOCK_KEY))
        .thenReturn(Single.just(LiteralValue.newBuilder().setStringValue("attr-value").build()));
    assertEquals(
        List.of("attr-value"),
        this.ruleEvaluator.calculateSpacesForRule(this.mockTrace, this.mockSpan, this.rule));
  }

  @Test
  void testConvertsIntValue() {
    when(this.mockAttributeReader.getSpanValue(this.mockTrace, this.mockSpan, MOCK_SCOPE, MOCK_KEY))
        .thenReturn(Single.just(LiteralValue.newBuilder().setIntValue(12).build()));
    assertEquals(
        List.of("12"),
        this.ruleEvaluator.calculateSpacesForRule(this.mockTrace, this.mockSpan, this.rule));
  }

  @Test
  void testConvertsNoValue() {
    when(this.mockAttributeReader.getSpanValue(this.mockTrace, this.mockSpan, MOCK_SCOPE, MOCK_KEY))
        .thenReturn(Single.error(new NoSuchElementException("no value")));
    assertEquals(
        List.of(),
        this.ruleEvaluator.calculateSpacesForRule(this.mockTrace, this.mockSpan, this.rule));
  }
}
