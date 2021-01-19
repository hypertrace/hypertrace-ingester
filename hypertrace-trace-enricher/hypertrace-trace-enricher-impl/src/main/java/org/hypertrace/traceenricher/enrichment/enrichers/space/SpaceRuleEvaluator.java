package org.hypertrace.traceenricher.enrichment.enrichers.space;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.attribute.service.v1.LiteralValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.spaces.config.service.v1.AttributeValueRuleData;
import org.hypertrace.spaces.config.service.v1.SpaceConfigRule;
import org.hypertrace.trace.reader.attributes.TraceAttributeReader;

class SpaceRuleEvaluator {
  private final TraceAttributeReader attributeReader;

  SpaceRuleEvaluator(TraceAttributeReader attributeReader) {
    this.attributeReader = attributeReader;
  }

  public List<String> calculateSpacesForRule(
      StructuredTrace trace, Event span, SpaceConfigRule rule) {
    switch (rule.getRuleDataCase()) {
      case ATTRIBUTE_VALUE_RULE_DATA:
        return this.calculateSpacesForAttribute(trace, span, rule.getAttributeValueRuleData());
      case RULEDATA_NOT_SET:
      default:
        return List.of();
    }
  }

  private List<String> calculateSpacesForAttribute(
      StructuredTrace trace, Event span, AttributeValueRuleData attributeValueRuleData) {

    return this.attributeReader
        .getSpanValue(
            trace,
            span,
            attributeValueRuleData.getAttributeScope(),
            attributeValueRuleData.getAttributeKey())
        .mapOptional(this::stringifyLiteral)
        .filter(string -> !string.isEmpty())
        .map(List::of)
        .onErrorComplete()
        .defaultIfEmpty(Collections.emptyList())
        .blockingGet();
  }

  private Optional<String> stringifyLiteral(LiteralValue literalValue) {
    switch (literalValue.getValueCase()) {
      case INT_VALUE:
        return Optional.of(String.valueOf(literalValue.getIntValue()));
      case STRING_VALUE:
        return Optional.of(literalValue.getStringValue());
      case FLOAT_VALUE:
        return Optional.of(String.valueOf(literalValue.getFloatValue()));
      case BOOLEAN_VALUE:
        return Optional.of(String.valueOf(literalValue.getBooleanValue()));
      case VALUE_NOT_SET:
      default:
        return Optional.empty();
    }
  }
}
