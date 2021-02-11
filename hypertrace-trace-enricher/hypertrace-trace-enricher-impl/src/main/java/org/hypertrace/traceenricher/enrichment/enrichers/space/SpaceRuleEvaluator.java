package org.hypertrace.traceenricher.enrichment.enrichers.space;

import java.util.Collections;
import java.util.List;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.spaces.config.service.v1.AttributeValueRuleData;
import org.hypertrace.spaces.config.service.v1.SpaceConfigRule;
import org.hypertrace.trace.reader.attributes.TraceAttributeReader;
import org.hypertrace.trace.reader.attributes.ValueCoercer;

class SpaceRuleEvaluator {
  private final TraceAttributeReader<StructuredTrace, Event> attributeReader;

  SpaceRuleEvaluator(TraceAttributeReader<StructuredTrace, Event> attributeReader) {
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
        .mapOptional(ValueCoercer::convertToString)
        .filter(string -> !string.isEmpty())
        .map(List::of)
        .onErrorComplete()
        .defaultIfEmpty(Collections.emptyList())
        .blockingGet();
  }
}
