package org.hypertrace.core.spannormalizer.jaeger;

import static org.hypertrace.core.spannormalizer.jaeger.JaegerSpanNormalizer.OLD_JAEGER_SERVICENAME_KEY;
import static org.hypertrace.semantic.convention.utils.http.HttpSemanticConventionUtils.FULL_URL_ATTRIBUTES;

import com.google.common.annotations.VisibleForTesting;
import com.typesafe.config.Config;
import io.jaegertracing.api_v2.JaegerSpanInternalModel;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.spannormalizer.util.JaegerHTTagsConverter;
import org.hypertrace.span.processing.config.service.v1.ExcludeSpanRule;
import org.hypertrace.span.processing.config.service.v1.Field;
import org.hypertrace.span.processing.config.service.v1.ListValue;
import org.hypertrace.span.processing.config.service.v1.LogicalOperator;
import org.hypertrace.span.processing.config.service.v1.LogicalSpanFilterExpression;
import org.hypertrace.span.processing.config.service.v1.RelationalOperator;
import org.hypertrace.span.processing.config.service.v1.RelationalSpanFilterExpression;
import org.hypertrace.span.processing.config.service.v1.SpanFilter;
import org.hypertrace.span.processing.config.service.v1.SpanFilterValue;

@Slf4j
public class ExcludeSpanRuleEvaluator {
  // rename and replace SpanFilter later.

  private final ExcludeSpanRulesCache excludeSpanRulesCache;

  public ExcludeSpanRuleEvaluator(Config config) {
    this.excludeSpanRulesCache = ExcludeSpanRulesCache.getInstance(config);
  }

  @VisibleForTesting
  public ExcludeSpanRuleEvaluator(ExcludeSpanRulesCache excludeSpanRulesCache) {
    this.excludeSpanRulesCache = excludeSpanRulesCache;
  }

  private static final String ENVIRONMENT_ATTRIBUTE = "deployment.environment";

  @SneakyThrows
  public boolean shouldDropSpan(
      Optional<String> tenantIdKey,
      String tenantId,
      JaegerSpanInternalModel.Span jaegerSpan,
      Map<String, JaegerSpanInternalModel.KeyValue> tags,
      Map<String, JaegerSpanInternalModel.KeyValue> processTags) {
    List<ExcludeSpanRule> excludeSpanRules = excludeSpanRulesCache.get(tenantId);
    if (excludeSpanRules.isEmpty()) {
      return false;
    }

    Map<String, AttributeValue> attributeFieldMap = new HashMap<>();

    List<JaegerSpanInternalModel.KeyValue> tagsList = jaegerSpan.getTagsList();
    for (JaegerSpanInternalModel.KeyValue keyValue : tagsList) {
      String key = keyValue.getKey().toLowerCase();
      if ((tenantIdKey.isPresent() && key.equals(tenantIdKey.get()))) {
        continue;
      }
      attributeFieldMap.put(key, JaegerHTTagsConverter.createFromJaegerKeyValue(keyValue));
    }

    String serviceName =
        !StringUtils.isEmpty(jaegerSpan.getProcess().getServiceName())
            ? jaegerSpan.getProcess().getServiceName()
            : attributeFieldMap.containsKey(OLD_JAEGER_SERVICENAME_KEY)
                ? attributeFieldMap.get(OLD_JAEGER_SERVICENAME_KEY).getValue()
                : StringUtils.EMPTY;

    return applyExcludeSpanRules(excludeSpanRules, tags, processTags, serviceName);
  }

  public boolean shouldDropEvent(Event event) {
    return false;
  }

  private boolean applyExcludeSpanRules(
      List<ExcludeSpanRule> excludeSpanRules,
      Map<String, JaegerSpanInternalModel.KeyValue> tags,
      Map<String, JaegerSpanInternalModel.KeyValue> processTags,
      String serviceName) {
    return excludeSpanRules.stream()
        .anyMatch(
            excludeSpanRule ->
                applyFilter(
                    excludeSpanRule.getRuleInfo().getFilter(), tags, processTags, serviceName));
  }

  private boolean applyFilter(
      SpanFilter filter,
      Map<String, JaegerSpanInternalModel.KeyValue> tags,
      Map<String, JaegerSpanInternalModel.KeyValue> processTags,
      String serviceName) {
    if (filter.hasRelationalSpanFilter()) {
      return matchesRelationalSpanFilter(
          filter.getRelationalSpanFilter(), tags, processTags, serviceName);
    } else {
      LogicalSpanFilterExpression logicalSpanFilterExpression = filter.getLogicalSpanFilter();
      if (filter
          .getLogicalSpanFilter()
          .getOperator()
          .equals(LogicalOperator.LOGICAL_OPERATOR_AND)) {
        return logicalSpanFilterExpression.getOperandsList().stream()
            .allMatch(spanFilter -> applyFilter(spanFilter, tags, processTags, serviceName));
      } else {
        return logicalSpanFilterExpression.getOperandsList().stream()
            .anyMatch(spanFilter -> applyFilter(spanFilter, tags, processTags, serviceName));
      }
    }
  }

  private boolean matchesRelationalSpanFilter(
      RelationalSpanFilterExpression relationalSpanFilterExpression,
      Map<String, JaegerSpanInternalModel.KeyValue> tags,
      Map<String, JaegerSpanInternalModel.KeyValue> processTags,
      String serviceName) {

    if (relationalSpanFilterExpression.hasSpanAttributeKey()) {
      String spanAttributeKey = relationalSpanFilterExpression.getSpanAttributeKey();
      return matches(
          tags,
          processTags,
          relationalSpanFilterExpression.getOperator(),
          spanAttributeKey,
          relationalSpanFilterExpression.getRightOperand());
    } else {
      Field field = relationalSpanFilterExpression.getField();
      switch (field) {
        case FIELD_SERVICE_NAME:
          return matches(
              serviceName,
              relationalSpanFilterExpression.getRightOperand(),
              relationalSpanFilterExpression.getOperator());
        case FIELD_ENVIRONMENT_NAME:
          return matches(
              tags,
              processTags,
              relationalSpanFilterExpression.getOperator(),
              ENVIRONMENT_ATTRIBUTE,
              relationalSpanFilterExpression.getRightOperand());
        case FIELD_URL:
          return FULL_URL_ATTRIBUTES.stream()
              .anyMatch(
                  urlAttribute ->
                      matches(
                          tags,
                          processTags,
                          relationalSpanFilterExpression.getOperator(),
                          urlAttribute,
                          relationalSpanFilterExpression.getRightOperand()));
        default:
          log.error("Unknown filter field: {}", field);
          return false;
      }
    }
  }

  private boolean matches(
      Map<String, JaegerSpanInternalModel.KeyValue> tags,
      Map<String, JaegerSpanInternalModel.KeyValue> processTags,
      RelationalOperator operator,
      String lhs,
      SpanFilterValue rhs) {
    return (tags.containsKey(lhs) && matches(tags.get(lhs).getVStr(), rhs, operator))
        || (processTags.containsKey(lhs) && matches(processTags.get(lhs).getVStr(), rhs, operator));
  }

  private boolean matches(
      String lhs,
      String rhs,
      org.hypertrace.span.processing.config.service.v1.RelationalOperator relationalOperator) {
    switch (relationalOperator) {
      case RELATIONAL_OPERATOR_CONTAINS:
        return lhs.contains(rhs);
      case RELATIONAL_OPERATOR_EQUALS:
        return lhs.equals(rhs);
      case RELATIONAL_OPERATOR_NOT_EQUALS:
        return !lhs.equals(rhs);
      case RELATIONAL_OPERATOR_STARTS_WITH:
        return lhs.startsWith(rhs);
      case RELATIONAL_OPERATOR_ENDS_WITH:
        return lhs.endsWith(rhs);
      case RELATIONAL_OPERATOR_REGEX_MATCH:
        return lhs.matches(rhs);
      default:
        log.error("Unsupported relational operator for string value rhs:{}", relationalOperator);
        return false;
    }
  }

  private boolean matches(
      String lhs,
      ListValue rhs,
      org.hypertrace.span.processing.config.service.v1.RelationalOperator relationalOperator) {
    switch (relationalOperator) {
      case RELATIONAL_OPERATOR_IN:
        return rhs.getValuesList().stream()
            .map(org.hypertrace.span.processing.config.service.v1.SpanFilterValue::getStringValue)
            .collect(Collectors.toUnmodifiableList())
            .contains(lhs);
      default:
        log.error("Unsupported relational operator for list value rhs:{}", relationalOperator);
        return false;
    }
  }

  private boolean matches(
      String lhs,
      org.hypertrace.span.processing.config.service.v1.SpanFilterValue rhs,
      org.hypertrace.span.processing.config.service.v1.RelationalOperator relationalOperator) {
    switch (rhs.getValueCase()) {
      case STRING_VALUE:
        return matches(lhs, rhs.getStringValue(), relationalOperator);
      case LIST_VALUE:
        return matches(lhs, rhs.getListValue(), relationalOperator);
      default:
        log.error("Unknown span filter value type:{}", rhs);
        return false;
    }
  }
}
