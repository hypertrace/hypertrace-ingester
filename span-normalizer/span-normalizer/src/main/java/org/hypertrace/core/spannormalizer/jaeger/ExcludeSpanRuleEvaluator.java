package org.hypertrace.core.spannormalizer.jaeger;

import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_URL;
import static org.hypertrace.core.span.constants.v1.OTSpanTag.OT_SPAN_TAG_HTTP_URL;
import static org.hypertrace.core.spannormalizer.jaeger.JaegerSpanNormalizer.OLD_JAEGER_SERVICENAME_KEY;

import io.jaegertracing.api_v2.JaegerSpanInternalModel;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.semantic.convention.constants.http.OTelHttpSemanticConventions;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.span.constants.v1.Http;
import org.hypertrace.core.spannormalizer.util.JaegerHTTagsConverter;
import org.hypertrace.span.processing.config.service.v1.ExcludeSpanRule;
import org.hypertrace.span.processing.config.service.v1.Field;
import org.hypertrace.span.processing.config.service.v1.LogicalOperator;
import org.hypertrace.span.processing.config.service.v1.LogicalSpanFilterExpression;
import org.hypertrace.span.processing.config.service.v1.RelationalOperator;
import org.hypertrace.span.processing.config.service.v1.RelationalSpanFilterExpression;
import org.hypertrace.span.processing.config.service.v1.SpanFilter;

public class ExcludeSpanRuleEvaluator {
  // rename and replace SpanFilter later.

  public ExcludeSpanRuleEvaluator() {
    // empty constructor
  }

  private static final List<String> FULL_URL_ATTRIBUTES =
      List.of(
          RawSpanConstants.getValue(OT_SPAN_TAG_HTTP_URL),
          RawSpanConstants.getValue(HTTP_REQUEST_URL),
          RawSpanConstants.getValue(Http.HTTP_URL),
          OTelHttpSemanticConventions.HTTP_URL.getValue());

  private static final String ENVIRONMENT_ATTRIBUTE = "deployment.environment";

  public boolean shouldDropSpan(
      Optional<String> tenantIdKey,
      JaegerSpanInternalModel.Span jaegerSpan,
      List<ExcludeSpanRule> excludeSpanRules,
      Map<String, JaegerSpanInternalModel.KeyValue> tags,
      Map<String, JaegerSpanInternalModel.KeyValue> processTags) {
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

  boolean applyExcludeSpanRules(
      List<ExcludeSpanRule> excludeSpanRules,
      Map<String, JaegerSpanInternalModel.KeyValue> tags,
      Map<String, JaegerSpanInternalModel.KeyValue> processTags,
      String serviceName) {
    boolean result = true;
    for (ExcludeSpanRule excludeSpanRule : excludeSpanRules) {
      result =
          result
              && applyFilter(
                  excludeSpanRule.getRuleInfo().getFilter(), tags, processTags, serviceName);
    }
    return result;
  }

  boolean applyFilter(
      SpanFilter filter,
      Map<String, JaegerSpanInternalModel.KeyValue> tags,
      Map<String, JaegerSpanInternalModel.KeyValue> processTags,
      String serviceName) {
    if (filter.hasRelationalSpanFilter()) {
      return evaluateRelationalSpanFilter(
          filter.getRelationalSpanFilter(), tags, processTags, serviceName);
    } else {
      LogicalSpanFilterExpression logicalSpanFilterExpression = filter.getLogicalSpanFilter();
      if (filter
          .getLogicalSpanFilter()
          .getOperator()
          .equals(LogicalOperator.LOGICAL_OPERATOR_AND)) {
        boolean result = true;
        for (SpanFilter spanFilter : logicalSpanFilterExpression.getOperandsList()) {
          result = result && applyFilter(spanFilter, tags, processTags, serviceName);
        }
        return result;
      } else {
        boolean result = false;
        for (SpanFilter spanFilter : logicalSpanFilterExpression.getOperandsList()) {
          result = result || applyFilter(spanFilter, tags, processTags, serviceName);
        }
        return result;
      }
    }
  }

  boolean evaluateRelationalSpanFilter(
      RelationalSpanFilterExpression relationalSpanFilterExpression,
      Map<String, JaegerSpanInternalModel.KeyValue> tags,
      Map<String, JaegerSpanInternalModel.KeyValue> processTags,
      String serviceName) {

    if (relationalSpanFilterExpression.hasSpanAttributeKey()) {
      String spanAttributeKey = relationalSpanFilterExpression.getSpanAttributeKey();
      return matchSpanFilter(
          tags,
          processTags,
          relationalSpanFilterExpression.getOperator(),
          spanAttributeKey,
          relationalSpanFilterExpression.getRightOperand().getStringValue());
    } else {
      Field field = relationalSpanFilterExpression.getField();
      switch (field) {
        case FIELD_SERVICE_NAME:
          return matchSpanFilter(
              relationalSpanFilterExpression.getOperator(),
              serviceName,
              relationalSpanFilterExpression.getRightOperand().getStringValue());
        case FIELD_ENVIRONMENT_NAME:
          return matchSpanFilter(
              tags,
              processTags,
              relationalSpanFilterExpression.getOperator(),
              ENVIRONMENT_ATTRIBUTE,
              relationalSpanFilterExpression.getRightOperand().getStringValue());
        case FIELD_URL:
          boolean result = false;
          for (String urlAttribute : FULL_URL_ATTRIBUTES) {
            result =
                result
                    || matchSpanFilter(
                        tags,
                        processTags,
                        relationalSpanFilterExpression.getOperator(),
                        urlAttribute,
                        relationalSpanFilterExpression.getRightOperand().getStringValue());
          }
          return result;
        default:
          return false;
      }
    }
  }

  private boolean matchSpanFilter(
      Map<String, JaegerSpanInternalModel.KeyValue> tags,
      Map<String, JaegerSpanInternalModel.KeyValue> processTags,
      RelationalOperator operator,
      String lhs,
      String rhs) {
    return (tags.containsKey(lhs) && matchSpanFilter(operator, tags.get(lhs).getVStr(), rhs))
        || (processTags.containsKey(lhs)
            && matchSpanFilter(operator, tags.get(lhs).getVStr(), rhs));
  }

  private boolean matchSpanFilter(RelationalOperator operator, String lhs, String rhs) {
    switch (operator) {
      case RELATIONAL_OPERATOR_EQUALS:
        return StringUtils.equals(lhs, rhs);
      case RELATIONAL_OPERATOR_NOT_EQUALS:
        return !StringUtils.equals(lhs, rhs);
      case RELATIONAL_OPERATOR_STARTS_WITH:
        return StringUtils.startsWith(lhs, rhs);
      case RELATIONAL_OPERATOR_ENDS_WITH:
        return StringUtils.endsWith(lhs, rhs);
      case RELATIONAL_OPERATOR_REGEX_MATCH:
        return lhs.matches(rhs);
      case RELATIONAL_OPERATOR_CONTAINS:
        return StringUtils.contains(lhs, rhs);
      default:
        return false;
    }
  }
}
