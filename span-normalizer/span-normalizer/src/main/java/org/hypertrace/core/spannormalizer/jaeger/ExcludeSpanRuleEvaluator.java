package org.hypertrace.core.spannormalizer.jaeger;

import com.google.common.annotations.VisibleForTesting;
import com.typesafe.config.Config;
import io.jaegertracing.api_v2.JaegerSpanInternalModel;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.span.processing.utils.SpanFilterMatcher;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.hypertrace.semantic.convention.utils.http.HttpSemanticConventionUtils;
import org.hypertrace.span.processing.config.service.v1.ExcludeSpanRule;
import org.hypertrace.span.processing.config.service.v1.Field;
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
  private final SpanFilterMatcher spanFilterMatcher;

  public ExcludeSpanRuleEvaluator(Config config, GrpcChannelRegistry grpcChannelRegistry) {
    this.excludeSpanRulesCache = ExcludeSpanRulesCache.getInstance(config, grpcChannelRegistry);
    this.spanFilterMatcher = new SpanFilterMatcher();
  }

  @VisibleForTesting
  public ExcludeSpanRuleEvaluator(ExcludeSpanRulesCache excludeSpanRulesCache) {
    this.excludeSpanRulesCache = excludeSpanRulesCache;
    this.spanFilterMatcher = new SpanFilterMatcher();
  }

  @SneakyThrows
  public boolean shouldDropSpan(
      String tenantId,
      Event event,
      Map<String, JaegerSpanInternalModel.KeyValue> tags,
      Map<String, JaegerSpanInternalModel.KeyValue> processTags) {
    List<ExcludeSpanRule> excludeSpanRules = excludeSpanRulesCache.get(tenantId);
    if (excludeSpanRules.isEmpty()) {
      return false;
    }

    return applyExcludeSpanRules(excludeSpanRules, tags, processTags, event);
  }

  private boolean applyExcludeSpanRules(
      List<ExcludeSpanRule> excludeSpanRules,
      Map<String, JaegerSpanInternalModel.KeyValue> tags,
      Map<String, JaegerSpanInternalModel.KeyValue> processTags,
      Event event) {
    return excludeSpanRules.stream()
        .filter(excludeSpanRule -> !excludeSpanRule.getRuleInfo().getDisabled())
        .anyMatch(
            excludeSpanRule ->
                applyFilter(excludeSpanRule.getRuleInfo().getFilter(), tags, processTags, event));
  }

  private boolean applyFilter(
      SpanFilter filter,
      Map<String, JaegerSpanInternalModel.KeyValue> tags,
      Map<String, JaegerSpanInternalModel.KeyValue> processTags,
      Event event) {
    if (filter.hasRelationalSpanFilter()) {
      return matchesRelationalSpanFilter(
          filter.getRelationalSpanFilter(), tags, processTags, event);
    } else {
      LogicalSpanFilterExpression logicalSpanFilterExpression = filter.getLogicalSpanFilter();
      if (filter
          .getLogicalSpanFilter()
          .getOperator()
          .equals(LogicalOperator.LOGICAL_OPERATOR_AND)) {
        return logicalSpanFilterExpression.getOperandsList().stream()
            .allMatch(spanFilter -> applyFilter(spanFilter, tags, processTags, event));
      } else {
        return logicalSpanFilterExpression.getOperandsList().stream()
            .anyMatch(spanFilter -> applyFilter(spanFilter, tags, processTags, event));
      }
    }
  }

  private boolean matchesRelationalSpanFilter(
      RelationalSpanFilterExpression relationalSpanFilterExpression,
      Map<String, JaegerSpanInternalModel.KeyValue> tags,
      Map<String, JaegerSpanInternalModel.KeyValue> processTags,
      Event event) {

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
          return spanFilterMatcher.matches(
              event.getServiceName(),
              relationalSpanFilterExpression.getRightOperand(),
              relationalSpanFilterExpression.getOperator());
        case FIELD_ENVIRONMENT_NAME:
          Optional<String> environmentMaybe =
              HttpSemanticConventionUtils.getEnvironmentForSpan(event);
          return environmentMaybe
              .filter(
                  environment ->
                      spanFilterMatcher.matches(
                          environment,
                          relationalSpanFilterExpression.getRightOperand(),
                          relationalSpanFilterExpression.getOperator()))
              .isPresent();
        case FIELD_URL:
          Optional<String> urlMaybe = getUrl(event);
          return urlMaybe
              .filter(
                  url ->
                      spanFilterMatcher.matches(
                          url,
                          relationalSpanFilterExpression.getRightOperand(),
                          relationalSpanFilterExpression.getOperator()))
              .isPresent();
        case FIELD_URL_PATH:
          Optional<String> maybeUrlPath = getUrlPath(event);
          return maybeUrlPath
              .filter(
                  urlPath ->
                      spanFilterMatcher.matches(
                          urlPath,
                          relationalSpanFilterExpression.getRightOperand(),
                          relationalSpanFilterExpression.getOperator()))
              .isPresent();
        default:
          log.error("Unknown filter field: {}", field);
          return false;
      }
    }
  }

  /**
   * Build the full url if possible, else fall back on the url path
   *
   * @param event Event
   * @return full url if available, else the url path
   */
  private Optional<String> getUrl(final Event event) {
    return HttpSemanticConventionUtils.getFullHttpUrl(event)
        .or(() -> HttpSemanticConventionUtils.getHttpPath(event));
  }

  private Optional<String> getUrlPath(final Event event) {
    return HttpSemanticConventionUtils.getHttpPath(event);
  }

  private boolean matches(
      Map<String, JaegerSpanInternalModel.KeyValue> tags,
      Map<String, JaegerSpanInternalModel.KeyValue> processTags,
      RelationalOperator operator,
      String lhs,
      SpanFilterValue rhs) {
    return (tags.containsKey(lhs)
            && spanFilterMatcher.matches(tags.get(lhs).getVStr(), rhs, operator))
        || (processTags.containsKey(lhs)
            && spanFilterMatcher.matches(processTags.get(lhs).getVStr(), rhs, operator));
  }
}
