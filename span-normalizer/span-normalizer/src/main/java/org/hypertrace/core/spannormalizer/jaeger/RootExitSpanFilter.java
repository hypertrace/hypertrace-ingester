package org.hypertrace.core.spannormalizer.jaeger;

import com.typesafe.config.Config;
import io.jaegertracing.api_v2.JaegerSpanInternalModel;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.span.constants.v1.SpanAttribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RootExitSpanFilter {
  /**
   * Config key using which a list of criterion can be specified to drop the matching spans. Any
   * span matching any one of the criterion is dropped. Each criteria is a comma separated list of
   * key:value pairs and multiple pairs in one criteria are AND'ed.
   *
   * <p>For example:
   * ["messaging.destination_kind:queue,messaging.operation:receive,messaging.system:jms"] drops all
   * spans which have all 3 attribute:value pairs.
   */
  public static final String ROOT_SPAN_DROP_CRITERION_CONFIG =
      "processor.rootExitSpanDropCriterion";

  public static final String DEFAULT_OPERATION = "defaultOperation";
  private static final Logger LOG = LoggerFactory.getLogger(SpanFilter.class);
  private static final String DROP = "drop";
  private static final String PROCESS = "process";
  private static final String EXCLUSIONS = "exclusionsMatchCriterion";

  private static final String COMMA = ",";
  private static final String COLON = ":";

  private static final String SPAN_KIND_TAG =
      RawSpanConstants.getValue(SpanAttribute.SPAN_ATTRIBUTE_SPAN_KIND);
  private static final String SPAN_KIND_CLIENT = "client";

  private String defaultOperation = PROCESS;
  private List<List<Pair<String, String>>> exclusionsMatchCriterion = Collections.emptyList();

  public RootExitSpanFilter(Config config) {
    if (config.hasPath(ROOT_SPAN_DROP_CRITERION_CONFIG)) {
      Config dropCriterionConfig = config.getConfig(ROOT_SPAN_DROP_CRITERION_CONFIG);
      LOG.info("Span drop criterion: {}", dropCriterionConfig);
      defaultOperation =
          dropCriterionConfig.hasPath(DEFAULT_OPERATION)
              ? dropCriterionConfig.getString(DEFAULT_OPERATION)
              : PROCESS;
      List<String> exclusionList =
          dropCriterionConfig.hasPath(EXCLUSIONS)
              ? dropCriterionConfig.getStringList(EXCLUSIONS)
              : Collections.emptyList();
      // Parse the config to see if there is any criteria to drop spans.
      this.exclusionsMatchCriterion =
          exclusionList.stream()
              // Split each criteria based on comma
              .map(s -> s.split(COMMA))
              .map(
                  a ->
                      Arrays.stream(a)
                          .map(this::convertToPair)
                          .filter(Objects::nonNull)
                          .collect(Collectors.toList()))
              .collect(Collectors.toList());
    }
  }

  @Nullable
  private Pair<String, String> convertToPair(String s) {
    if (s != null && s.contains(COLON)) {
      String[] parts = s.split(COLON);
      if (parts.length == 2) {
        return Pair.of(parts[0], parts[1]);
      }
    }
    return null;
  }

  /**
   * Method to check if the given span attributes match any of the drop criterion. Returns true if
   * the span should be dropped, false otherwise.
   */
  public boolean shouldDropSpan(
      JaegerSpanInternalModel.Span span, Map<String, JaegerSpanInternalModel.KeyValue> tags) {
    return isRootExitSpan(span, tags)
            && (PROCESS.equals(defaultOperation)
                && exclusionsMatchCriterion.stream()
                    .anyMatch(
                        l ->
                            l.stream()
                                .allMatch(
                                    p ->
                                        tags.containsKey(p.getLeft())
                                            && StringUtils.equals(
                                                tags.get(p.getLeft()).getVStr(), p.getRight()))))
        || (DROP.equals(defaultOperation)
            && exclusionsMatchCriterion.stream()
                .noneMatch(
                    l ->
                        l.stream()
                            .allMatch(
                                p ->
                                    tags.containsKey(p.getLeft())
                                        && StringUtils.equals(
                                            tags.get(p.getLeft()).getVStr(), p.getRight()))));
  }

  private boolean isRootExitSpan(
      JaegerSpanInternalModel.Span span, Map<String, JaegerSpanInternalModel.KeyValue> tags) {
    if (!span.getReferencesList().isEmpty()) {
      return false;
    }
    JaegerSpanInternalModel.KeyValue spanKindKeyValue = tags.get(SPAN_KIND_TAG);
    if (spanKindKeyValue == null) {
      return false;
    }

    return SPAN_KIND_CLIENT.equals(spanKindKeyValue.getVStr());
  }
}
