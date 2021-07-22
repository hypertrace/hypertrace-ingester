package org.hypertrace.core.spannormalizer.jaeger;

import io.jaegertracing.api_v2.JaegerSpanInternalModel.KeyValue;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpanFilter {

  private static final Logger LOG = LoggerFactory.getLogger(SpanFilter.class);
  /**
   * Config key using which a list of criterion can be specified to drop the matching spans. Any
   * span matching any one of the criterion is dropped. Each criteria is a comma separated list of
   * key:value pairs and multiple pairs in one criteria are AND'ed.
   *
   * <p>For example:
   * ["messaging.destination_kind:queue,messaging.operation:receive,messaging.system:jms"] drops all
   * spans which have all 3 attribute:value pairs.
   */
  public static final String SPAN_DROP_CRITERION_CONFIG = "processor.spanDropCriterion";

  public static final String ROOT_SPAN_DROP_CRITERION_CONFIG = "processor.rootSpanDropCriterion";

  private static final String COMMA = ",";
  private static final String COLON = ":";

  private final List<List<Pair<String, String>>> spanDropCriterion;

  public SpanFilter(List<String> criterion) {
    // Parse the config to see if there is any criteria to drop spans.
    this.spanDropCriterion =
        criterion.stream()
            // Split each criteria based on comma
            .map(s -> s.split(COMMA))
            .map(
                a ->
                    Arrays.stream(a)
                        .map(this::convertToPair)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList()))
            .collect(Collectors.toList());

    if (!this.spanDropCriterion.isEmpty()) {
      LOG.info("Span drop criterion: {}", this.spanDropCriterion);
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
  public boolean shouldDropSpan(Map<String, KeyValue> tags) {
    return this.spanDropCriterion.stream()
        .anyMatch(
            l ->
                l.stream()
                    .allMatch(
                        p ->
                            tags.containsKey(p.getLeft())
                                && StringUtils.equals(
                                    tags.get(p.getLeft()).getVStr(), p.getRight())));
  }
}
