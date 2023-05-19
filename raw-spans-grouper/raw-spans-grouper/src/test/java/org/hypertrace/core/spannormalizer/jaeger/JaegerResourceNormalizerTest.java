package org.hypertrace.core.spannormalizer.jaeger;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.jaegertracing.api_v2.JaegerSpanInternalModel.KeyValue;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.Process;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.Span;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;
import org.hypertrace.core.datamodel.Resource;
import org.junit.jupiter.api.Test;

class JaegerResourceNormalizerTest {
  private final JaegerResourceNormalizer normalizer = new JaegerResourceNormalizer();

  @Test
  void producesResourceWithAllKeys() {
    Resource createdResource =
        normalizer
            .normalize(
                buildInputSpanWithResourceAttributes(
                    List.of(
                        Map.entry("first", "first-value"),
                        Map.entry("second", "second-value"),
                        Map.entry("third", "third-value"))),
                Optional.empty())
            .orElseThrow();

    assertEquals(3, createdResource.getAttributes().getAttributeMap().size());
    assertEquals(
        "first-value", createdResource.getAttributes().getAttributeMap().get("first").getValue());
    assertEquals(
        "second-value", createdResource.getAttributes().getAttributeMap().get("second").getValue());
    assertEquals(
        "third-value", createdResource.getAttributes().getAttributeMap().get("third").getValue());
  }

  @Test
  void ignoresDuplicateKeys() {
    Resource createdResource =
        normalizer
            .normalize(
                buildInputSpanWithResourceAttributes(
                    List.of(Map.entry("foo", "bar"), Map.entry("foo", "baz"))),
                Optional.empty())
            .orElseThrow();

    assertEquals(1, createdResource.getAttributes().getAttributeMap().size());
    assertEquals("bar", createdResource.getAttributes().getAttributeMap().get("foo").getValue());
  }

  @Test
  void returnsEmptyOptionalIfNoResourceAttributes() {
    assertEquals(
        Optional.empty(),
        normalizer.normalize(
            buildInputSpanWithResourceAttributes(Collections.emptyList()), Optional.empty()));
  }

  @Test
  void returnsEmptyOptionalWithOnlyTenantIdKey() {
    assertEquals(
        Optional.empty(),
        normalizer.normalize(
            buildInputSpanWithResourceAttributes(List.of(Map.entry("tenant-key", "tenant-id"))),
            Optional.of("tenant-key")));
  }

  @Test
  void ignoresTenantIdKey() {
    Resource createdResource =
        normalizer
            .normalize(
                buildInputSpanWithResourceAttributes(
                    List.of(Map.entry("foo", "bar"), Map.entry("tenant-key", "tenant-id"))),
                Optional.of("tenant-key"))
            .orElseThrow();

    assertEquals(1, createdResource.getAttributes().getAttributeMap().size());
    assertEquals("bar", createdResource.getAttributes().getAttributeMap().get("foo").getValue());
  }

  // Take a list of pairs instead of a map so we can test dupe behavior
  Span buildInputSpanWithResourceAttributes(List<Entry<String, String>> resourceAttributes) {
    List<KeyValue> keyValues =
        resourceAttributes.stream()
            .map(
                entry ->
                    KeyValue.newBuilder().setKey(entry.getKey()).setVStr(entry.getValue()).build())
            .collect(Collectors.toList());
    return Span.newBuilder().setProcess(Process.newBuilder().addAllTags(keyValues)).build();
  }
}
