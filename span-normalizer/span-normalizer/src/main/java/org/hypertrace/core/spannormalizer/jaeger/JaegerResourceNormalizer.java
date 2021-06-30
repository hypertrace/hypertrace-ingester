package org.hypertrace.core.spannormalizer.jaeger;

import static java.util.function.Predicate.not;
import static org.hypertrace.core.spannormalizer.util.JaegerHTTagsConverter.createFromJaegerKeyValue;

import io.jaegertracing.api_v2.JaegerSpanInternalModel.KeyValue;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.Process;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.Span;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.Resource;

class JaegerResourceNormalizer {
  // Shouldn't get any dupe keys, but in case we do, only read the first
  private static final Collector<Entry<String, AttributeValue>, ?, Map<String, AttributeValue>>
      MAP_COLLECTOR =
          Collectors.toUnmodifiableMap(Entry::getKey, Entry::getValue, (first, second) -> first);

  Optional<Resource> normalize(Span span) {
    return Optional.of(span.getProcess())
        .map(Process::getTagsList)
        .filter(not(List::isEmpty))
        .map(this::buildResource);
  }

  private Resource buildResource(List<KeyValue> keyValueList) {
    return keyValueList.stream()
        .map(this::buildResourceValue)
        .collect(Collectors.collectingAndThen(MAP_COLLECTOR, this::buildResource));
  }

  private Resource buildResource(Map<String, AttributeValue> resourceValueMap) {
    return Resource.newBuilder()
        .setAttributesBuilder(Attributes.newBuilder().setAttributeMap(resourceValueMap))
        .build();
  }

  private Entry<String, AttributeValue> buildResourceValue(KeyValue keyValue) {
    return Map.entry(keyValue.getKey().toLowerCase(), createFromJaegerKeyValue(keyValue));
  }
}
