package org.hypertrace.core.spannormalizer.jaeger;

import static org.hypertrace.core.datamodel.shared.AvroBuilderCache.fastNewBuilder;
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

  Optional<Resource> normalize(Span span, Optional<String> tenantIdKey) {
    return Optional.of(span.getProcess())
        .map(Process::getTagsList)
        .flatMap(keyValueList -> buildResource(keyValueList, tenantIdKey));
  }

  private Optional<Resource> buildResource(
      List<KeyValue> keyValueList, Optional<String> tenantIdKey) {
    return keyValueList.stream()
        .filter(kv -> tenantIdKey.isEmpty() || !tenantIdKey.get().equalsIgnoreCase(kv.getKey()))
        .map(this::buildResourceValue)
        .collect(Collectors.collectingAndThen(MAP_COLLECTOR, this::buildResource));
  }

  private Optional<Resource> buildResource(Map<String, AttributeValue> resourceValueMap) {
    if (resourceValueMap.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(
        fastNewBuilder(Resource.Builder.class)
            .setAttributesBuilder(
                fastNewBuilder(Attributes.Builder.class).setAttributeMap(resourceValueMap))
            .build());
  }

  private Entry<String, AttributeValue> buildResourceValue(KeyValue keyValue) {
    return Map.entry(keyValue.getKey().toLowerCase(), createFromJaegerKeyValue(keyValue));
  }
}
