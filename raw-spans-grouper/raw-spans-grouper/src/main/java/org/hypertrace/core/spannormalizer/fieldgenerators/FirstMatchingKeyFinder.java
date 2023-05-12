package org.hypertrace.core.spannormalizer.fieldgenerators;

import io.jaegertracing.api_v2.JaegerSpanInternalModel;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

public class FirstMatchingKeyFinder {
  public static Optional<String> getStringValueByFirstMatchingKey(
      Map<String, JaegerSpanInternalModel.KeyValue> tagsMap,
      List<String> tagKeys,
      Predicate<String> valueIsValid) {
    return getValueByFirstMatchingKey(tagsMap, tagKeys, ValueConverter::getString, valueIsValid);
  }

  public static Optional<String> getStringValueByFirstMatchingKey(
      Map<String, JaegerSpanInternalModel.KeyValue> tagsMap, List<String> tagKeys) {
    return getStringValueByFirstMatchingKey(tagsMap, tagKeys, s -> true);
  }

  public static Optional<Integer> getIntegerValueByFirstMatchingKey(
      Map<String, JaegerSpanInternalModel.KeyValue> tagsMap, List<String> tagKeys) {
    return getValueByFirstMatchingKey(tagsMap, tagKeys, ValueConverter::getInteger, v -> true);
  }

  private static <T> Optional<T> getValueByFirstMatchingKey(
      Map<String, JaegerSpanInternalModel.KeyValue> tagsMap,
      List<String> tagKeys,
      Function<JaegerSpanInternalModel.KeyValue, T> valueGetter,
      Predicate<T> valueIsValid) {
    for (String key : tagKeys) {
      JaegerSpanInternalModel.KeyValue keyValue = tagsMap.get(key);
      if (keyValue != null) {
        T val = valueGetter.apply(keyValue);
        if (valueIsValid.test(val)) {
          return Optional.of(val);
        }
      }
    }

    return Optional.empty();
  }
}
