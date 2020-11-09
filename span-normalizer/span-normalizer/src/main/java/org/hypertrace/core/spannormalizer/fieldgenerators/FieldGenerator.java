package org.hypertrace.core.spannormalizer.fieldgenerators;

import io.jaegertracing.api_v2.JaegerSpanInternalModel;
import java.util.Map;

/**
 * This is an interface that contains a single method run() whose implementation will contain the
 * logic to turn a keyValue into a field in the builder. An example of how to use this is to
 * implement a lambda. eg (key, keyValue, builder, tagsMap) -> builder.turnIntoPath(builder,
 * keyValue, tagsMap)
 *
 * @param <T> which is an avro builder .
 */
interface FieldGenerator<T> {
  void run(
      String key,
      JaegerSpanInternalModel.KeyValue keyValue,
      T builder,
      Map<String, JaegerSpanInternalModel.KeyValue> tagsMap);
}
