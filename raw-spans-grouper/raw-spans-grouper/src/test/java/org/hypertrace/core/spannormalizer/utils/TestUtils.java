package org.hypertrace.core.spannormalizer.utils;

import io.jaegertracing.api_v2.JaegerSpanInternalModel;

public class TestUtils {
  public static JaegerSpanInternalModel.KeyValue createKeyValue(String val) {
    return JaegerSpanInternalModel.KeyValue.newBuilder().setVStr(val).build();
  }

  public static JaegerSpanInternalModel.KeyValue createKeyValue(int val) {
    return JaegerSpanInternalModel.KeyValue.newBuilder()
        .setVInt64(val)
        .setVType(JaegerSpanInternalModel.ValueType.INT64)
        .build();
  }

  public static JaegerSpanInternalModel.KeyValue createKeyValue(String key, String val) {
    return JaegerSpanInternalModel.KeyValue.newBuilder()
        .setKey(key)
        .setVType(JaegerSpanInternalModel.ValueType.STRING)
        .setVStr(val)
        .build();
  }

  public static JaegerSpanInternalModel.KeyValue createKeyValue(String key, int val) {
    return JaegerSpanInternalModel.KeyValue.newBuilder()
        .setKey(key)
        .setVType(JaegerSpanInternalModel.ValueType.INT64)
        .setVInt64(val)
        .build();
  }
}
