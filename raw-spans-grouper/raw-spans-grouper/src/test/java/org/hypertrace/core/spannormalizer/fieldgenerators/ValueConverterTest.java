package org.hypertrace.core.spannormalizer.fieldgenerators;

import com.google.protobuf.ByteString;
import io.jaegertracing.api_v2.JaegerSpanInternalModel;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ValueConverterTest {
  @Test
  public void testStringConversion() {
    Assertions.assertEquals(
        "test-str",
        ValueConverter.getString(
            JaegerSpanInternalModel.KeyValue.newBuilder()
                .setVStr("test-str")
                .setVType(JaegerSpanInternalModel.ValueType.STRING)
                .build()));
    Assertions.assertEquals(
        "true",
        ValueConverter.getString(
            JaegerSpanInternalModel.KeyValue.newBuilder()
                .setVBool(true)
                .setVType(JaegerSpanInternalModel.ValueType.BOOL)
                .build()));
    Assertions.assertEquals(
        "23",
        ValueConverter.getString(
            JaegerSpanInternalModel.KeyValue.newBuilder()
                .setVInt64(23)
                .setVType(JaegerSpanInternalModel.ValueType.INT64)
                .build()));
    Assertions.assertEquals(
        "23.314",
        ValueConverter.getString(
            JaegerSpanInternalModel.KeyValue.newBuilder()
                .setVFloat64(23.314)
                .setVType(JaegerSpanInternalModel.ValueType.FLOAT64)
                .build()));
    Assertions.assertEquals(
        "some-byte-string",
        ValueConverter.getString(
            JaegerSpanInternalModel.KeyValue.newBuilder()
                .setVBinary(ByteString.copyFrom("some-byte-string".getBytes()))
                .setVType(JaegerSpanInternalModel.ValueType.BINARY)
                .build()));
  }

  @Test
  public void testIntegerConversion() {
    Assertions.assertEquals(
        67,
        ValueConverter.getInteger(
            JaegerSpanInternalModel.KeyValue.newBuilder()
                .setVStr("67")
                .setVType(JaegerSpanInternalModel.ValueType.STRING)
                .build()));
    Assertions.assertEquals(
        23,
        ValueConverter.getInteger(
            JaegerSpanInternalModel.KeyValue.newBuilder()
                .setVInt64(23)
                .setVType(JaegerSpanInternalModel.ValueType.INT64)
                .build()));
    Assertions.assertEquals(
        17,
        ValueConverter.getInteger(
            JaegerSpanInternalModel.KeyValue.newBuilder()
                .setVFloat64(17.0)
                .setVType(JaegerSpanInternalModel.ValueType.FLOAT64)
                .build()));
    Assertions.assertEquals(
        18,
        ValueConverter.getInteger(
            JaegerSpanInternalModel.KeyValue.newBuilder()
                .setVFloat64(18.3)
                .setVType(JaegerSpanInternalModel.ValueType.FLOAT64)
                .build()));
  }

  @Test
  public void testBooleanConversion() {
    Assertions.assertTrue(
        ValueConverter.getBoolean(
            JaegerSpanInternalModel.KeyValue.newBuilder()
                .setVBool(true)
                .setVType(JaegerSpanInternalModel.ValueType.BOOL)
                .build()));
    Assertions.assertFalse(
        ValueConverter.getBoolean(
            JaegerSpanInternalModel.KeyValue.newBuilder()
                .setVStr("false")
                .setVType(JaegerSpanInternalModel.ValueType.STRING)
                .build()));
    Assertions.assertFalse(
        ValueConverter.getBoolean(
            JaegerSpanInternalModel.KeyValue.newBuilder()
                .setVStr("some-str")
                .setVType(JaegerSpanInternalModel.ValueType.STRING)
                .build()));
    Assertions.assertTrue(
        ValueConverter.getBoolean(
            JaegerSpanInternalModel.KeyValue.newBuilder()
                .setVInt64(65L)
                .setVType(JaegerSpanInternalModel.ValueType.INT64)
                .build()));
    Assertions.assertFalse(
        ValueConverter.getBoolean(
            JaegerSpanInternalModel.KeyValue.newBuilder()
                .setVFloat64(-1000.89)
                .setVType(JaegerSpanInternalModel.ValueType.FLOAT64)
                .build()));
  }

  @Test
  public void testConvertingBoolToInteger() {
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () ->
            ValueConverter.getInteger(
                JaegerSpanInternalModel.KeyValue.newBuilder()
                    .setVType(JaegerSpanInternalModel.ValueType.BOOL)
                    .build()));
  }

  @Test
  public void testConvertingBinaryToInteger() {
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () ->
            ValueConverter.getInteger(
                JaegerSpanInternalModel.KeyValue.newBuilder()
                    .setVType(JaegerSpanInternalModel.ValueType.BINARY)
                    .build()));
  }

  @Test
  public void testConvertingBinaryToBoolean() {
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () ->
            ValueConverter.getBoolean(
                JaegerSpanInternalModel.KeyValue.newBuilder()
                    .setVType(JaegerSpanInternalModel.ValueType.BINARY)
                    .build()));
  }
}
