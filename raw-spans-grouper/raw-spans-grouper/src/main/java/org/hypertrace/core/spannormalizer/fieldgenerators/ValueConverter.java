package org.hypertrace.core.spannormalizer.fieldgenerators;

import io.jaegertracing.api_v2.JaegerSpanInternalModel;

public class ValueConverter {
  public static String getString(JaegerSpanInternalModel.KeyValue keyValue) {
    switch (keyValue.getVType()) {
      case STRING:
        return keyValue.getVStr();
      case BOOL:
        return String.valueOf(keyValue.getVBool());
      case INT64:
        return String.valueOf(keyValue.getVInt64());
      case FLOAT64:
        return String.valueOf(keyValue.getVFloat64());
      case BINARY:
        // Convert to UTF-8 String for now.
        return keyValue.getVBinary().toStringUtf8();
      default:
        throw new UnsupportedOperationException(
            "ValueConverter cannot convert UNRECOGNIZED to String for KeyValue with key:"
                + keyValue.getKey());
    }
  }

  public static int getInteger(JaegerSpanInternalModel.KeyValue keyValue) {
    switch (keyValue.getVType()) {
      case STRING:
        return Integer.parseInt(keyValue.getVStr().trim());
      case BOOL:
        throw new UnsupportedOperationException(
            "ValueConverter cannot convert BOOL to Integer for KeyValue with key:"
                + keyValue.getKey());
      case INT64:
        return (int) keyValue.getVInt64();
      case FLOAT64:
        return (int) keyValue.getVFloat64();
      case BINARY:
        throw new UnsupportedOperationException(
            "ValueConverter cannot convert BINARY to Integer for KeyValue with key:"
                + keyValue.getKey());
      default:
        throw new UnsupportedOperationException(
            "ValueConverter cannot convert UNRECOGNIZED to Integer for KeyValue with key:"
                + keyValue.getKey());
    }
  }

  public static boolean getBoolean(JaegerSpanInternalModel.KeyValue keyValue) {
    switch (keyValue.getVType()) {
      case STRING:
        return Boolean.parseBoolean(keyValue.getVStr().trim());
      case BOOL:
        return keyValue.getVBool();
      case INT64:
        return keyValue.getVInt64() > 0;
      case FLOAT64:
        return keyValue.getVFloat64() > 0;
      case BINARY:
        throw new UnsupportedOperationException(
            "ValueConverter cannot convert BINARY to Boolean for KeyValue with key:"
                + keyValue.getKey());
      default:
        throw new UnsupportedOperationException(
            "ValueConverter cannot convert UNRECOGNIZED to Boolean for KeyValue with key:"
                + keyValue.getKey());
    }
  }
}
