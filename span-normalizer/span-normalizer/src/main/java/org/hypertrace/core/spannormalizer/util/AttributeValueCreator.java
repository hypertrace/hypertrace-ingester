package org.hypertrace.core.spannormalizer.util;

import io.jaegertracing.api_v2.JaegerSpanInternalModel;
import org.hypertrace.core.datamodel.AttributeValue;

public class AttributeValueCreator {
  public static AttributeValue createFromJaegerKeyValue(JaegerSpanInternalModel.KeyValue keyValue) {
    AttributeValue.Builder valueBuilder = AttributeValue.newBuilder();
    switch (keyValue.getVType()) {
      case STRING:
        valueBuilder.setValue(keyValue.getVStr());
        break;
      case BOOL:
        valueBuilder.setValue(String.valueOf(keyValue.getVBool()));
        break;
      case INT64:
        valueBuilder.setValue(String.valueOf(keyValue.getVInt64()));
        break;
      case FLOAT64:
        valueBuilder.setValue(String.valueOf(keyValue.getVFloat64()));
        break;
      case BINARY:
        valueBuilder.setBinaryValue(keyValue.getVBinary().asReadOnlyByteBuffer());
        break;
      case UNRECOGNIZED:
        break;
    }

    return valueBuilder.build();
  }
}
