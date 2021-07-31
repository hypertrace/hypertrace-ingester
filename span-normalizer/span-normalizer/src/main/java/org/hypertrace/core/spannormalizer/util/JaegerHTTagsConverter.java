package org.hypertrace.core.spannormalizer.util;

import com.google.protobuf.ByteString;
import io.jaegertracing.api_v2.JaegerSpanInternalModel;
import org.hypertrace.core.datamodel.AttributeValue;

public class JaegerHTTagsConverter {
  private static final AttributeValue.Builder ATTRIBUTE_VALUE_BUILDER = AttributeValue.newBuilder();

  public static AttributeValue createFromJaegerKeyValue(JaegerSpanInternalModel.KeyValue keyValue) {
    // newBuilder() in Avro classes uses SpecificData.getForSchema which uses
    // reflection(Class.forName) for loading the class
    // This may be too expensive when being done at a large scale(like in the case of
    // AttributeValue).
    // By using an existing empty builder to create a new builder we are bypassing the reflection
    AttributeValue.Builder valueBuilder = AttributeValue.newBuilder(ATTRIBUTE_VALUE_BUILDER);
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

  public static JaegerSpanInternalModel.KeyValue convertAttributeToKeyValue(
      org.hypertrace.core.datamodel.AttributeValue attributeValue) {

    if (attributeValue.getValue() != null) {
      return JaegerSpanInternalModel.KeyValue.newBuilder()
          .setVType(JaegerSpanInternalModel.ValueType.STRING)
          .setVStr(attributeValue.getValue())
          .build();
    } else if (attributeValue.getBinaryValue() != null) {
      return JaegerSpanInternalModel.KeyValue.newBuilder()
          .setVType(JaegerSpanInternalModel.ValueType.BINARY)
          .setVBinary(ByteString.copyFrom(attributeValue.getBinaryValue()))
          .build();
    }

    return JaegerSpanInternalModel.KeyValue.newBuilder().build();
  }
}
