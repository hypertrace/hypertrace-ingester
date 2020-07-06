package org.hypertrace.core.spannormalizer.jaeger;

import io.jaegertracing.api_v2.JaegerSpanInternalModel.Span;
import java.io.IOException;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public class JaegerSpanSchema implements SerializationSchema<Span>, DeserializationSchema<Span> {

  @Override
  public Span deserialize(byte[] message) throws IOException {
    return Span.parseFrom(message);
  }

  @Override
  public boolean isEndOfStream(Span nextElement) {
    return false;
  }

  @Override
  public TypeInformation getProducedType() {
    return TypeInformation.of(Span.class);
  }

  @Override
  public byte[] serialize(Span element) {
    return element.toByteArray();
  }
}
