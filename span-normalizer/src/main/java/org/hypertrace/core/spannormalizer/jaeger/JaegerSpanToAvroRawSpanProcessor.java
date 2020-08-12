package org.hypertrace.core.spannormalizer.jaeger;

import com.typesafe.config.Config;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.Span;
import java.io.IOException;
import java.io.ObjectInputStream;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.hypertrace.core.datamodel.RawSpan;

/**
 * The Processor function to convert a protobuf based JaegerSpan to RawSpan in Avro format. Not that
 * due to the type infer issue, we have to make OUT Type as Object instead of RawSpan.
 */
public class JaegerSpanToAvroRawSpanProcessor extends ProcessFunction<Span, Object> {

  /**
   * Keep the config in this because the entire ProcessorFunction implementation needs to be
   * serializable.
   */
  @SuppressWarnings("FieldCanBeLocal")
  private final Config config;

  // Any state that's not serializable should be marked as transient.
  private transient JaegerSpanNormalizer converter;

  public JaegerSpanToAvroRawSpanProcessor(Config config) {
    this.config = config;
    initTransients();
  }

  private void initTransients() {
    this.converter = JaegerSpanNormalizer.get(config);
  }

  @Override
  public void processElement(Span jaegerSpan, Context ctx, Collector<Object> out) throws Exception {
    RawSpan rawSpan = converter.convert(jaegerSpan);
    if (rawSpan != null) {
      out.collect(rawSpan);
    }
  }

  /**
   * This method actually deserializes the object when this is sent over the wire by Flink. This is
   * required so that the transient fields are initialized properly.
   */
  private void readObject(ObjectInputStream inputStream)
      throws IOException, ClassNotFoundException {
    inputStream.defaultReadObject();
    initTransients();
  }
}
