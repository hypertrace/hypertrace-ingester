package org.hypertrace.core.spannormalizer.jaeger;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import io.jaegertracing.api_v2.JaegerSpanInternalModel;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.Log;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.Span;
import org.hypertrace.core.datamodel.LogEvents;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class JaegerSpanToLogRecordsTransformerTest {

  @Test
  void testBuildLogEventRecords() {
    Span span =
        Span.newBuilder()
            .setSpanId(ByteString.copyFrom("1".getBytes()))
            .setTraceId(ByteString.copyFrom("trace-1".getBytes()))
            .addTags(
                JaegerSpanInternalModel.KeyValue.newBuilder()
                    .setKey("jaeger.servicename")
                    .setVStr("SERVICE_NAME")
                    .build())
            .addLogs(
                Log.newBuilder()
                    .setTimestamp(Timestamp.newBuilder().setSeconds(5).build())
                    .addFields(
                        JaegerSpanInternalModel.KeyValue.newBuilder()
                            .setKey("e1")
                            .setVStr("some event detail")
                            .build())
                    .addFields(
                        JaegerSpanInternalModel.KeyValue.newBuilder()
                            .setKey("e2")
                            .setVStr("some event detail")
                            .build()))
            .addLogs(
                Log.newBuilder()
                    .setTimestamp(Timestamp.newBuilder().setSeconds(10).build())
                    .addFields(
                        JaegerSpanInternalModel.KeyValue.newBuilder()
                            .setKey("z2")
                            .setVStr("some event detail")
                            .build()))
            .build();

    LogEvents logEvents = new JaegerSpanToLogRecordsTransformer().buildLogEventRecords(span, "tenant");
    Assertions.assertEquals(2, logEvents.getLogEvents().size());
    Assertions.assertEquals(2, logEvents.getLogEvents().get(0).getAttributes().getAttributeMap().size());
    Assertions.assertEquals(1, logEvents.getLogEvents().get(1).getAttributes().getAttributeMap().size());
  }
}