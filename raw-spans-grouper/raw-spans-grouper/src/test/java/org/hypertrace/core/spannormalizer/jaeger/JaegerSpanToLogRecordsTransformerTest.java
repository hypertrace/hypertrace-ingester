package org.hypertrace.core.spannormalizer.jaeger;

import static org.hypertrace.core.spannormalizer.util.EventBuilder.buildEvent;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.typesafe.config.ConfigFactory;
import io.jaegertracing.api_v2.JaegerSpanInternalModel;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.Log;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.Span;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.hypertrace.core.datamodel.LogEvents;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class JaegerSpanToLogRecordsTransformerTest {

  @Test
  void testBuildLogEventRecords() {
    LogEvents logEvents =
        new JaegerSpanToLogRecordsTransformer().buildLogEventRecords(getTestSpan(), "tenant");
    Assertions.assertEquals(2, logEvents.getLogEvents().size());
    Assertions.assertEquals(
        2, logEvents.getLogEvents().get(0).getAttributes().getAttributeMap().size());
    Assertions.assertEquals(
        1, logEvents.getLogEvents().get(1).getAttributes().getAttributeMap().size());
  }

  @Test
  void testDropLogEventRecords() {
    Map<String, Object> configs = new HashMap<>();
    configs.putAll(
        Map.of(
            "processor",
            Map.of("tenantIdTagKey", "tenant-key", "excludeLogsTenantIds", List.of("tenant-1"))));

    ProcessorContext processorContext = Mockito.mock(ProcessorContext.class);
    Mockito.when(processorContext.appConfigs())
        .thenReturn(Map.of("raw-spans-grouper-job-config", ConfigFactory.parseMap(configs)));
    JaegerSpanToLogRecordsTransformer jaegerSpanToLogRecordsTransformer =
        new JaegerSpanToLogRecordsTransformer();
    jaegerSpanToLogRecordsTransformer.init(processorContext);
    KeyValue<String, LogEvents> keyValue =
        jaegerSpanToLogRecordsTransformer.transform(
            null,
            new PreProcessedSpan(
                "tenant-1",
                getTestSpan(),
                buildEvent("tenant-1", getTestSpan(), Optional.of("tenant-key"))));
    Assertions.assertNull(keyValue);
  }

  private Span getTestSpan() {
    return Span.newBuilder()
        .setSpanId(ByteString.copyFrom("1".getBytes()))
        .setTraceId(ByteString.copyFrom("trace-1".getBytes()))
        .addTags(
            JaegerSpanInternalModel.KeyValue.newBuilder()
                .setKey("jaeger.servicename")
                .setVStr("SERVICE_NAME")
                .setKey("")
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
  }
}
