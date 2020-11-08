package org.hypertrace.core.rawspansgrouper;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.hypertrace.core.datamodel.RawSpans;
import org.hypertrace.core.spannormalizer.TraceIdentity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TraceEmitPunctuatorTest {

  private TraceEmitPunctuator underTest;
  private ProcessorContext context;
  private KeyValueStore<TraceIdentity, ValueAndTimestamp<RawSpans>> inflightTraceStore;
  private KeyValueStore<TraceIdentity, Long> traceEmitTriggerStore;
  private To outputTopicProducer;

  @BeforeEach
  public void setUp() {
    context = mock(ProcessorContext.class);
    inflightTraceStore = mock(KeyValueStore.class);
    traceEmitTriggerStore = mock(KeyValueStore.class);
    outputTopicProducer = mock(To.class);
    underTest = new TraceEmitPunctuator(
        TraceIdentity.newBuilder().setTenantId("__default").setTraceId(ByteBuffer.wrap("trace-1".getBytes())).build(), context, inflightTraceStore, traceEmitTriggerStore,
        outputTopicProducer, 100, -1);
    underTest.setCancellable(mock(Cancellable.class));
  }

  @Test
  public void whenPunctuatorIsRescheduledExpectNoChangesToTraceEmitTriggerStore() {
    when(traceEmitTriggerStore.get(eq(TraceIdentity.newBuilder().setTenantId("__default").setTraceId(ByteBuffer.wrap("trace-1".getBytes())).build()))).thenReturn(500l);
    underTest.punctuate(200);
    // the above when() call should be the only interaction
    verify(traceEmitTriggerStore, times(1));
  }

  @Test
  public void whenTraceIsEmittedExpectDeleteOperationOnBothStores() {
    when(traceEmitTriggerStore.get(eq(TraceIdentity.newBuilder().setTenantId("__default").setTraceId(ByteBuffer.wrap("trace-1".getBytes())).build()))).thenReturn(100l);
    RawSpans rawSpans = RawSpans.newBuilder().build();
    ValueAndTimestamp<RawSpans> agg = ValueAndTimestamp.make(rawSpans, 300l);
    when(inflightTraceStore.delete(eq(TraceIdentity.newBuilder().setTenantId("__default").setTraceId(ByteBuffer.wrap("trace-1".getBytes())).build()))).thenReturn(agg);
    underTest.punctuate(200);
    // the above when() call should be the only interaction
    verify(traceEmitTriggerStore).delete(eq(TraceIdentity.newBuilder().setTenantId("__default").setTraceId(ByteBuffer.wrap("trace-1".getBytes())).build()));
    verify(inflightTraceStore).delete(eq(TraceIdentity.newBuilder().setTenantId("__default").setTraceId(ByteBuffer.wrap("trace-1".getBytes())).build()));
  }
}