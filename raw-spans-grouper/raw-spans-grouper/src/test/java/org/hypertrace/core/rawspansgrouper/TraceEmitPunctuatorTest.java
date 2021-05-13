package org.hypertrace.core.rawspansgrouper;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.RawSpan;
import org.hypertrace.core.kafkastreams.framework.serdes.AvroSerde;
import org.hypertrace.core.spannormalizer.SpanIdentity;
import org.hypertrace.core.spannormalizer.TraceIdentity;
import org.hypertrace.core.spannormalizer.TraceState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TraceEmitPunctuatorTest {

  private TraceEmitPunctuator underTest;
  private WindowStore<SpanIdentity, byte[]> spanStore;
  private KeyValueStore<TraceIdentity, TraceState> traceStateStore;

  @BeforeEach
  public void setUp() {
    AvroSerde avroSerde = new AvroSerde();
    ProcessorContext context = mock(ProcessorContext.class);
    when(context.keySerde()).thenReturn(avroSerde);
    spanStore = mock(WindowStore.class);
    traceStateStore = mock(KeyValueStore.class);
    To outputTopicProducer = mock(To.class);
    underTest =
        new TraceEmitPunctuator(
            TraceIdentity.newBuilder()
                .setTenantId("__default")
                .setTraceId(ByteBuffer.wrap("trace-1".getBytes()))
                .build(),
            context,
            spanStore,
            traceStateStore,
            outputTopicProducer,
            100,
            -1);
    underTest.setCancellable(mock(Cancellable.class));
  }

  @Test
  public void whenPunctuatorIsRescheduledExpectNoChangesToTraceEmitTriggerStore() {
    when(traceStateStore.get(
            eq(
                TraceIdentity.newBuilder()
                    .setTenantId("__default")
                    .setTraceId(ByteBuffer.wrap("trace-1".getBytes()))
                    .build())))
        .thenReturn(
            TraceState.newBuilder()
                .setSpanIds(List.of(ByteBuffer.wrap("span-1".getBytes())))
                .setEmitTs(300)
                .setTraceStartTimestamp(150)
                .setTraceEndTimestamp(300)
                .setTenantId("tenant")
                .setTraceId(ByteBuffer.wrap("trace-1".getBytes()))
                .build());
    underTest.punctuate(200);
    // the above when() call should be the only interaction
    verify(traceStateStore, times(1)).get(any());
  }

  @Test
  public void whenTraceIsEmittedExpectDeleteOperationOnTraceStateStore() {

    when(traceStateStore.get(
            eq(
                TraceIdentity.newBuilder()
                    .setTenantId("__default")
                    .setTraceId(ByteBuffer.wrap("trace-1".getBytes()))
                    .build())))
        .thenReturn(
            TraceState.newBuilder()
                .setSpanIds(List.of(ByteBuffer.wrap("span-1".getBytes())))
                .setEmitTs(180)
                .setTraceStartTimestamp(100)
                .setTraceEndTimestamp(130)
                .setTenantId("tenant")
                .setTraceId(ByteBuffer.wrap("trace-1".getBytes()))
                .build());
    List<KeyValue<Windowed<SpanIdentity>, RawSpan>> list = new ArrayList<>();
    list.add(
        new KeyValue<>(
            new Windowed<>(
                SpanIdentity.newBuilder()
                    .setSpanId(ByteBuffer.wrap("span-1".getBytes()))
                    .setTraceId(ByteBuffer.wrap("trace-1".getBytes()))
                    .setTenantId("__default")
                    .build(),
                new TimeWindow(100, 130)),
            RawSpan.newBuilder()
                .setCustomerId("__default")
                .setEvent(
                    Event.newBuilder()
                        .setEventId(ByteBuffer.wrap("span-1".getBytes()))
                        .setCustomerId("__default")
                        .build())
                .setTraceId(ByteBuffer.wrap("trace-1".getBytes()))
                .build()));

    Iterator<KeyValue<Windowed<SpanIdentity>, RawSpan>> iterator = list.iterator();
    when(spanStore.fetch(any(), any(), any(), any()))
        .thenReturn(
            new KeyValueIterator<Windowed<SpanIdentity>, RawSpan>() {
              @Override
              public void close() {}

              @Override
              public Windowed<SpanIdentity> peekNextKey() {
                return null;
              }

              @Override
              public boolean hasNext() {
                return iterator.hasNext();
              }

              @Override
              public KeyValue<Windowed<SpanIdentity>, RawSpan> next() {
                return iterator.next();
              }
            });
    underTest.punctuate(200); // the above when() call should be the only interaction
    verify(traceStateStore, times(1)).get(any());
    verify(spanStore, times(1)).fetch(any(), any(), any(), any());
    verify(traceStateStore)
        .delete(
            eq(
                TraceIdentity.newBuilder()
                    .setTenantId("__default")
                    .setTraceId(ByteBuffer.wrap("trace-1".getBytes()))
                    .build()));
  }
}
