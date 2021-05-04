package org.hypertrace.core.rawspansgrouper;

class TraceEmitPunctuatorTest {
  /**
   * private TraceEmitPunctuator underTest; private ProcessorContext context; private
   * KeyValueStore<TraceIdentity, ValueAndTimestamp<RawSpans>> inflightTraceStore; private
   * KeyValueStore<TraceIdentity, Long> traceEmitTriggerStore; private To
   * outputTopicProducer; @BeforeEach public void setUp() { context = mock(ProcessorContext.class);
   * inflightTraceStore = mock(KeyValueStore.class); traceEmitTriggerStore =
   * mock(KeyValueStore.class); outputTopicProducer = mock(To.class); underTest = new
   * TraceEmitPunctuator( TraceIdentity.newBuilder() .setTenantId("__default")
   * .setTraceId(ByteBuffer.wrap("trace-1".getBytes())) .build(), context, inflightTraceStore,
   * traceEmitTriggerStore, outputTopicProducer, 100, -1);
   * underTest.setCancellable(mock(Cancellable.class)); } @Test public void
   * whenPunctuatorIsRescheduledExpectNoChangesToTraceEmitTriggerStore() {
   * when(traceEmitTriggerStore.get( eq( TraceIdentity.newBuilder() .setTenantId("__default")
   * .setTraceId(ByteBuffer.wrap("trace-1".getBytes())) .build()))) .thenReturn(500l);
   * underTest.punctuate(200); // the above when() call should be the only interaction
   * verify(traceEmitTriggerStore, times(1)); } @Test public void
   * whenTraceIsEmittedExpectDeleteOperationOnBothStores() { when(traceEmitTriggerStore.get( eq(
   * TraceIdentity.newBuilder() .setTenantId("__default")
   * .setTraceId(ByteBuffer.wrap("trace-1".getBytes())) .build()))) .thenReturn(100l); RawSpans
   * rawSpans = RawSpans.newBuilder().build(); ValueAndTimestamp<RawSpans> agg =
   * ValueAndTimestamp.make(rawSpans, 300l); when(inflightTraceStore.delete( eq(
   * TraceIdentity.newBuilder() .setTenantId("__default")
   * .setTraceId(ByteBuffer.wrap("trace-1".getBytes())) .build()))) .thenReturn(agg);
   * underTest.punctuate(200); // the above when() call should be the only interaction
   * verify(traceEmitTriggerStore) .delete( eq( TraceIdentity.newBuilder() .setTenantId("__default")
   * .setTraceId(ByteBuffer.wrap("trace-1".getBytes())) .build())); verify(inflightTraceStore)
   * .delete( eq( TraceIdentity.newBuilder() .setTenantId("__default")
   * .setTraceId(ByteBuffer.wrap("trace-1".getBytes())) .build())); } *
   */
}
