package org.hypertrace.core.spannormalizer.rawspan;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.nio.ByteBuffer;
import java.util.Map;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.RawSpan;
import org.hypertrace.core.spannormalizer.TraceIdentity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ByPassPredicateTest {
  private static final String CUSTOMER_ID = "__default";

  @Test
  void testByPassPredicateWithKeyDefined() {
    Config jobConfig = ConfigFactory.parseMap(Map.of("processor.bypass.key", "test.bypass"));
    ByPassPredicate byPassPredicateUnderTest = new ByPassPredicate(jobConfig);

    TraceIdentity traceIdentity = TraceIdentity.newBuilder()
        .setTenantId(CUSTOMER_ID).setTraceId(ByteBuffer.wrap("trace-1".getBytes())).build();
    ByteBuffer span1 = ByteBuffer.wrap("span-1".getBytes());
    ByteBuffer span2 = ByteBuffer.wrap("span-2".getBytes());

    // test bypass key having true value
    RawSpan rawSpan = RawSpan.newBuilder()
        .setCustomerId(CUSTOMER_ID)
        .setTraceId(traceIdentity.getTraceId())
        .setEvent(Event.newBuilder()
            .setCustomerId(CUSTOMER_ID)
            .setEventId(span1)
            .setAttributes(Attributes.newBuilder()
                .setAttributeMap(Map.of("test.bypass",
                    AttributeValue.newBuilder().setValue("true").build()))
                .build())
            .build())
        .build();

    Assertions.assertTrue(byPassPredicateUnderTest.test(traceIdentity, rawSpan));

    // test bypass key having false value
    rawSpan = RawSpan.newBuilder()
        .setCustomerId(CUSTOMER_ID)
        .setTraceId(traceIdentity.getTraceId())
        .setEvent(Event.newBuilder()
            .setCustomerId(CUSTOMER_ID)
            .setEventId(span2)
            .setAttributes(Attributes.newBuilder()
                .setAttributeMap(Map.of("test.bypass",
                    AttributeValue.newBuilder().setValue("false").build()))
                .build())
            .build())
        .build();

    Assertions.assertFalse(byPassPredicateUnderTest.test(traceIdentity, rawSpan));
  }

  @Test
  void testByPassPredicateWhenKeyIsNotDefined() {
    Config jobConfig = ConfigFactory.parseMap(Map.of());
    ByPassPredicate byPassPredicateUnderTest = new ByPassPredicate(jobConfig);

    TraceIdentity traceIdentity = TraceIdentity.newBuilder()
        .setTenantId(CUSTOMER_ID).setTraceId(ByteBuffer.wrap("trace-2".getBytes())).build();
    ByteBuffer span1 = ByteBuffer.wrap("span-1".getBytes());
    ByteBuffer span2 = ByteBuffer.wrap("span-2".getBytes());

    // tags won't matter as bypass.key is not defined
    RawSpan rawSpan = RawSpan.newBuilder()
        .setCustomerId(CUSTOMER_ID)
        .setTraceId(traceIdentity.getTraceId())
        .setEvent(Event.newBuilder()
            .setCustomerId(CUSTOMER_ID)
            .setEventId(span1)
            .setAttributes(Attributes.newBuilder()
                .setAttributeMap(Map.of("test.bypass",
                    AttributeValue.newBuilder().setValue("true").build()))
                .build())
            .build())
        .build();

    Assertions.assertFalse(byPassPredicateUnderTest.test(traceIdentity, rawSpan));

    // tags won't matter as bypass.key is not defined
    rawSpan = RawSpan.newBuilder()
        .setCustomerId(CUSTOMER_ID)
        .setTraceId(traceIdentity.getTraceId())
        .setEvent(Event.newBuilder()
            .setCustomerId(CUSTOMER_ID)
            .setEventId(span2)
            .setAttributes(Attributes.newBuilder()
                .setAttributeMap(Map.of("test.bypass",
                    AttributeValue.newBuilder().setValue("false").build()))
                .build())
            .build())
        .build();

    Assertions.assertFalse(byPassPredicateUnderTest.test(traceIdentity, rawSpan));
  }
}
