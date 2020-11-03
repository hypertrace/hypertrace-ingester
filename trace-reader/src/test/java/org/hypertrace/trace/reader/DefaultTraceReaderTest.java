package org.hypertrace.trace.reader;

import static org.hypertrace.trace.reader.AvroUtil.buildAttributesWithKeyValue;
import static org.hypertrace.trace.reader.AvroUtil.defaultedEventBuilder;
import static org.hypertrace.trace.reader.AvroUtil.defaultedStructuredTraceBuilder;
import static org.hypertrace.trace.reader.LiteralValueUtil.stringLiteral;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.reactivex.rxjava3.core.Single;
import org.hypertrace.core.attribute.service.cachingclient.CachingAttributeClient;
import org.hypertrace.core.attribute.service.v1.AttributeDefinition;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeType;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DefaultTraceReaderTest {

  @Mock CachingAttributeClient mockAttributeClient;
  private TraceReader traceReader;

  @BeforeEach
  void beforeEach() {
    this.traceReader = TraceReader.build(this.mockAttributeClient);
  }

  @Test
  void canReadSpanValues() {
    AttributeMetadata metadata =
        AttributeMetadata.newBuilder()
            .setScopeString("TEST_SCOPE")
            .setType(AttributeType.ATTRIBUTE)
            .setValueKind(AttributeKind.TYPE_STRING)
            .setDefinition(AttributeDefinition.newBuilder().setSourcePath("attrPath").build())
            .build();
    when(this.mockAttributeClient.get("TEST_SCOPE", "key")).thenReturn(Single.just(metadata));

    Event span =
        defaultedEventBuilder()
            .setAttributes(buildAttributesWithKeyValue("attrPath", "attrValue"))
            .build();

    assertEquals(
        stringLiteral("attrValue"),
        this.traceReader
            .getSpanValue(mock(StructuredTrace.class), span, "TEST_SCOPE", "key")
            .blockingGet());
  }

  @Test
  void canReadTraceValues() {
    AttributeMetadata metadata =
        AttributeMetadata.newBuilder()
            .setScopeString("TRACE")
            .setType(AttributeType.ATTRIBUTE)
            .setValueKind(AttributeKind.TYPE_STRING)
            .setDefinition(AttributeDefinition.newBuilder().setSourcePath("attrPath").build())
            .build();
    when(this.mockAttributeClient.get("TRACE", "key")).thenReturn(Single.just(metadata));

    StructuredTrace trace =
        defaultedStructuredTraceBuilder()
            .setAttributes(buildAttributesWithKeyValue("attrPath", "attrValue"))
            .build();

    assertEquals(
        stringLiteral("attrValue"), this.traceReader.getTraceValue(trace, "key").blockingGet());
  }
}
