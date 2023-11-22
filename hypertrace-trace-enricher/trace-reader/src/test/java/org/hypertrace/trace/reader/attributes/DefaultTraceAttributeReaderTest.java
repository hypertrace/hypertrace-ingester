package org.hypertrace.trace.reader.attributes;

import static org.hypertrace.trace.reader.attributes.AvroUtil.buildAttributesWithKeyValue;
import static org.hypertrace.trace.reader.attributes.AvroUtil.defaultedEventBuilder;
import static org.hypertrace.trace.reader.attributes.AvroUtil.defaultedStructuredTraceBuilder;
import static org.hypertrace.trace.reader.attributes.LiteralValueUtil.stringLiteral;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;
import org.hypertrace.core.attribute.service.client.AttributeServiceCachedClient;
import org.hypertrace.core.attribute.service.v1.AttributeDefinition;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeType;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DefaultTraceAttributeReaderTest {

  @Mock AttributeServiceCachedClient mockAttributeClient;
  private TraceAttributeReader<StructuredTrace, Event> traceAttributeReader;
  private static final ArgumentMatcher<RequestContext> MATCHING_TENANT_REQUEST_CONTEXT =
      arg ->
          arg.buildContextualKey()
              .equals(RequestContext.forTenantId("defaultCustomerId").buildContextualKey());

  @BeforeEach
  void beforeEach() {
    this.traceAttributeReader = TraceAttributeReaderFactory.build(this.mockAttributeClient);
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
    when(this.mockAttributeClient.get(
            argThat(MATCHING_TENANT_REQUEST_CONTEXT), eq("TEST_SCOPE"), eq("key")))
        .thenReturn(Optional.of(metadata));

    Event span =
        defaultedEventBuilder()
            .setAttributes(buildAttributesWithKeyValue("attrPath", "attrValue"))
            .build();

    assertEquals(
        stringLiteral("attrValue"),
        this.traceAttributeReader
            .getSpanValue(mock(StructuredTrace.class), span, "TEST_SCOPE", "key")
            .get());
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
    when(this.mockAttributeClient.get(
            argThat(MATCHING_TENANT_REQUEST_CONTEXT), eq("TRACE"), eq("key")))
        .thenReturn(Optional.of(metadata));

    StructuredTrace trace =
        defaultedStructuredTraceBuilder()
            .setAttributes(buildAttributesWithKeyValue("attrPath", "attrValue"))
            .build();

    assertEquals(
        stringLiteral("attrValue"), this.traceAttributeReader.getTraceValue(trace, "key").get());
  }
}
