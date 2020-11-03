package org.hypertrace.trace.reader;

import static org.hypertrace.trace.reader.AvroUtil.buildAttributesWithKeyValue;
import static org.hypertrace.trace.reader.AvroUtil.buildMetricsWithKeyValue;
import static org.hypertrace.trace.reader.AvroUtil.defaultedEventBuilder;
import static org.hypertrace.trace.reader.AvroUtil.defaultedStructuredTraceBuilder;
import static org.hypertrace.trace.reader.LiteralValueUtil.longLiteral;
import static org.hypertrace.trace.reader.LiteralValueUtil.stringLiteral;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.reactivex.rxjava3.core.Single;
import org.hypertrace.core.attribute.service.cachingclient.CachingAttributeClient;
import org.hypertrace.core.attribute.service.projection.AttributeProjectionRegistry;
import org.hypertrace.core.attribute.service.v1.AttributeDefinition;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeType;
import org.hypertrace.core.attribute.service.v1.Projection;
import org.hypertrace.core.attribute.service.v1.ProjectionExpression;
import org.hypertrace.core.attribute.service.v1.ProjectionOperator;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DefaultValueResolverTest {

  @Mock CachingAttributeClient mockAttributeClient;
  @Mock StructuredTrace mockStructuredTrace;

  private DefaultValueResolver resolver;

  @BeforeEach
  void beforeEach() {
    this.resolver =
        new DefaultValueResolver(this.mockAttributeClient, new AttributeProjectionRegistry());
  }

  @Test
  void resolvesAttributes() {
    AttributeMetadata metadata =
        AttributeMetadata.newBuilder()
            .setScopeString("TEST_SCOPE")
            .setType(AttributeType.ATTRIBUTE)
            .setValueKind(AttributeKind.TYPE_STRING)
            .setDefinition(AttributeDefinition.newBuilder().setSourcePath("attrPath").build())
            .build();

    Event span =
        defaultedEventBuilder()
            .setAttributes(buildAttributesWithKeyValue("attrPath", "attrValue"))
            .build();

    assertEquals(
        stringLiteral("attrValue"),
        this.resolver
            .resolve(ValueSource.forSpan(this.mockStructuredTrace, span), metadata)
            .blockingGet());
  }

  @Test
  void resolvesMetrics() {
    AttributeMetadata metadata =
        AttributeMetadata.newBuilder()
            .setScopeString("TEST_SCOPE")
            .setType(AttributeType.METRIC)
            .setValueKind(AttributeKind.TYPE_INT64)
            .setDefinition(AttributeDefinition.newBuilder().setSourcePath("metricPath").build())
            .build();

    Event span =
        defaultedEventBuilder().setMetrics(buildMetricsWithKeyValue("metricPath", 42)).build();

    assertEquals(
        longLiteral(42),
        this.resolver
            .resolve(ValueSource.forSpan(this.mockStructuredTrace, span), metadata)
            .blockingGet());
  }

  @Test
  void resolvesLiteralProjections() {
    AttributeMetadata metadata =
        AttributeMetadata.newBuilder()
            .setDefinition(
                AttributeDefinition.newBuilder()
                    .setProjection(Projection.newBuilder().setLiteral(stringLiteral("projection"))))
            .build();

    assertEquals(
        stringLiteral("projection"),
        this.resolver
            .resolve(ValueSource.forSpan(this.mockStructuredTrace, mock(Event.class)), metadata)
            .blockingGet());
  }

  @Test
  void resolvesAttributeProjections() {
    AttributeMetadata projectionMetadata =
        AttributeMetadata.newBuilder()
            .setDefinition(
                AttributeDefinition.newBuilder()
                    .setProjection(Projection.newBuilder().setAttributeId("TEST_SCOPE.other")))
            .build();

    AttributeMetadata otherMetadata =
        AttributeMetadata.newBuilder()
            .setScopeString("TEST_SCOPE")
            .setType(AttributeType.METRIC)
            .setValueKind(AttributeKind.TYPE_INT64)
            .setDefinition(AttributeDefinition.newBuilder().setSourcePath("metricPath").build())
            .build();
    when(this.mockAttributeClient.get("TEST_SCOPE.other")).thenReturn(Single.just(otherMetadata));

    Event span =
        defaultedEventBuilder().setMetrics(buildMetricsWithKeyValue("metricPath", 42)).build();

    assertEquals(
        longLiteral(42),
        this.resolver
            .resolve(ValueSource.forSpan(this.mockStructuredTrace, span), projectionMetadata)
            .blockingGet());
  }

  @Test
  void resolvesExpressionProjections() {
    AttributeMetadata projectionMetadata =
        AttributeMetadata.newBuilder()
            .setDefinition(
                AttributeDefinition.newBuilder()
                    .setProjection(
                        Projection.newBuilder()
                            .setExpression(
                                ProjectionExpression.newBuilder()
                                    .setOperator(ProjectionOperator.PROJECTION_OPERATOR_CONCAT)
                                    .addArguments(
                                        Projection.newBuilder().setAttributeId("TEST_SCOPE.first"))
                                    .addArguments(
                                        Projection.newBuilder()
                                            .setAttributeId("TEST_SCOPE.second")))))
            .build();

    AttributeMetadata firstMetadata =
        AttributeMetadata.newBuilder()
            .setScopeString("TEST_SCOPE")
            .setType(AttributeType.METRIC)
            .setValueKind(AttributeKind.TYPE_INT64)
            .setDefinition(AttributeDefinition.newBuilder().setSourcePath("metricPath").build())
            .build();
    AttributeMetadata secondMetadata =
        AttributeMetadata.newBuilder()
            .setScopeString("TEST_SCOPE")
            .setType(AttributeType.ATTRIBUTE)
            .setValueKind(AttributeKind.TYPE_STRING)
            .setDefinition(AttributeDefinition.newBuilder().setSourcePath("attrPath").build())
            .build();
    when(this.mockAttributeClient.get("TEST_SCOPE.first")).thenReturn(Single.just(firstMetadata));
    when(this.mockAttributeClient.get("TEST_SCOPE.second")).thenReturn(Single.just(secondMetadata));

    Event span =
        defaultedEventBuilder()
            .setMetrics(buildMetricsWithKeyValue("metricPath", 42))
            .setAttributes(buildAttributesWithKeyValue("attrPath", "coolString"))
            .build();

    assertEquals(
        stringLiteral("42coolString"),
        this.resolver
            .resolve(ValueSource.forSpan(this.mockStructuredTrace, span), projectionMetadata)
            .blockingGet());
  }

  @Test
  void resolvesProjectionsAcrossScopes() {
    AttributeMetadata projectionMetadata =
        AttributeMetadata.newBuilder()
            .setDefinition(
                AttributeDefinition.newBuilder()
                    .setProjection(Projection.newBuilder().setAttributeId("TRACE.other")))
            .build();

    AttributeMetadata otherMetadata =
        AttributeMetadata.newBuilder()
            .setScopeString("TRACE")
            .setType(AttributeType.METRIC)
            .setValueKind(AttributeKind.TYPE_INT64)
            .setDefinition(AttributeDefinition.newBuilder().setSourcePath("metricPath").build())
            .build();
    when(this.mockAttributeClient.get("TRACE.other")).thenReturn(Single.just(otherMetadata));

    StructuredTrace trace =
        defaultedStructuredTraceBuilder()
            .setMetrics(buildMetricsWithKeyValue("metricPath", 42))
            .build();

    assertEquals(
        longLiteral(42),
        this.resolver
            .resolve(ValueSource.forSpan(trace, mock(Event.class)), projectionMetadata)
            .blockingGet());
  }
}
