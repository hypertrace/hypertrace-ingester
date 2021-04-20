package org.hypertrace.trace.reader.attributes;

import static org.hypertrace.trace.reader.attributes.AvroUtil.buildAttributesWithKeyValue;
import static org.hypertrace.trace.reader.attributes.AvroUtil.buildAttributesWithKeyValues;
import static org.hypertrace.trace.reader.attributes.AvroUtil.buildMetricsWithKeyValue;
import static org.hypertrace.trace.reader.attributes.AvroUtil.defaultedEventBuilder;
import static org.hypertrace.trace.reader.attributes.AvroUtil.defaultedStructuredTraceBuilder;
import static org.hypertrace.trace.reader.attributes.LiteralValueUtil.longLiteral;
import static org.hypertrace.trace.reader.attributes.LiteralValueUtil.stringLiteral;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.reactivex.rxjava3.core.Single;
import java.util.Map;
import org.hypertrace.core.attribute.service.cachingclient.CachingAttributeClient;
import org.hypertrace.core.attribute.service.projection.AttributeProjectionRegistry;
import org.hypertrace.core.attribute.service.v1.AttributeDefinition;
import org.hypertrace.core.attribute.service.v1.AttributeDefinition.AttributeDefinitions;
import org.hypertrace.core.attribute.service.v1.AttributeDefinition.SourceField;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeType;
import org.hypertrace.core.attribute.service.v1.LiteralValue;
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
            .resolve(ValueSourceFactory.forSpan(this.mockStructuredTrace, span), metadata)
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
            .resolve(ValueSourceFactory.forSpan(this.mockStructuredTrace, span), metadata)
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
            .resolve(
                ValueSourceFactory.forSpan(this.mockStructuredTrace, mock(Event.class)), metadata)
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
            .resolve(ValueSourceFactory.forSpan(this.mockStructuredTrace, span), projectionMetadata)
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
            .resolve(ValueSourceFactory.forSpan(this.mockStructuredTrace, span), projectionMetadata)
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
            .resolve(ValueSourceFactory.forSpan(trace, mock(Event.class)), projectionMetadata)
            .blockingGet());
  }

  @Test
  void resolveFields() {
    Event span = defaultedEventBuilder().setStartTimeMillis(123).setEndTimeMillis(234).build();

    AttributeMetadata metadataStartTime =
        AttributeMetadata.newBuilder()
            .setScopeString("TEST_SCOPE")
            .setType(AttributeType.ATTRIBUTE)
            .setValueKind(AttributeKind.TYPE_INT64)
            .setDefinition(
                AttributeDefinition.newBuilder()
                    .setSourceField(SourceField.SOURCE_FIELD_START_TIME)
                    .build())
            .build();

    AttributeMetadata metadataEndTime =
        metadataStartTime.toBuilder()
            .setDefinition(
                AttributeDefinition.newBuilder()
                    .setSourceField(SourceField.SOURCE_FIELD_END_TIME)
                    .build())
            .build();

    assertEquals(
        longLiteral(123),
        this.resolver
            .resolve(ValueSourceFactory.forSpan(this.mockStructuredTrace, span), metadataStartTime)
            .blockingGet());
    assertEquals(
        longLiteral(234),
        this.resolver
            .resolve(ValueSourceFactory.forSpan(this.mockStructuredTrace, span), metadataEndTime)
            .blockingGet());
  }

  @Test
  void resolvesFirstAvailableDefinition() {
    AttributeMetadata metadata =
        AttributeMetadata.newBuilder()
            .setScopeString("TEST_SCOPE")
            .setType(AttributeType.ATTRIBUTE)
            .setValueKind(AttributeKind.TYPE_INT64)
            .setDefinition(
                AttributeDefinition.newBuilder()
                    .setFirstValuePresent(
                        AttributeDefinitions.newBuilder()
                            .addDefinitions( // Should error due to data type
                                AttributeDefinition.newBuilder().setSourcePath("path.to.string"))
                            .addDefinitions( // Should be empty and skipped
                                AttributeDefinition.newBuilder().setSourcePath("non.existent"))
                            .addDefinitions(
                                AttributeDefinition.newBuilder().setSourcePath("path.to.int"))
                            .addDefinitions( // Shouldn't be reached
                                AttributeDefinition.newBuilder()
                                    .setSourceField(SourceField.SOURCE_FIELD_START_TIME))))
            .build();

    Event span =
        defaultedEventBuilder()
            .setAttributes(
                buildAttributesWithKeyValues(Map.of("path.to.string", "foo", "path.to.int", "14")))
            .build();

    assertEquals(
        longLiteral(14),
        this.resolver
            .resolve(ValueSourceFactory.forSpan(this.mockStructuredTrace, span), metadata)
            .blockingGet());
  }

  @Test
  void resolvesEmptyIfNoDefinitionAvailable() {
    AttributeMetadata metadata =
        AttributeMetadata.newBuilder()
            .setScopeString("TEST_SCOPE")
            .setType(AttributeType.ATTRIBUTE)
            .setValueKind(AttributeKind.TYPE_INT64)
            .setDefinition(
                AttributeDefinition.newBuilder()
                    .setFirstValuePresent(
                        AttributeDefinitions.newBuilder()
                            .addDefinitions( // Should error due to data type
                                AttributeDefinition.newBuilder().setSourcePath("path.to.string"))
                            .addDefinitions( // Should be empty and skipped
                                AttributeDefinition.newBuilder().setSourcePath("non.existent"))))
            .build();

    Event span =
        defaultedEventBuilder()
            .setAttributes(
                buildAttributesWithKeyValues(Map.of("path.to.string", "foo", "path.to.int", "14")))
            .build();

    assertEquals(
        LiteralValue.getDefaultInstance(),
        this.resolver
            .resolve(ValueSourceFactory.forSpan(this.mockStructuredTrace, span), metadata)
            .blockingGet());
  }

  @Test
  void resolvesFirstAttributeProjection() {
    AttributeMetadata metadata =
        AttributeMetadata.newBuilder()
            .setScopeString("TEST_SCOPE")
            .setType(AttributeType.ATTRIBUTE)
            .setValueKind(AttributeKind.TYPE_INT64)
            .setDefinition(
                AttributeDefinition.newBuilder()
                    .setFirstValuePresent(
                        AttributeDefinitions.newBuilder()
                            .addDefinitions( // Should be empty and skipped
                                AttributeDefinition.newBuilder().setSourcePath("non.existent"))
                            .addDefinitions(
                                AttributeDefinition.newBuilder()
                                    .setProjection(
                                        Projection.newBuilder()
                                            .setLiteral(
                                                LiteralValue.newBuilder()
                                                    .setStringValue("expected-value"))))))
            .build();

    Event span = defaultedEventBuilder().build();

    assertEquals(
        stringLiteral("expected-value"),
        this.resolver
            .resolve(ValueSourceFactory.forSpan(this.mockStructuredTrace, span), metadata)
            .blockingGet());
  }

  @Test
  void resolvesNestedFirstAttribute() {
    AttributeMetadata metadata =
        AttributeMetadata.newBuilder()
            .setScopeString("TEST_SCOPE")
            .setType(AttributeType.ATTRIBUTE)
            .setValueKind(AttributeKind.TYPE_INT64)
            .setDefinition(
                AttributeDefinition.newBuilder()
                    .setFirstValuePresent(
                        AttributeDefinitions.newBuilder()
                            .addDefinitions( // Should be empty and skipped
                                AttributeDefinition.newBuilder().setSourcePath("non.existent"))
                            .addDefinitions(
                                AttributeDefinition.newBuilder()
                                    .setFirstValuePresent(
                                        AttributeDefinitions.newBuilder()
                                            .addDefinitions(
                                                AttributeDefinition.newBuilder()
                                                    .setSourcePath("non.existent.other"))
                                            .addDefinitions(
                                                AttributeDefinition.newBuilder()
                                                    .setSourceField(
                                                        SourceField.SOURCE_FIELD_START_TIME))))))
            .build();

    Event span = defaultedEventBuilder().setStartTimeMillis(13).build();

    assertEquals(
        longLiteral(13),
        this.resolver
            .resolve(ValueSourceFactory.forSpan(this.mockStructuredTrace, span), metadata)
            .blockingGet());
  }
}
