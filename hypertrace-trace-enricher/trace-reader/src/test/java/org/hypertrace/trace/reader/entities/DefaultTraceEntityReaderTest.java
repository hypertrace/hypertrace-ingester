package org.hypertrace.trace.reader.entities;

import static org.hypertrace.trace.reader.attributes.AvroUtil.defaultedEventBuilder;
import static org.hypertrace.trace.reader.attributes.AvroUtil.defaultedStructuredTraceBuilder;
import static org.hypertrace.trace.reader.attributes.EntityUtil.buildAttributesWithKeyValues;
import static org.hypertrace.trace.reader.attributes.LiteralValueUtil.longLiteral;
import static org.hypertrace.trace.reader.attributes.LiteralValueUtil.stringLiteral;
import static org.hypertrace.trace.reader.entities.AttributeValueUtil.longAttributeValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Single;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.NoSuchElementException;
import org.hypertrace.core.attribute.service.cachingclient.CachingAttributeClient;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeSource;
import org.hypertrace.core.attribute.service.v1.AttributeType;
import org.hypertrace.core.attribute.service.v1.LiteralValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.entity.data.service.rxclient.EntityDataClient;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.data.service.v1.MergeAndUpsertEntityRequest.UpsertCondition;
import org.hypertrace.entity.data.service.v1.MergeAndUpsertEntityRequest.UpsertCondition.Predicate;
import org.hypertrace.entity.data.service.v1.MergeAndUpsertEntityRequest.UpsertCondition.Predicate.PredicateOperator;
import org.hypertrace.entity.type.service.rxclient.EntityTypeClient;
import org.hypertrace.entity.type.service.v2.EntityType;
import org.hypertrace.entity.type.service.v2.EntityType.EntityFormationCondition;
import org.hypertrace.trace.reader.attributes.TraceAttributeReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DefaultTraceEntityReaderTest {
  private static final String TENANT_ID = "tenant-id";
  private static final String TEST_ENTITY_TYPE_NAME = "ENTITY_TYPE_1";
  private static final String TEST_ENTITY_ID_ATTRIBUTE_KEY = "id";
  private static final String TEST_ENTITY_ID_ATTRIBUTE_VALUE = "id-value";
  private static final String TEST_ENTITY_NAME_ATTRIBUTE_KEY = "name";
  private static final String TEST_ENTITY_NAME_ATTRIBUTE_VALUE = "name-value";
  private static final String TEST_ENTITY_TIMESTAMP_ATTRIBUTE_KEY = "timestamp";
  private static final AttributeMetadata TEST_ENTITY_ID_ATTRIBUTE =
      AttributeMetadata.newBuilder()
          .setScopeString(TEST_ENTITY_TYPE_NAME)
          .setKey(TEST_ENTITY_ID_ATTRIBUTE_KEY)
          .addSources(AttributeSource.EDS)
          .setType(AttributeType.ATTRIBUTE)
          .build();
  private static final AttributeMetadata TEST_ENTITY_NAME_ATTRIBUTE =
      AttributeMetadata.newBuilder()
          .setScopeString(TEST_ENTITY_TYPE_NAME)
          .addSources(AttributeSource.EDS)
          .setKey(TEST_ENTITY_NAME_ATTRIBUTE_KEY)
          .setType(AttributeType.ATTRIBUTE)
          .build();
  private static final AttributeMetadata TEST_ENTITY_TIMESTAMP_ATTRIBUTE =
      AttributeMetadata.newBuilder()
          .setScopeString(TEST_ENTITY_TYPE_NAME)
          .addSources(AttributeSource.EDS)
          .setKey(TEST_ENTITY_TIMESTAMP_ATTRIBUTE_KEY)
          .setType(AttributeType.ATTRIBUTE)
          .build();
  private static final EntityType TEST_ENTITY_TYPE =
      EntityType.newBuilder()
          .setAttributeScope(TEST_ENTITY_TYPE_NAME)
          .setName(TEST_ENTITY_TYPE_NAME)
          .setIdAttributeKey(TEST_ENTITY_ID_ATTRIBUTE_KEY)
          .setNameAttributeKey(TEST_ENTITY_NAME_ATTRIBUTE_KEY)
          .build();

  private static final StructuredTrace TEST_TRACE = defaultedStructuredTraceBuilder().build();
  private static final Event TEST_SPAN = defaultedEventBuilder().setCustomerId(TENANT_ID).build();
  private static final Entity EXPECTED_ENTITY =
      Entity.newBuilder()
          .setEntityType(TEST_ENTITY_TYPE_NAME)
          .setEntityId(TEST_ENTITY_ID_ATTRIBUTE_VALUE)
          .setEntityName(TEST_ENTITY_NAME_ATTRIBUTE_VALUE)
          .putAllAttributes(
              buildAttributesWithKeyValues(
                  Map.of(
                      TEST_ENTITY_ID_ATTRIBUTE_KEY, TEST_ENTITY_ID_ATTRIBUTE_VALUE,
                      TEST_ENTITY_NAME_ATTRIBUTE_KEY, TEST_ENTITY_NAME_ATTRIBUTE_VALUE)))
          .build();

  @Mock EntityTypeClient mockTypeClient;
  @Mock EntityDataClient mockDataClient;
  @Mock CachingAttributeClient mockAttributeClient;
  @Mock TraceAttributeReader<StructuredTrace, Event> mockAttributeReader;

  private DefaultTraceEntityReader<StructuredTrace, Event> entityReader;

  @BeforeEach
  void beforeEach() {
    this.entityReader =
        new DefaultTraceEntityReader<>(
            this.mockTypeClient,
            this.mockDataClient,
            this.mockAttributeClient,
            this.mockAttributeReader,
            Duration.ofSeconds(15));
  }

  @Test
  void canReadAnEntity() {
    mockSingleEntityType();
    mockGetAllAttributes(TEST_ENTITY_ID_ATTRIBUTE, TEST_ENTITY_NAME_ATTRIBUTE);
    mockTenantId();
    mockAttributeRead(TEST_ENTITY_ID_ATTRIBUTE, stringLiteral(TEST_ENTITY_ID_ATTRIBUTE_VALUE));
    mockAttributeRead(TEST_ENTITY_NAME_ATTRIBUTE, stringLiteral(TEST_ENTITY_NAME_ATTRIBUTE_VALUE));
    mockEntityUpsert();

    assertEquals(
        EXPECTED_ENTITY,
        this.entityReader
            .getAssociatedEntityForSpan(TEST_ENTITY_TYPE_NAME, TEST_TRACE, TEST_SPAN)
            .blockingGet());
  }

  @Test
  void canReadAllEntities() {
    mockAllEntityTypes();
    mockGetAllAttributes(TEST_ENTITY_ID_ATTRIBUTE, TEST_ENTITY_NAME_ATTRIBUTE);
    mockTenantId();
    mockAttributeRead(TEST_ENTITY_ID_ATTRIBUTE, stringLiteral(TEST_ENTITY_ID_ATTRIBUTE_VALUE));
    mockAttributeRead(TEST_ENTITY_NAME_ATTRIBUTE, stringLiteral(TEST_ENTITY_NAME_ATTRIBUTE_VALUE));
    mockEntityUpsert();

    assertEquals(
        Map.of(TEST_ENTITY_TYPE_NAME, EXPECTED_ENTITY),
        this.entityReader.getAssociatedEntitiesForSpan(TEST_TRACE, TEST_SPAN).blockingGet());
  }

  @Test
  void omitsEntityBasedOnMissingAttributes() {
    mockSingleEntityType();
    mockGetAllAttributes(TEST_ENTITY_ID_ATTRIBUTE, TEST_ENTITY_NAME_ATTRIBUTE);
    mockTenantId();
    mockAttributeRead(TEST_ENTITY_ID_ATTRIBUTE, LiteralValue.getDefaultInstance());
    mockAttributeRead(TEST_ENTITY_NAME_ATTRIBUTE, stringLiteral(TEST_ENTITY_NAME_ATTRIBUTE_VALUE));

    assertTrue(
        this.entityReader
            .getAssociatedEntityForSpan(TEST_ENTITY_TYPE_NAME, TEST_TRACE, TEST_SPAN)
            .isEmpty()
            .blockingGet());
  }

  @Test
  void omitsUpsertConditionIfNoTimestampAttributeDefined() {
    mockSingleEntityType();
    mockGetAllAttributes(TEST_ENTITY_ID_ATTRIBUTE, TEST_ENTITY_NAME_ATTRIBUTE);
    mockTenantId();
    mockAttributeRead(TEST_ENTITY_ID_ATTRIBUTE, stringLiteral(TEST_ENTITY_ID_ATTRIBUTE_VALUE));
    mockAttributeRead(TEST_ENTITY_NAME_ATTRIBUTE, stringLiteral(TEST_ENTITY_NAME_ATTRIBUTE_VALUE));
    mockEntityUpsert();

    this.entityReader
        .getAssociatedEntityForSpan(TEST_ENTITY_TYPE_NAME, TEST_TRACE, TEST_SPAN)
        .blockingSubscribe();

    verify(mockDataClient, times(1))
        .createOrUpdateEntityEventually(any(), eq(UpsertCondition.getDefaultInstance()), any());
  }

  @Test
  void includesUpsertConditionIfTimestampAttributeDefined() {
    mockSingleEntityType(
        TEST_ENTITY_TYPE.toBuilder()
            .setTimestampAttributeKey(TEST_ENTITY_TIMESTAMP_ATTRIBUTE_KEY)
            .build());
    mockGetAllAttributes(
        TEST_ENTITY_ID_ATTRIBUTE, TEST_ENTITY_NAME_ATTRIBUTE, TEST_ENTITY_TIMESTAMP_ATTRIBUTE);
    mockGetSingleAttribute(TEST_ENTITY_TIMESTAMP_ATTRIBUTE);
    mockTenantId();
    mockAttributeRead(TEST_ENTITY_ID_ATTRIBUTE, stringLiteral(TEST_ENTITY_ID_ATTRIBUTE_VALUE));
    mockAttributeRead(TEST_ENTITY_NAME_ATTRIBUTE, stringLiteral(TEST_ENTITY_NAME_ATTRIBUTE_VALUE));
    mockAttributeRead(TEST_ENTITY_TIMESTAMP_ATTRIBUTE, longLiteral(30));
    mockEntityUpsert();

    this.entityReader
        .getAssociatedEntityForSpan(TEST_ENTITY_TYPE_NAME, TEST_TRACE, TEST_SPAN)
        .blockingSubscribe();

    UpsertCondition expectedCondition =
        UpsertCondition.newBuilder()
            .setPropertyPredicate(
                Predicate.newBuilder()
                    .setAttributeKey(TEST_ENTITY_TIMESTAMP_ATTRIBUTE_KEY)
                    .setOperator(PredicateOperator.PREDICATE_OPERATOR_LESS_THAN)
                    .setValue(longAttributeValue(30))
                    .build())
            .build();
    verify(mockDataClient, times(1))
        .createOrUpdateEntityEventually(any(), eq(expectedCondition), any());
  }

  @Test
  void enforceFormationConditions() {
    AttributeMetadata otherAttribute =
        TEST_ENTITY_NAME_ATTRIBUTE.toBuilder().setKey("other").build();
    mockSingleEntityType(
        TEST_ENTITY_TYPE.toBuilder()
            .addRequiredConditions(
                EntityFormationCondition.newBuilder()
                    .setRequiredKey(TEST_ENTITY_NAME_ATTRIBUTE_KEY))
            .addRequiredConditions(EntityFormationCondition.newBuilder().setRequiredKey("other"))
            .build());
    mockGetAllAttributes(TEST_ENTITY_ID_ATTRIBUTE, TEST_ENTITY_NAME_ATTRIBUTE, otherAttribute);
    mockTenantId();
    mockEntityUpsert();

    mockAttributeRead(TEST_ENTITY_ID_ATTRIBUTE, stringLiteral(TEST_ENTITY_ID_ATTRIBUTE_VALUE));
    mockAttributeRead(TEST_ENTITY_NAME_ATTRIBUTE, stringLiteral(TEST_ENTITY_NAME_ATTRIBUTE_VALUE));
    mockAttributeReadError(otherAttribute);
    // No "other" attribute, should not form entity

    this.entityReader
        .getAssociatedEntityForSpan(TEST_ENTITY_TYPE_NAME, TEST_TRACE, TEST_SPAN)
        .blockingSubscribe();

    verifyNoInteractions(mockDataClient);

    // Now add "other"
    mockAttributeRead(otherAttribute, stringLiteral("other-value"));

    this.entityReader
        .getAssociatedEntityForSpan(TEST_ENTITY_TYPE_NAME, TEST_TRACE, TEST_SPAN)
        .blockingSubscribe();

    verify(mockDataClient, times(1)).createOrUpdateEntityEventually(any(), any(), any());
  }

  @Test
  void skipsUnsetFormationConditions() {
    mockSingleEntityType(
        TEST_ENTITY_TYPE.toBuilder()
            .addRequiredConditions(EntityFormationCondition.newBuilder())
            .build());
    mockGetAllAttributes(TEST_ENTITY_ID_ATTRIBUTE, TEST_ENTITY_NAME_ATTRIBUTE);
    mockTenantId();
    mockEntityUpsert();

    mockAttributeRead(TEST_ENTITY_ID_ATTRIBUTE, stringLiteral(TEST_ENTITY_ID_ATTRIBUTE_VALUE));
    mockAttributeRead(TEST_ENTITY_NAME_ATTRIBUTE, stringLiteral(TEST_ENTITY_NAME_ATTRIBUTE_VALUE));

    this.entityReader
        .getAssociatedEntityForSpan(TEST_ENTITY_TYPE_NAME, TEST_TRACE, TEST_SPAN)
        .blockingSubscribe();

    verify(mockDataClient, times(1)).createOrUpdateEntityEventually(any(), any(), any());
  }

  private void mockTenantId() {
    when(this.mockAttributeReader.getTenantId(TEST_SPAN)).thenReturn(TENANT_ID);
  }

  private void mockAttributeRead(AttributeMetadata attributeMetadata, LiteralValue value) {
    when(this.mockAttributeReader.getSpanValue(
            TEST_TRACE, TEST_SPAN, attributeMetadata.getScopeString(), attributeMetadata.getKey()))
        .thenReturn(Single.just(value));
  }

  private void mockAttributeReadError(AttributeMetadata attributeMetadata) {
    when(this.mockAttributeReader.getSpanValue(
            TEST_TRACE, TEST_SPAN, attributeMetadata.getScopeString(), attributeMetadata.getKey()))
        .thenReturn(Single.error(new NoSuchElementException()));
  }

  private void mockEntityUpsert() {
    when(this.mockDataClient.createOrUpdateEntityEventually(
            any(Entity.class), any(UpsertCondition.class), any(Duration.class)))
        .thenAnswer(invocation -> Single.just(invocation.getArgument(0)));
  }

  private void mockGetAllAttributes(AttributeMetadata... attributeMetadata) {
    when(this.mockAttributeClient.getAllInScope(TEST_ENTITY_TYPE_NAME))
        .thenReturn(Single.just(Arrays.asList(attributeMetadata)));
  }

  private void mockGetSingleAttribute(AttributeMetadata attributeMetadata) {
    when(this.mockAttributeClient.get(
            attributeMetadata.getScopeString(), attributeMetadata.getKey()))
        .thenReturn(Single.just(attributeMetadata));
  }

  private void mockSingleEntityType() {
    mockSingleEntityType(TEST_ENTITY_TYPE);
  }

  private void mockSingleEntityType(EntityType entityType) {
    when(this.mockTypeClient.get(entityType.getName())).thenReturn(Single.just(entityType));
  }

  private void mockAllEntityTypes() {
    when(this.mockTypeClient.getAll()).thenReturn(Observable.just(TEST_ENTITY_TYPE));
  }
}
