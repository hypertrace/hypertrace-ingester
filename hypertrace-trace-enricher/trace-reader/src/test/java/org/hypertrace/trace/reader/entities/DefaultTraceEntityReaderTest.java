package org.hypertrace.trace.reader.entities;

import static org.hypertrace.trace.reader.attributes.AvroUtil.defaultedEventBuilder;
import static org.hypertrace.trace.reader.attributes.AvroUtil.defaultedStructuredTraceBuilder;
import static org.hypertrace.trace.reader.attributes.EntityUtil.buildAttributesWithKeyValues;
import static org.hypertrace.trace.reader.attributes.LiteralValueUtil.stringLiteral;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Single;
import java.util.List;
import java.util.Map;
import org.hypertrace.core.attribute.service.cachingclient.CachingAttributeClient;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeType;
import org.hypertrace.core.attribute.service.v1.LiteralValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.entity.data.service.rxclient.EntityDataClient;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.type.service.rxclient.EntityTypeClient;
import org.hypertrace.entity.type.service.v2.EntityType;
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
  private static final AttributeMetadata TEST_ENTITY_ID_ATTRIBUTE =
      AttributeMetadata.newBuilder()
          .setScopeString(TEST_ENTITY_TYPE_NAME)
          .setKey(TEST_ENTITY_ID_ATTRIBUTE_KEY)
          .setType(AttributeType.ATTRIBUTE)
          .build();
  private static final AttributeMetadata TEST_ENTITY_NAME_ATTRIBUTE =
      AttributeMetadata.newBuilder()
          .setScopeString(TEST_ENTITY_TYPE_NAME)
          .setKey(TEST_ENTITY_NAME_ATTRIBUTE_KEY)
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
            this.mockAttributeReader);
  }

  @Test
  void canReadAnEntity() {
    mockSingleEntityType();
    mockAvailableAttributes();
    mockCustomerId();
    mockEntityIdWith(stringLiteral(TEST_ENTITY_ID_ATTRIBUTE_VALUE));
    mockEntityNameWith(stringLiteral(TEST_ENTITY_NAME_ATTRIBUTE_VALUE));
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
    mockAvailableAttributes();
    mockCustomerId();
    mockEntityIdWith(stringLiteral(TEST_ENTITY_ID_ATTRIBUTE_VALUE));
    mockEntityNameWith(stringLiteral(TEST_ENTITY_NAME_ATTRIBUTE_VALUE));
    mockEntityUpsert();

    assertEquals(
        Map.of(TEST_ENTITY_TYPE_NAME, EXPECTED_ENTITY),
        this.entityReader.getAssociatedEntitiesForSpan(TEST_TRACE, TEST_SPAN).blockingGet());
  }

  @Test
  void omitsEntityBasedOnMissingAttributes() {
    mockSingleEntityType();
    mockAvailableAttributes();
    mockCustomerId();
    mockEntityIdWith(LiteralValue.getDefaultInstance());
    mockEntityNameWith(stringLiteral(TEST_ENTITY_NAME_ATTRIBUTE_VALUE));

    assertTrue(
        this.entityReader
            .getAssociatedEntityForSpan(TEST_ENTITY_TYPE_NAME, TEST_TRACE, TEST_SPAN)
            .isEmpty()
            .blockingGet());
  }

  private void mockCustomerId() {
    when(this.mockAttributeReader.getTenantId(TEST_SPAN)).thenReturn(TENANT_ID);
  }

  private void mockEntityNameWith(LiteralValue value) {
    when(this.mockAttributeReader.getSpanValue(
            TEST_TRACE, TEST_SPAN, TEST_ENTITY_TYPE_NAME, TEST_ENTITY_NAME_ATTRIBUTE_KEY))
        .thenReturn(Single.just(value));
  }

  private void mockEntityIdWith(LiteralValue value) {
    when(this.mockAttributeReader.getSpanValue(
            TEST_TRACE, TEST_SPAN, TEST_ENTITY_TYPE_NAME, TEST_ENTITY_ID_ATTRIBUTE_KEY))
        .thenReturn(Single.just(value));
  }

  private void mockEntityUpsert() {
    when(this.mockDataClient.getOrCreateEntity(any(Entity.class)))
        .thenAnswer(invocation -> Single.just(invocation.getArgument(0)));
  }

  private void mockAvailableAttributes() {
    when(this.mockAttributeClient.getAllInScope(TEST_ENTITY_TYPE_NAME))
        .thenReturn(Single.just(List.of(TEST_ENTITY_ID_ATTRIBUTE, TEST_ENTITY_NAME_ATTRIBUTE)));
  }

  private void mockSingleEntityType() {
    when(this.mockTypeClient.get(TEST_ENTITY_TYPE_NAME)).thenReturn(Single.just(TEST_ENTITY_TYPE));
  }

  private void mockAllEntityTypes() {
    when(this.mockTypeClient.getAll()).thenReturn(Observable.just(TEST_ENTITY_TYPE));
  }
}
