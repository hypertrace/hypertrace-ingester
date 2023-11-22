package org.hypertrace.trace.accessor.entities;

import static org.hypertrace.trace.accessor.entities.AttributeValueUtil.longAttributeValue;
import static org.hypertrace.trace.reader.attributes.AvroUtil.defaultedEventBuilder;
import static org.hypertrace.trace.reader.attributes.AvroUtil.defaultedStructuredTraceBuilder;
import static org.hypertrace.trace.reader.attributes.EntityUtil.buildAttributesWithKeyValues;
import static org.hypertrace.trace.reader.attributes.LiteralValueUtil.longLiteral;
import static org.hypertrace.trace.reader.attributes.LiteralValueUtil.stringLiteral;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.schedulers.Schedulers;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.hypertrace.core.attribute.service.client.AttributeServiceCachedClient;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeSource;
import org.hypertrace.core.attribute.service.v1.AttributeType;
import org.hypertrace.core.attribute.service.v1.LiteralValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.data.service.rxclient.EntityDataClient;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.data.service.v1.MergeAndUpsertEntityRequest.UpsertCondition;
import org.hypertrace.entity.data.service.v1.MergeAndUpsertEntityRequest.UpsertCondition.Predicate;
import org.hypertrace.entity.data.service.v1.MergeAndUpsertEntityRequest.UpsertCondition.Predicate.PredicateOperator;
import org.hypertrace.entity.type.service.rxclient.EntityTypeClient;
import org.hypertrace.entity.type.service.v2.EntityType;
import org.hypertrace.entity.type.service.v2.EntityType.EntityFormationCondition;
import org.hypertrace.trace.reader.attributes.TraceAttributeReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DefaultTraceEntityAccessorTest {
  private static final String TENANT_ID = "tenant-id";
  private static final String TEST_ENTITY_TYPE_NAME = "ENTITY_TYPE_1";
  private static final String TEST_ENTITY_ID_ATTRIBUTE_KEY = "id";
  private static final String TEST_ENTITY_ID_ATTRIBUTE_VALUE = "id-value";
  private static final String TEST_ENTITY_NAME_ATTRIBUTE_KEY = "name";
  private static final String TEST_ENTITY_NAME_ATTRIBUTE_VALUE = "name-value";
  private static final String TEST_ENTITY_TIMESTAMP_ATTRIBUTE_KEY = "timestamp";
  private static final String TEST_EXCLUDE_ENTITY_TYPE_NAME = "EXCLUDE_ENTITY_TYPE";
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

  private static final EntityType TEST_EXCLUDE_ENTITY_TYPE =
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
  private static final ArgumentMatcher<RequestContext> MATCHING_TENANT_REQUEST_CONTEXT =
      arg ->
          arg.buildContextualKey()
              .equals(RequestContext.forTenantId(TENANT_ID).buildContextualKey());
  private static final Duration DEFAULT_DURATION = Duration.ofSeconds(15);
  private static final Set<String> EXCLUDE_ENTITY_TYPES = Set.of(TEST_EXCLUDE_ENTITY_TYPE_NAME);

  @Mock EntityTypeClient mockTypeClient;
  @Mock EntityDataClient mockDataClient;
  @Mock AttributeServiceCachedClient mockAttributeClient;
  @Mock TraceAttributeReader<StructuredTrace, Event> mockAttributeReader;
  MockedStatic<Schedulers> mockSchedulers;

  private DefaultTraceEntityAccessor entityAccessor;

  @BeforeEach
  void beforeEach() {
    Scheduler trampoline = Schedulers.trampoline();
    this.entityAccessor =
        new DefaultTraceEntityAccessor(
            this.mockTypeClient,
            this.mockDataClient,
            this.mockAttributeClient,
            this.mockAttributeReader,
            DEFAULT_DURATION,
            EXCLUDE_ENTITY_TYPES);
    when(mockAttributeReader.getRequestContext(any()))
        .thenAnswer(
            inv -> {
              Event event = inv.getArgument(0);
              return RequestContext.forTenantId(event.getCustomerId());
            });
    mockSchedulers = Mockito.mockStatic(Schedulers.class);
    mockSchedulers.when(Schedulers::io).thenReturn(trampoline);
  }

  @AfterEach
  void afterEach() {
    mockSchedulers.close();
  }

  @Test
  void canWriteAllEntities() {
    mockAllEntityTypes();
    mockGetAllAttributes(TEST_ENTITY_ID_ATTRIBUTE, TEST_ENTITY_NAME_ATTRIBUTE);
    mockTenantId();
    mockAttributeRead(TEST_ENTITY_ID_ATTRIBUTE, stringLiteral(TEST_ENTITY_ID_ATTRIBUTE_VALUE));
    mockAttributeRead(TEST_ENTITY_NAME_ATTRIBUTE, stringLiteral(TEST_ENTITY_NAME_ATTRIBUTE_VALUE));
    this.entityAccessor.writeAssociatedEntitiesForSpanEventually(TEST_TRACE, TEST_SPAN);

    verify(mockDataClient, times(1))
        .createOrUpdateEntityEventually(
            argThat(MATCHING_TENANT_REQUEST_CONTEXT),
            eq(EXPECTED_ENTITY),
            eq(UpsertCondition.getDefaultInstance()),
            eq(DEFAULT_DURATION));
  }

  @Test
  void omitsEntityBasedOnMissingAttributes() {
    mockAllEntityTypes();
    mockGetAllAttributes(TEST_ENTITY_ID_ATTRIBUTE, TEST_ENTITY_NAME_ATTRIBUTE);
    mockTenantId();
    mockAttributeRead(TEST_ENTITY_ID_ATTRIBUTE, LiteralValue.getDefaultInstance());
    mockAttributeRead(TEST_ENTITY_NAME_ATTRIBUTE, stringLiteral(TEST_ENTITY_NAME_ATTRIBUTE_VALUE));

    this.entityAccessor.writeAssociatedEntitiesForSpanEventually(TEST_TRACE, TEST_SPAN);
    verifyNoInteractions(mockDataClient);
  }

  @Test
  void omitsEntityBasedOnEmptyId() {
    mockAllEntityTypes();
    mockGetAllAttributes(TEST_ENTITY_ID_ATTRIBUTE, TEST_ENTITY_NAME_ATTRIBUTE);
    mockTenantId();
    mockAttributeRead(TEST_ENTITY_ID_ATTRIBUTE, stringLiteral(""));
    mockAttributeRead(TEST_ENTITY_NAME_ATTRIBUTE, stringLiteral(TEST_ENTITY_NAME_ATTRIBUTE_VALUE));

    this.entityAccessor.writeAssociatedEntitiesForSpanEventually(TEST_TRACE, TEST_SPAN);
    verifyNoInteractions(mockDataClient);
  }

  @Test
  void omitsEntityBasedOnEntityType() {
    mockAllEntityTypes(TEST_EXCLUDE_ENTITY_TYPE);
    mockGetAllAttributes(TEST_ENTITY_ID_ATTRIBUTE, TEST_ENTITY_NAME_ATTRIBUTE);
    mockTenantId();
    mockAttributeRead(TEST_ENTITY_ID_ATTRIBUTE, stringLiteral(""));
    mockAttributeRead(TEST_ENTITY_NAME_ATTRIBUTE, stringLiteral(TEST_EXCLUDE_ENTITY_TYPE_NAME));

    this.entityAccessor.writeAssociatedEntitiesForSpanEventually(TEST_TRACE, TEST_SPAN);
    verifyNoInteractions(mockDataClient);
  }

  @Test
  void includesUpsertConditionIfTimestampAttributeDefined() {
    mockAllEntityTypes(
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

    this.entityAccessor.writeAssociatedEntitiesForSpanEventually(TEST_TRACE, TEST_SPAN);

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
        .createOrUpdateEntityEventually(
            argThat(MATCHING_TENANT_REQUEST_CONTEXT),
            eq(
                EXPECTED_ENTITY.toBuilder()
                    .putAttributes(TEST_ENTITY_TIMESTAMP_ATTRIBUTE_KEY, longAttributeValue(30))
                    .build()),
            eq(expectedCondition),
            eq(DEFAULT_DURATION));
  }

  @Test
  void enforceFormationConditions() {
    AttributeMetadata otherAttribute =
        TEST_ENTITY_NAME_ATTRIBUTE.toBuilder().setKey("other").build();
    mockAllEntityTypes(
        TEST_ENTITY_TYPE.toBuilder()
            .addRequiredConditions(
                EntityFormationCondition.newBuilder()
                    .setRequiredKey(TEST_ENTITY_NAME_ATTRIBUTE_KEY))
            .addRequiredConditions(EntityFormationCondition.newBuilder().setRequiredKey("other"))
            .build());
    mockGetAllAttributes(TEST_ENTITY_ID_ATTRIBUTE, TEST_ENTITY_NAME_ATTRIBUTE, otherAttribute);
    mockTenantId();

    mockAttributeRead(TEST_ENTITY_ID_ATTRIBUTE, stringLiteral(TEST_ENTITY_ID_ATTRIBUTE_VALUE));
    mockAttributeRead(TEST_ENTITY_NAME_ATTRIBUTE, stringLiteral(TEST_ENTITY_NAME_ATTRIBUTE_VALUE));
    mockAttributeReadError(otherAttribute);
    // No "other" attribute, should not form entity

    this.entityAccessor.writeAssociatedEntitiesForSpanEventually(TEST_TRACE, TEST_SPAN);
    verifyNoInteractions(mockDataClient);

    // Now add "other"
    mockAttributeRead(otherAttribute, stringLiteral("other-value"));

    this.entityAccessor.writeAssociatedEntitiesForSpanEventually(TEST_TRACE, TEST_SPAN);

    verify(mockDataClient, times(1)).createOrUpdateEntityEventually(any(), any(), any(), any());
  }

  @Test
  void skipsUnsetFormationConditions() {
    mockAllEntityTypes(
        TEST_ENTITY_TYPE.toBuilder()
            .addRequiredConditions(EntityFormationCondition.newBuilder())
            .build());
    mockGetAllAttributes(TEST_ENTITY_ID_ATTRIBUTE, TEST_ENTITY_NAME_ATTRIBUTE);
    mockTenantId();

    mockAttributeRead(TEST_ENTITY_ID_ATTRIBUTE, stringLiteral(TEST_ENTITY_ID_ATTRIBUTE_VALUE));
    mockAttributeRead(TEST_ENTITY_NAME_ATTRIBUTE, stringLiteral(TEST_ENTITY_NAME_ATTRIBUTE_VALUE));

    this.entityAccessor.writeAssociatedEntitiesForSpanEventually(TEST_TRACE, TEST_SPAN);

    verify(mockDataClient, times(1)).createOrUpdateEntityEventually(any(), any(), any(), any());
  }

  private void mockTenantId() {
    when(this.mockAttributeReader.getTenantId(TEST_SPAN)).thenReturn(TENANT_ID);
  }

  private void mockAttributeRead(AttributeMetadata attributeMetadata, LiteralValue value) {
    when(this.mockAttributeReader.getSpanValue(
            TEST_TRACE, TEST_SPAN, attributeMetadata.getScopeString(), attributeMetadata.getKey()))
        .thenReturn(Optional.of(value));
  }

  private void mockAttributeReadError(AttributeMetadata attributeMetadata) {
    when(this.mockAttributeReader.getSpanValue(
            TEST_TRACE, TEST_SPAN, attributeMetadata.getScopeString(), attributeMetadata.getKey()))
        .thenReturn(Optional.empty());
  }

  private void mockGetAllAttributes(AttributeMetadata... attributeMetadata) {
    when(this.mockAttributeClient.getAllInScope(
            argThat(MATCHING_TENANT_REQUEST_CONTEXT), eq(TEST_ENTITY_TYPE_NAME)))
        .thenReturn(Arrays.asList(attributeMetadata));
  }

  private void mockGetSingleAttribute(AttributeMetadata attributeMetadata) {
    when(this.mockAttributeClient.get(
            argThat(MATCHING_TENANT_REQUEST_CONTEXT),
            eq(attributeMetadata.getScopeString()),
            eq(attributeMetadata.getKey())))
        .thenReturn(Optional.of(attributeMetadata));
  }

  private void mockAllEntityTypes(EntityType entityType) {
    when(this.mockTypeClient.getAll()).thenReturn(Observable.just(entityType));
  }

  private void mockAllEntityTypes() {
    mockAllEntityTypes(TEST_ENTITY_TYPE);
  }
}
