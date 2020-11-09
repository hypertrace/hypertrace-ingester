package org.hypertrace.traceenricher.enrichment.enrichers;

import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
import org.hypertrace.entity.constants.v1.ApiAttribute;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.CommonAttribute;
import org.hypertrace.traceenricher.util.Constants;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockitoAnnotations;

public class TransactionNameEnricherTest extends AbstractAttributeEnricherTest {
  private static final Long ROOT_START_TIME = 55555111111L;
  private static final Long CHILD_START_TIME = 55555555555L;
  private static final String API_NAME_VAL = "/login";
  private static final CommonAttribute TRANSACTION_NAME = CommonAttribute.COMMON_ATTRIBUTE_TRANSACTION_NAME;

  private List<Event> eventList;
  private TransactionNameEnricher target;
  private StructuredTrace structuredTrace;
  private Event rootEvent;
  private Event childEvent;
  private Attributes traceAttributes;
  private Map<String, AttributeValue> rootEventAttributesMap;
  private Map<String, AttributeValue> childEventAttributesMap;

  @BeforeEach
  public void setup() {
    MockitoAnnotations.initMocks(this);
    rootEvent = createMockEntryEvent();
    rootEventAttributesMap = rootEvent.getAttributes().getAttributeMap();
    childEvent = mock(Event.class, RETURNS_DEEP_STUBS);
    childEventAttributesMap = new HashMap<>();
    when(rootEvent.getStartTimeMillis()).thenReturn(ROOT_START_TIME);
    when(childEvent.getStartTimeMillis()).thenReturn(CHILD_START_TIME);

    structuredTrace = mock(StructuredTrace.class);
    traceAttributes = createNewAvroAttributes();
    eventList = Lists.newArrayList(rootEvent, childEvent);
    when(structuredTrace.getAttributes()).thenReturn(traceAttributes);
    when(structuredTrace.getEventList()).thenReturn(eventList);
    when(childEvent.getAttributes().getAttributeMap()).thenReturn(childEventAttributesMap);
    target = new TransactionNameEnricher();
  }

  @Test
  public void test_enrichTrace_nullAttributes_noNPE() {
    StructuredTrace nullAttributesTrace = mock(StructuredTrace.class);
    target.enrichTrace(nullAttributesTrace);
  }

  @Test
  public void test_enrichTrace_nullAttributesMap_noNPE() {
    StructuredTrace nullAttributesMapTrace = mock(StructuredTrace.class, RETURNS_DEEP_STUBS);
    when(nullAttributesMapTrace.getAttributes().getAttributeMap()).thenReturn(null);
    target.enrichTrace(nullAttributesMapTrace);
  }

  @Test
  public void test_enrichTrace_noApiNameAttribute_returnUnknownTransactionName() {
    target.enrichTrace(structuredTrace);
    Assertions.assertNull(structuredTrace.getAttributes().getAttributeMap()
        .get(Constants.getEnrichedSpanConstant(TRANSACTION_NAME)));
  }

  @Test
  public void test_enrichTrace_validApiNameAttribute_returnValidTransactionName() {
    rootEventAttributesMap.put(Constants.getEntityConstant(ApiAttribute.API_ATTRIBUTE_NAME),
        AttributeValueCreator.create(API_NAME_VAL));
    target.enrichTrace(structuredTrace);
    Assertions.assertEquals(getApiNameVal(structuredTrace), API_NAME_VAL);
  }

  @Test
  public void test_enrichTrace_noEvent_returnUnknownTransactionName() {
    when(structuredTrace.getEventList()).thenReturn(Collections.emptyList());
    target.enrichTrace(structuredTrace);
    Assertions.assertNull(structuredTrace.getAttributes().getAttributeMap()
        .get(Constants.getEnrichedSpanConstant(TRANSACTION_NAME)));
  }

  private String getApiNameVal(StructuredTrace structuredTrace) {
    return structuredTrace.getAttributes().getAttributeMap().get(
        Constants.getEnrichedSpanConstant(TRANSACTION_NAME)).getValue();
  }
}
