package org.hypertrace.traceenricher.enrichment.enrichers;

import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.StructuredTraceGraph;
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.utils.EnrichedSpanUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.BoundaryTypeValue;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.CommonAttribute;
import org.hypertrace.traceenricher.util.Constants;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class ApiBoundaryTypeAttributeEnricherTest extends AbstractAttributeEnricherTest {
  private static final BoundaryTypeValue ENTRY = BoundaryTypeValue.BOUNDARY_TYPE_VALUE_ENTRY;
  private static final BoundaryTypeValue EXIT = BoundaryTypeValue.BOUNDARY_TYPE_VALUE_EXIT;
  private static final BoundaryTypeValue UNKNOWN = BoundaryTypeValue.BOUNDARY_TYPE_VALUE_UNSPECIFIED;

  private Event outerEntrySpan;
  private Event innerEntrySpan;
  private Event innerExitSpan;
  private Event outerExitSpan;
  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private StructuredTrace trace;
  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private StructuredTraceGraph graph;
  private ApiBoundaryTypeAttributeEnricher target;

  @BeforeEach
  public void setup() {
    MockitoAnnotations.initMocks(this);
    outerEntrySpan = createMockEntryEvent();
    innerEntrySpan = createMockEntryEvent();
    innerExitSpan = createMockExitEvent();
    outerExitSpan = createMockExitEvent();
    target = Mockito.spy(new ApiBoundaryTypeAttributeEnricher());
    mockStructuredGraph();
  }

  @Test
  public void test_enrichEvent_noAttributes_noNPE() {
    StructuredTrace noAttributesTrace = mock(StructuredTrace.class, RETURNS_DEEP_STUBS);
    when(noAttributesTrace.getAttributes()).thenReturn(null);
  }

  @Test
  public void test_enrichEvent_noAttributesMap_noNPE() {
    StructuredTrace noAttributesMapTrace = mock(StructuredTrace.class, RETURNS_DEEP_STUBS);
    when(noAttributesMapTrace.getAttributes().getAttributeMap()).thenReturn(null);
  }

  @Test
  public void test_enrichEvent_unknownSpanKind_shouldReturnNull() {
    addEnrichedAttributeToEvent(outerEntrySpan, Constants.getEnrichedSpanConstant(CommonAttribute.COMMON_ATTRIBUTE_SPAN_TYPE),
        AttributeValueCreator.create(Constants.getEnrichedSpanConstant(UNKNOWN)));
    target.enrichEvent(trace, outerEntrySpan);
    Assertions.assertNull(EnrichedSpanUtils.getApiBoundaryType(outerEntrySpan));
  }

  @Test
  public void test_enrichEvent_validEntryExitSpan_MarkAsApiEntryAndExit() {
    // InnerEntrySpan -> InnerExitSpan scenario
    target.enrichEvent(trace, innerEntrySpan);
    Assertions.assertEquals(EnrichedSpanUtils.getApiBoundaryType(innerEntrySpan), Constants.getEnrichedSpanConstant(ENTRY));
    target.enrichEvent(trace, innerExitSpan);
    Assertions.assertEquals(EnrichedSpanUtils.getApiBoundaryType(innerExitSpan), Constants.getEnrichedSpanConstant(EXIT));
  }

  @Test
  public void test_enrichEvent_doubleEntryExitSpan_markTheOuterOneAsApiBoundary() {
    // OuterEntry -> inner Entry -> inner Exit -> outer Exit
    mockDoubleEntryStructuredGraph();
    target.enrichEvent(trace, outerEntrySpan);
    Assertions.assertEquals(EnrichedSpanUtils.getApiBoundaryType(outerEntrySpan), Constants.getEnrichedSpanConstant(ENTRY));
    target.enrichEvent(trace, innerEntrySpan);
    Assertions.assertNull(EnrichedSpanUtils.getApiBoundaryType(innerEntrySpan));
    target.enrichEvent(trace, innerEntrySpan);
    Assertions.assertNull(EnrichedSpanUtils.getApiBoundaryType(innerExitSpan));
    target.enrichEvent(trace, outerExitSpan);
    Assertions.assertEquals(EnrichedSpanUtils.getApiBoundaryType(outerExitSpan), Constants.getEnrichedSpanConstant(EXIT));
  }

  @Test
  public void test_enrichEvent_loopToItsOwnService_findTwoAPIEntries() {
    // Entry to API 1 -> Exit From API 1 & Re-entry -> Entry to API 1 -> Exit to API 2
    Event firstEntry = createMockEntryEvent();
    Event firstExit = createMockExitEvent();
    Event secondEntry = createMockEntryEvent();
    Event secondExit = createMockExitEvent();

    when(graph.getParentEvent(firstEntry)).thenReturn(null);
    when(graph.getParentEvent(secondEntry)).thenReturn(firstExit);

    when(graph.getChildrenEvents(firstExit)).thenReturn(Lists.newArrayList(secondEntry));
    when(graph.getChildrenEvents(secondExit)).thenReturn(null);

    when(target.buildGraph(trace)).thenReturn(graph);

    target.enrichEvent(trace, firstEntry);
    Assertions.assertEquals(EnrichedSpanUtils.getApiBoundaryType(firstEntry), Constants.getEnrichedSpanConstant(ENTRY));
    target.enrichEvent(trace, firstExit);
    Assertions.assertEquals(EnrichedSpanUtils.getApiBoundaryType(firstExit), Constants.getEnrichedSpanConstant(EXIT));
    target.enrichEvent(trace, secondEntry);
    Assertions.assertEquals(EnrichedSpanUtils.getApiBoundaryType(secondEntry), Constants.getEnrichedSpanConstant(ENTRY));
    target.enrichEvent(trace, secondExit);
    Assertions.assertEquals(EnrichedSpanUtils.getApiBoundaryType(secondExit), Constants.getEnrichedSpanConstant(EXIT));
  }

  @Test
  public void testEnrichEventWithRequestHostHeader() {
    addEnrichedAttributeToEvent(innerEntrySpan,
        RawSpanConstants.getValue(org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_HOST_HEADER),
        AttributeValueCreator.create("testHost"));
    target.enrichEvent(trace, innerEntrySpan);
    Assertions.assertEquals(EnrichedSpanUtils.getHostHeader(innerEntrySpan), "testHost");
  }

  @Test
  public void testEnrichEventWithAuthorityHeader() {
    addEnrichedAttributeToEvent(innerEntrySpan,
        RawSpanConstants.getValue(org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_AUTHORITY_HEADER),
        AttributeValueCreator.create("testHost"));
    target.enrichEvent(trace, innerEntrySpan);
    Assertions.assertEquals(EnrichedSpanUtils.getHostHeader(innerEntrySpan), "testHost");
  }

  @Test
  public void testEnrichEventWithHostHeader() {
    addEnrichedAttributeToEvent(innerEntrySpan,
        RawSpanConstants.getValue(org.hypertrace.core.span.constants.v1.Http.HTTP_HOST),
        AttributeValueCreator.create("testHost"));
    target.enrichEvent(trace, innerEntrySpan);
    Assertions.assertEquals(EnrichedSpanUtils.getHostHeader(innerEntrySpan), "testHost");
  }

  @Test
  public void testEnrichEventWithForwardedHost() {
    addEnrichedAttributeToEvent(innerEntrySpan, "x-forwarded-host",
        AttributeValueCreator.create("testHost"));
    target.enrichEvent(trace, innerEntrySpan);
    Assertions.assertEquals(EnrichedSpanUtils.getHostHeader(innerEntrySpan), "testHost");

    // Add port also to the host value and test again.
    addEnrichedAttributeToEvent(innerEntrySpan, "x-forwarded-host",
        AttributeValueCreator.create("testHost:443"));
    target.enrichEvent(trace, innerEntrySpan);
    Assertions.assertEquals(EnrichedSpanUtils.getHostHeader(innerEntrySpan), "testHost");
  }

  @Test
  public void testEnrichEventWithLocalHostAndForwardedHost() {
    addEnrichedAttributeToEvent(innerEntrySpan,
        RawSpanConstants.getValue(org.hypertrace.core.span.constants.v1.Http.HTTP_HOST),
        AttributeValueCreator.create("localhost:443"));
    addEnrichedAttributeToEvent(innerEntrySpan,
        RawSpanConstants.getValue(org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_AUTHORITY_HEADER),
        AttributeValueCreator.create("localhost"));
    addEnrichedAttributeToEvent(innerEntrySpan, "x-forwarded-host",
        AttributeValueCreator.create("testHost"));
    target.enrichEvent(trace, innerEntrySpan);
    Assertions.assertEquals(EnrichedSpanUtils.getHostHeader(innerEntrySpan), "testHost");
  }

  private void mockStructuredGraph() {
    when(graph.getParentEvent(innerExitSpan)).thenReturn(innerEntrySpan);
    when(graph.getChildrenEvents(innerExitSpan)).thenReturn(null);

    when(graph.getParentEvent(innerEntrySpan)).thenReturn(null);

    doReturn(graph).when(target).buildGraph(trace);
  }

  private void mockDoubleEntryStructuredGraph() {
    when(graph.getParentEvent(innerEntrySpan)).thenReturn(outerEntrySpan);
    when(graph.getParentEvent(outerEntrySpan)).thenReturn(null);

    when(graph.getChildrenEvents(innerExitSpan)).thenReturn(Lists.newArrayList(outerExitSpan));
    when(graph.getChildrenEvents(outerExitSpan)).thenReturn(null);

    doReturn(graph).when(target).buildGraph(trace);
  }
}
