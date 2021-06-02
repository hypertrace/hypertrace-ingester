package org.hypertrace.traceenricher.enrichment.enrichers;

import static org.hypertrace.core.span.constants.v1.CensusResponse.CENSUS_RESPONSE_STATUS_MESSAGE;
import static org.hypertrace.core.span.constants.v1.Grpc.GRPC_ERROR_MESSAGE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.eventfields.grpc.Response;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.span.constants.v1.Grpc;
import org.hypertrace.core.span.constants.v1.Http;
import org.hypertrace.core.span.constants.v1.OTSpanTag;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Api;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.ApiStatus;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.CommonAttribute;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Protocol;
import org.hypertrace.traceenricher.util.Constants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ApiStatusEnricherTest extends AbstractAttributeEnricherTest {

  private ApiStatusEnricher target;

  @BeforeEach
  public void setup() {
    target = new ApiStatusEnricher();
  }

  @Test
  public void test_enrich_httpSuccess_shouldEnrich() {
    String expectedStatusCode = "200";
    Event event = createMockEvent();
    mockProtocol(event, Protocol.PROTOCOL_HTTP);
    addAttribute(event,Constants.getRawSpanConstant(OTSpanTag.OT_SPAN_TAG_HTTP_STATUS_CODE),expectedStatusCode);

    target.enrichEvent(null, event);

    assertEquals(expectedStatusCode, getStatusCode(event));
    assertEquals("OK", getStatusMessage(event));
    assertEquals(Constants.getEnrichedSpanConstant(ApiStatus.API_STATUS_SUCCESS), getStatus(event));
  }

  @Test
  public void test_statusEnricher_grpcSuccess_shouldEnrichAll() {
    String expectedStatusCode = "0";
    Event event = createMockEvent();
    mockProtocol(event, Protocol.PROTOCOL_GRPC);
    addAttribute(event,Constants.getRawSpanConstant(Grpc.GRPC_STATUS_CODE),expectedStatusCode);
    target.enrichEvent(null, event);

    assertEquals(expectedStatusCode, getStatusCode(event));
    assertEquals("OK", getStatusMessage(event));
    assertEquals(Constants.getEnrichedSpanConstant(ApiStatus.API_STATUS_SUCCESS), getStatus(event));
  }

  @Test
  public void test_enrich_statusCode_http() {
    Event e = createMockEvent();
    mockProtocol(e, Protocol.PROTOCOL_HTTP);
    // First try with http response size attribute.
    addAttribute(e,Constants.getRawSpanConstant(Http.HTTP_RESPONSE_STATUS_CODE),"200");
    target.enrichEvent(null, e);
    assertEquals("200", getStatusCode(e));
  }

  @Test
  public void test_enrich_statusCode_grpc() {
    // Try the GRPC response length parsing.
    Event e = createMockEvent();
    mockProtocol(e, Protocol.PROTOCOL_GRPC);
    addAttribute(e,Constants.getRawSpanConstant(Grpc.GRPC_STATUS_CODE),"5");
    target.enrichEvent(null, e);
    assertEquals("5", getStatusCode(e));
  }

  @Test
  public void test_enrich_statusCode_grpc_fields_default() {
    // Try the GRPC response length parsing.
    Event e = createMockEvent();
    addAttribute(e,RawSpanConstants.getValue(Grpc.GRPC_STATUS_CODE),"5");

    Response response = mock(Response.class);
    when(response.getStatusCode()).thenReturn(5);
    mockProtocol(e, Protocol.PROTOCOL_GRPC);
    target.enrichEvent(null, e);
    assertEquals("5", getStatusCode(e));
    assertEquals("NOT_FOUND", getStatusMessage(e));
    assertEquals("FAIL", getStatus(e));
  }

  @Test
  public void test_enrich_statusCode_grpc_fields_success() {
    // Try the GRPC response length parsing.
    Event e = createMockEvent();
    addAttribute(e,RawSpanConstants.getValue(Grpc.GRPC_STATUS_CODE),"0");
    addAttribute(e,RawSpanConstants.getValue(CENSUS_RESPONSE_STATUS_MESSAGE),"Call was successful");

    Response response = mock(Response.class);
    when(response.getStatusCode()).thenReturn(0);
    when(response.getStatusMessage()).thenReturn("Call was successful");
    mockProtocol(e, Protocol.PROTOCOL_GRPC);
    target.enrichEvent(null, e);
    assertEquals("0", getStatusCode(e));
    assertEquals("Call was successful", getStatusMessage(e));
    assertEquals("SUCCESS", getStatus(e));
  }

  @Test
  public void test_enrich_statusCode_grpc_fields_failure() {
    // Try the GRPC response length parsing.
    Event e = createMockEvent();
    addAttribute(e,RawSpanConstants.getValue(Grpc.GRPC_STATUS_CODE),"5");
    addAttribute(e,RawSpanConstants.getValue(CENSUS_RESPONSE_STATUS_MESSAGE),"Call was a failure");

    Response response = mock(Response.class);
    when(response.getStatusCode()).thenReturn(5);
    when(response.getErrorMessage()).thenReturn("Call was a failure");
    mockProtocol(e, Protocol.PROTOCOL_GRPC);
    target.enrichEvent(null, e);
    assertEquals("5", getStatusCode(e));
    assertEquals("Call was a failure", getStatusMessage(e));
    assertEquals("FAIL", getStatus(e));
  }

  @Test
  public void test_enrich_statusCode_grpc_fields_unset() {
    // Try the GRPC response length parsing.
    Event e = createMockEvent();
    Response response = mock(Response.class);
    when(response.getStatusCode()).thenReturn(-1);
    mockProtocol(e, Protocol.PROTOCOL_GRPC);
    target.enrichEvent(null, e);
    assertEquals(
        null,
        e.getEnrichedAttributes()
            .getAttributeMap()
            .get(Constants.getEnrichedSpanConstant(Api.API_STATUS_CODE)));
  }

  private String getStatusCode(Event event) {
    return event
        .getEnrichedAttributes()
        .getAttributeMap()
        .get(Constants.getEnrichedSpanConstant(Api.API_STATUS_CODE))
        .getValue();
  }

  private String getStatusMessage(Event event) {
    return event
        .getEnrichedAttributes()
        .getAttributeMap()
        .get(Constants.getEnrichedSpanConstant(Api.API_STATUS_MESSAGE))
        .getValue();
  }

  private String getStatus(Event event) {
    return event
        .getEnrichedAttributes()
        .getAttributeMap()
        .get(Constants.getEnrichedSpanConstant(Api.API_STATUS))
        .getValue();
  }

  private void mockProtocol(Event event, Protocol protocol) {
    event
        .getEnrichedAttributes()
        .getAttributeMap()
        .put(
            Constants.getEnrichedSpanConstant(CommonAttribute.COMMON_ATTRIBUTE_PROTOCOL),
            AttributeValue.newBuilder()
                .setValue(Constants.getEnrichedSpanConstant(protocol))
                .build());
  }

  private void addAttribute(Event event,String key,String val) {
    event.getAttributes().getAttributeMap().put(key, AttributeValue.newBuilder().setValue(val).build());
  }
}
