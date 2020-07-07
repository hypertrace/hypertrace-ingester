package org.hypertrace.traceenricher.enrichment.enrichers;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
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
    event.getAttributes().getAttributeMap()
        .put(Constants.getRawSpanConstant(OTSpanTag.OT_SPAN_TAG_HTTP_STATUS_CODE),
            AttributeValue.newBuilder().setValue(expectedStatusCode).build()
        );

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
    event.getAttributes().getAttributeMap()
        .put(Constants.getRawSpanConstant(Grpc.GRPC_STATUS_CODE),
            AttributeValue.newBuilder().setValue(expectedStatusCode).build()
        );

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
    e.getAttributes().getAttributeMap()
        .put(Constants.getRawSpanConstant(Http.HTTP_RESPONSE_STATUS_CODE),
            AttributeValue.newBuilder().setValue("200").build());
    target.enrichEvent(null, e);
    assertEquals("200", getStatusCode(e));
  }

  @Test
  public void test_enrich_statusCode_grpc() {
    // Try the GRPC response length parsing.
    Event e = createMockEvent();
    mockProtocol(e, Protocol.PROTOCOL_GRPC);
    e.getAttributes().getAttributeMap()
        .put(Constants.getRawSpanConstant(Grpc.GRPC_STATUS_CODE),
            AttributeValue.newBuilder().setValue("5").build());
    target.enrichEvent(null, e);
    assertEquals("5", getStatusCode(e));
  }

  private String getStatusCode(Event event) {
    return event.getEnrichedAttributes().getAttributeMap()
        .get(Constants.getEnrichedSpanConstant(Api.API_STATUS_CODE)).getValue();
  }

  private String getStatusMessage(Event event) {
    return event.getEnrichedAttributes().getAttributeMap()
        .get(Constants.getEnrichedSpanConstant(Api.API_STATUS_MESSAGE)).getValue();
  }

  private String getStatus(Event event) {
    return event.getEnrichedAttributes().getAttributeMap()
        .get(Constants.getEnrichedSpanConstant(Api.API_STATUS)).getValue();
  }

  private void mockProtocol(Event event, Protocol protocol) {
    event.getEnrichedAttributes().getAttributeMap()
        .put(Constants.getEnrichedSpanConstant(CommonAttribute.COMMON_ATTRIBUTE_PROTOCOL),
            AttributeValue.newBuilder().setValue(Constants.getEnrichedSpanConstant(protocol)).build()
        );
  }
}
