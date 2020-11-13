package org.hypertrace.traceenricher.enrichment.enrichers.resolver.backend;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.EventRef;
import org.hypertrace.core.datamodel.EventRefType;
import org.hypertrace.core.datamodel.MetricValue;
import org.hypertrace.core.datamodel.Metrics;
import org.hypertrace.core.datamodel.eventfields.http.Request;
import org.hypertrace.core.datamodel.shared.StructuredTraceGraph;
import org.hypertrace.core.span.constants.v1.Grpc;
import org.hypertrace.core.span.constants.v1.Http;
import org.hypertrace.core.span.constants.v1.Mongo;
import org.hypertrace.core.span.constants.v1.Sql;
import org.hypertrace.entity.constants.v1.BackendAttribute;
import org.hypertrace.entity.constants.v1.K8sEntityAttribute;
import org.hypertrace.entity.constants.v1.ServiceAttribute;
import org.hypertrace.entity.data.service.client.EntityDataServiceClient;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.service.constants.EntityConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.Backend;
import org.hypertrace.traceenricher.enrichment.enrichers.AbstractAttributeEnricherTest;
import org.hypertrace.traceenricher.enrichment.enrichers.BackendType;
import org.hypertrace.traceenricher.enrichment.enrichers.resolver.FQNResolver;
import org.hypertrace.traceenricher.util.Constants;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

public class BackendEntityResolverTest extends AbstractAttributeEnricherTest {
  private static final String MONGO_URL = "mongo:27017";
  private static final String SERVICE_NAME_ATTR =
      EntityConstants.getValue(ServiceAttribute.SERVICE_ATTRIBUTE_NAME);

  @Mock
  private EntityDataServiceClient edsClient;

  private BackendEntityResolver backendEntityResolver;
  private StructuredTraceGraph structuredTraceGraph;

  @BeforeEach
  public void setup() {
    backendEntityResolver = new BackendEntityResolver(new FQNResolver(edsClient));
    structuredTraceGraph = mock(StructuredTraceGraph.class);
  }

  @Test
  public void checkBackendEntityGeneratedFromHttpEventType1() {
    Event e = Event.newBuilder().setCustomerId("__default")
        .setEventId(ByteBuffer.wrap("bdf03dfabf5c70f8".getBytes()))
        .setEntityIdList(Arrays.asList("4bfca8f7-4974-36a4-9385-dd76bf5c8824"))
        .setEnrichedAttributes(
            Attributes.newBuilder().setAttributeMap(
                Map.of("SPAN_TYPE", AttributeValue.newBuilder().setValue("EXIT").build(),
                    "PROTOCOL", AttributeValue.newBuilder().setValue("HTTP").build())).build())
        .setAttributes(Attributes.newBuilder().setAttributeMap(Map
            .of("http.status_code", AttributeValue.newBuilder().setValue("200").build(),
                "http.user_agent", AttributeValue.newBuilder().setValue("").build(),
                "http.path", AttributeValue.newBuilder().setValue("/product/5d644175551847d7408760b1").build(),
                "FLAGS", AttributeValue.newBuilder().setValue("OK").build(),
                "status.message", AttributeValue.newBuilder().setValue("200").build(),
                Constants.getRawSpanConstant(Http.HTTP_METHOD), AttributeValue.newBuilder().setValue("GET").build(),
                "http.host", AttributeValue.newBuilder().setValue("dataservice:9394").build(),
                "status.code", AttributeValue.newBuilder().setValue("0").build(),
                Constants.getEntityConstant(K8sEntityAttribute.K8S_ENTITY_ATTRIBUTE_CLUSTER_NAME), AttributeValue.newBuilder().setValue("devcluster").build(),
                Constants.getEntityConstant(K8sEntityAttribute.K8S_ENTITY_ATTRIBUTE_NAMESPACE_NAME), AttributeValue.newBuilder().setValue("mypastryshop").build()))
            .build())
        .setEventName("Sent./product/5d644175551847d7408760b1").setStartTimeMillis(1566869077746L)
        .setEndTimeMillis(1566869077750L).setMetrics(Metrics.newBuilder()
            .setMetricMap(Map.of("Duration", MetricValue.newBuilder().setValue(4.0).build()))
            .build())
        .setEventRefList(Arrays.asList(
            EventRef.newBuilder().setTraceId(ByteBuffer.wrap("random_trace_id".getBytes()))
                .setEventId(ByteBuffer.wrap("random_event_id".getBytes()))
                .setRefType(EventRefType.CHILD_OF).build()))
            .setHttp(org.hypertrace.core.datamodel.eventfields.http.Http.newBuilder()
                    .setRequest(Request.newBuilder()
                            .setHost("dataservice:9394")
                            .setPath("/product/5d644175551847d7408760b1")
                            .build())
                    .build())
            .build();

    final Entity backendEntity = backendEntityResolver.resolveEntity(e, structuredTraceGraph).get();
    assertEquals(backendEntity.getEntityName(), "dataservice.mypastryshop.devcluster:9394");
    assertEquals(3, backendEntity.getIdentifyingAttributesCount());
    assertEquals(BackendType.HTTP.name(),
        backendEntity.getIdentifyingAttributesMap().get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_PROTOCOL))
            .getValue().getString());
    assertEquals(
        backendEntity.getIdentifyingAttributesMap().get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_HOST)).getValue()
            .getString(), "dataservice.mypastryshop.devcluster");
    assertEquals(
        backendEntity.getIdentifyingAttributesMap().get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_PORT)).getValue()
            .getString(), "9394");
    assertEquals(
        backendEntity.getAttributesMap().get(Constants.getEnrichedSpanConstant(Backend.BACKEND_FROM_EVENT)).getValue().getString(),
        "Sent./product/5d644175551847d7408760b1");
    assertEquals(
        backendEntity.getAttributesMap().get(Constants.getEnrichedSpanConstant(Backend.BACKEND_FROM_EVENT_ID)).getValue()
            .getString(), "62646630336466616266356337306638");
    assertEquals(
        backendEntity.getAttributesMap().get(Constants.getRawSpanConstant(Http.HTTP_METHOD)).getValue()
            .getString(),
        "GET");
  }

  @Test
  public void checkBackendEntityGeneratedFromHttpEventType2() {
    Event e = Event.newBuilder().setCustomerId("__default")
        .setEventId(ByteBuffer.wrap("bdf03dfabf5c70f8".getBytes()))
        .setEntityIdList(Arrays.asList("4bfca8f7-4974-36a4-9385-dd76bf5c8824"))
        .setEnrichedAttributes(Attributes.newBuilder().setAttributeMap(
            Map.of("SPAN_TYPE", AttributeValue.newBuilder().setValue("EXIT").build(),
                "PROTOCOL", AttributeValue.newBuilder().setValue("HTTP").build(),
                Constants.getEntityConstant(K8sEntityAttribute.K8S_ENTITY_ATTRIBUTE_CLUSTER_NAME), AttributeValue.newBuilder().setValue("devcluster").build(),
                Constants.getEntityConstant(K8sEntityAttribute.K8S_ENTITY_ATTRIBUTE_NAMESPACE_NAME), AttributeValue.newBuilder().setValue("mypastryshop").build()
            )).build())
        .setAttributes(Attributes.newBuilder().setAttributeMap(
            Map.of("http.response.header.x-envoy-upstream-service-time", AttributeValue.newBuilder().setValue("11").build(),
                "http.response.header.x-forwarded-proto", AttributeValue.newBuilder().setValue("http").build(),
                "http.status_code", AttributeValue.newBuilder().setValue("200").build(),
                "FLAGS", AttributeValue.newBuilder().setValue("OK").build(),
                "http.protocol", AttributeValue.newBuilder().setValue("HTTP/1.1").build(),
                Constants.getRawSpanConstant(Http.HTTP_METHOD), AttributeValue.newBuilder().setValue("GET").build(),
                "http.url", AttributeValue.newBuilder().setValue("http://dataservice:9394/product/5d644175551847d7408760b4").build(),
                "downstream_cluster", AttributeValue.newBuilder().setValue("-").build()
            )).build())
        .setEventName("egress_http").setStartTimeMillis(1566869077746L)
        .setEndTimeMillis(1566869077750L).setMetrics(Metrics.newBuilder()
            .setMetricMap(Map.of("Duration", MetricValue.newBuilder().setValue(4.0).build())).build())
        .setEventRefList(Arrays.asList(
            EventRef.newBuilder().setTraceId(ByteBuffer.wrap("random_trace_id".getBytes()))
                .setEventId(ByteBuffer.wrap("random_event_id".getBytes()))
                .setRefType(EventRefType.CHILD_OF).build()))
            .setHttp(org.hypertrace.core.datamodel.eventfields.http.Http.newBuilder()
                    .setRequest(Request.newBuilder()
                            .setUrl("http://dataservice:9394/product/5d644175551847d7408760b4")
                            .setHost("dataservice:9394")
                            .setPath("product/5d644175551847d7408760b4")
                            .build())
                    .build())
            .build();

    final Entity backendEntity = backendEntityResolver.resolveEntity(e, structuredTraceGraph).get();
    assertEquals("dataservice.mypastryshop.devcluster:9394", backendEntity.getEntityName());
    assertEquals(3, backendEntity.getIdentifyingAttributesCount());
    assertEquals(BackendType.HTTP.name(),
        backendEntity.getIdentifyingAttributesMap().get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_PROTOCOL))
            .getValue().getString());
    assertEquals("dataservice.mypastryshop.devcluster",
        backendEntity.getIdentifyingAttributesMap().get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_HOST)).getValue()
            .getString());
    assertEquals(
        backendEntity.getIdentifyingAttributesMap().get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_PORT)).getValue()
            .getString(), "9394");
    assertEquals(
        backendEntity.getAttributesMap().get(Constants.getEnrichedSpanConstant(Backend.BACKEND_FROM_EVENT)).getValue().getString(),
        "egress_http");
    assertEquals("62646630336466616266356337306638",
        backendEntity.getAttributesMap().get(Constants.getEnrichedSpanConstant(Backend.BACKEND_FROM_EVENT_ID)).getValue()
            .getString());
    assertEquals("GET",
        backendEntity.getAttributesMap().get(Constants.getRawSpanConstant(Http.HTTP_METHOD)).getValue().getString());
  }

  @Test
  public void checkBackendEntityGeneratedFromHttpEventType3() {
    Event e = Event.newBuilder().setCustomerId("__default")
        .setEventId(ByteBuffer.wrap("bdf03dfabf5c70f8".getBytes()))
        .setEntityIdList(Arrays.asList("4bfca8f7-4974-36a4-9385-dd76bf5c8824")).setEnrichedAttributes(
            Attributes.newBuilder().setAttributeMap(
                Map.of("SPAN_TYPE", AttributeValue.newBuilder().setValue("EXIT").build(),
                    "PROTOCOL", AttributeValue.newBuilder().setValue("HTTP").build())).build())
        .setAttributes(Attributes.newBuilder().setAttributeMap(Map
            .of("http.request.method", AttributeValue.newBuilder().setValue("GET").build(), "FLAGS",
                AttributeValue.newBuilder().setValue("OK").build(), "http.request.url",
                AttributeValue.newBuilder()
                    .setValue("http://dataservice:9394/userreview?productId=5d644175551847d7408760b4")
                    .build(),
                Constants.getEntityConstant(K8sEntityAttribute.K8S_ENTITY_ATTRIBUTE_CLUSTER_NAME),
                AttributeValue.newBuilder().setValue("devcluster").build(),
                Constants.getEntityConstant(K8sEntityAttribute.K8S_ENTITY_ATTRIBUTE_NAMESPACE_NAME),
                AttributeValue.newBuilder().setValue("mypastryshop").build())).build()).setEventName("jaxrs.client.exit").setStartTimeMillis(1566869077746L)
        .setEndTimeMillis(1566869077750L).setMetrics(Metrics.newBuilder()
            .setMetricMap(Map.of("Duration", MetricValue.newBuilder().setValue(4.0).build())).build())
        .setEventRefList(Arrays.asList(
            EventRef.newBuilder().setTraceId(ByteBuffer.wrap("random_trace_id".getBytes()))
                .setEventId(ByteBuffer.wrap("random_event_id".getBytes()))
                .setRefType(EventRefType.CHILD_OF).build()))
            .setHttp(org.hypertrace.core.datamodel.eventfields.http.Http.newBuilder()
                    .setRequest(Request.newBuilder()
                            .setUrl("http://dataservice:9394/userreview?productId=5d644175551847d7408760b4")
                            .setHost("dataservice:9394")
                            .setPath("/userreview")
                            .setQueryString("productId=5d644175551847d7408760b4")
                            .build())
                    .build())
            .build();

    final Entity backendEntity = backendEntityResolver.resolveEntity(e, structuredTraceGraph).get();
    assertEquals("dataservice.mypastryshop.devcluster:9394", backendEntity.getEntityName());
    assertEquals(3, backendEntity.getIdentifyingAttributesCount());
    assertEquals(BackendType.HTTP.name(),
        backendEntity.getIdentifyingAttributesMap().get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_PROTOCOL))
            .getValue().getString());
    assertEquals("dataservice.mypastryshop.devcluster",
        backendEntity.getIdentifyingAttributesMap().get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_HOST)).getValue()
            .getString());
    assertEquals(
        backendEntity.getIdentifyingAttributesMap().get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_PORT)).getValue()
            .getString(), "9394");
    assertEquals(
        backendEntity.getAttributesMap().get(Constants.getEnrichedSpanConstant(Backend.BACKEND_FROM_EVENT)).getValue().getString(),
        "jaxrs.client.exit");
    assertEquals(
        backendEntity.getAttributesMap().get(Constants.getEnrichedSpanConstant(Backend.BACKEND_FROM_EVENT_ID)).getValue()
            .getString(), "62646630336466616266356337306638");
    assertEquals(
        backendEntity.getAttributesMap().get("http.request.method").getValue().getString(), "GET");
  }

  @Test
  public void checkBackendEntityGeneratedFromHttpsEvent() {
    Event e = Event.newBuilder().setCustomerId("__default")
        .setEventId(ByteBuffer.wrap("bdf03dfabf5c707f".getBytes()))
        .setEntityIdList(Arrays.asList("4bfca8f7-4974-36a4-9385-dd76bf5c8865"))
        .setEnrichedAttributes(
            Attributes.newBuilder().setAttributeMap(
                Map.of("SPAN_TYPE", AttributeValue.newBuilder().setValue("EXIT").build(),
                    "PROTOCOL", AttributeValue.newBuilder().setValue("HTTPS").build())).build())
        .setAttributes(Attributes.newBuilder().setAttributeMap(Map
            .of("http.status_code", AttributeValue.newBuilder().setValue("200").build(),
                "http.user_agent", AttributeValue.newBuilder().setValue("").build(),
                "http.path", AttributeValue.newBuilder().setValue("/product/5d644175551847d7408760b1").build(),
                "FLAGS", AttributeValue.newBuilder().setValue("OK").build(),
                "status.message", AttributeValue.newBuilder().setValue("200").build(),
                Constants.getRawSpanConstant(Http.HTTP_METHOD), AttributeValue.newBuilder().setValue("GET").build(),
                "http.host", AttributeValue.newBuilder().setValue("dataservice:9394").build(),
                "status.code", AttributeValue.newBuilder().setValue("0").build(),
                Constants.getEntityConstant(K8sEntityAttribute.K8S_ENTITY_ATTRIBUTE_CLUSTER_NAME), AttributeValue.newBuilder().setValue("devcluster").build(),
                Constants.getEntityConstant(K8sEntityAttribute.K8S_ENTITY_ATTRIBUTE_NAMESPACE_NAME), AttributeValue.newBuilder().setValue("mypastryshop").build()))
            .build())
        .setEventName("Sent./product/5d644175551847d7408760b1").setStartTimeMillis(1566869077746L)
        .setEndTimeMillis(1566869077750L).setMetrics(Metrics.newBuilder()
            .setMetricMap(Map.of("Duration", MetricValue.newBuilder().setValue(4.0).build()))
            .build())
        .setEventRefList(Arrays.asList(
            EventRef.newBuilder().setTraceId(ByteBuffer.wrap("random_trace_id".getBytes()))
                .setEventId(ByteBuffer.wrap("random_event_id".getBytes()))
                .setRefType(EventRefType.CHILD_OF).build()))
            .setHttp(org.hypertrace.core.datamodel.eventfields.http.Http.newBuilder()
                    .setRequest(Request.newBuilder()
                            .setHost("dataservice:9394")
                            .setPath("/product/5d644175551847d7408760b1")
                            .build())
                    .build())
            .build();

    final Entity backendEntity = backendEntityResolver.resolveEntity(e, structuredTraceGraph).get();
    assertEquals(backendEntity.getEntityName(), "dataservice.mypastryshop.devcluster:9394");
    assertEquals(3, backendEntity.getIdentifyingAttributesCount());
    assertEquals(BackendType.HTTPS.name(),
        backendEntity.getIdentifyingAttributesMap().get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_PROTOCOL))
            .getValue().getString());
    assertEquals(
        backendEntity.getIdentifyingAttributesMap().get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_HOST)).getValue()
            .getString(), "dataservice.mypastryshop.devcluster");
    assertEquals(
        backendEntity.getIdentifyingAttributesMap().get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_PORT)).getValue()
            .getString(), "9394");
    assertEquals(
        backendEntity.getAttributesMap().get(Constants.getEnrichedSpanConstant(Backend.BACKEND_FROM_EVENT)).getValue().getString(),
        "Sent./product/5d644175551847d7408760b1");
    assertEquals(
        backendEntity.getAttributesMap().get(Constants.getEnrichedSpanConstant(Backend.BACKEND_FROM_EVENT_ID)).getValue()
            .getString(), "62646630336466616266356337303766");
    assertEquals(
        backendEntity.getAttributesMap().get(Constants.getRawSpanConstant(Http.HTTP_METHOD)).getValue()
            .getString(),
        "GET");
  }

  @Test
  public void checkBackendEntityGeneratedFromHttpEventUrlWithIllegalQueryCharacter() {
    Event e = Event.newBuilder().setCustomerId("__default")
        .setEventId(ByteBuffer.wrap("bdf03dfabf5c70f8".getBytes()))
        .setEntityIdList(Arrays.asList("4bfca8f7-4974-36a4-9385-dd76bf5c8824"))
        .setEnrichedAttributes(Attributes.newBuilder().setAttributeMap(
            Map.of("SPAN_TYPE", AttributeValue.newBuilder().setValue("EXIT").build(),
                "PROTOCOL", AttributeValue.newBuilder().setValue("HTTP").build(),
                Constants.getEntityConstant(K8sEntityAttribute.K8S_ENTITY_ATTRIBUTE_CLUSTER_NAME), AttributeValue.newBuilder().setValue("devcluster").build(),
                Constants.getEntityConstant(K8sEntityAttribute.K8S_ENTITY_ATTRIBUTE_NAMESPACE_NAME), AttributeValue.newBuilder().setValue("mypastryshop").build()
            )).build())
        .setAttributes(Attributes.newBuilder().setAttributeMap(
            Map.of("http.response.header.x-envoy-upstream-service-time", AttributeValue.newBuilder().setValue("11").build(),
                "http.response.header.x-forwarded-proto", AttributeValue.newBuilder().setValue("http").build(),
                "http.status_code", AttributeValue.newBuilder().setValue("200").build(),
                "FLAGS", AttributeValue.newBuilder().setValue("OK").build(),
                "http.protocol", AttributeValue.newBuilder().setValue("HTTP/1.1").build(),
                Constants.getRawSpanConstant(Http.HTTP_METHOD), AttributeValue.newBuilder().setValue("GET").build(),
                "http.url", AttributeValue.newBuilder().setValue(
                    "http://dataservice:9394/api/timelines?uri=|%20wget%20https://iplogger.org/1pzQq7").build(),
                "downstream_cluster", AttributeValue.newBuilder().setValue("-").build()
            )).build())
        .setEventName("egress_http").setStartTimeMillis(1566869077746L)
        .setEndTimeMillis(1566869077750L).setMetrics(Metrics.newBuilder()
            .setMetricMap(Map.of("Duration", MetricValue.newBuilder().setValue(4.0).build())).build())
        .setEventRefList(Arrays.asList(
            EventRef.newBuilder().setTraceId(ByteBuffer.wrap("random_trace_id".getBytes()))
                .setEventId(ByteBuffer.wrap("random_event_id".getBytes()))
                .setRefType(EventRefType.CHILD_OF).build()))
        .setHttp(org.hypertrace.core.datamodel.eventfields.http.Http.newBuilder()
                .setRequest(Request.newBuilder()
                        .setUrl("http://dataservice:9394/api/timelines?uri=|%20wget%20https://iplogger.org/1pzQq7")
                        .setHost("dataservice:9394")
                        .setPath("/api/timelines")
                        .setQueryString("uri=|%20wget%20https://iplogger.org/1pzQq")
                        .build())
                .build())
        .build();

    Entity backendEntity = backendEntityResolver.resolveEntity(e, structuredTraceGraph).get();
    assertEquals("dataservice.mypastryshop.devcluster:9394", backendEntity.getEntityName());
    assertEquals(3, backendEntity.getIdentifyingAttributesCount());
    assertEquals("HTTP", backendEntity.getIdentifyingAttributesMap()
            .get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_PROTOCOL)).getValue().getString());
    assertEquals("dataservice.mypastryshop.devcluster", backendEntity.getIdentifyingAttributesMap()
            .get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_HOST)).getValue().getString());
    assertEquals("9394", backendEntity.getIdentifyingAttributesMap()
            .get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_PORT)).getValue().getString());
    assertEquals("egress_http", backendEntity.getAttributesMap()
            .get(Constants.getEnrichedSpanConstant(Backend.BACKEND_FROM_EVENT)).getValue().getString());
    assertEquals("62646630336466616266356337306638", backendEntity.getAttributesMap()
            .get(Constants.getEnrichedSpanConstant(Backend.BACKEND_FROM_EVENT_ID)).getValue().getString());
    assertEquals("GET", backendEntity.getAttributesMap().
            get(Constants.getRawSpanConstant(Http.HTTP_METHOD)).getValue().getString());
  }

  @Test
  public void checkBackendEntityGeneratedFromHttpEventUrlWithIllegalCharacterAndHttpHostSet() {
    Event e = Event.newBuilder().setCustomerId("__default")
        .setEventId(ByteBuffer.wrap("bdf03dfabf5c70f8".getBytes()))
        .setEntityIdList(Arrays.asList("4bfca8f7-4974-36a4-9385-dd76bf5c8824"))
        .setEnrichedAttributes(
            Attributes.newBuilder().setAttributeMap(
                Map.of("SPAN_TYPE", AttributeValue.newBuilder().setValue("EXIT").build(),
                    "PROTOCOL", AttributeValue.newBuilder().setValue("HTTP").build())).build())
        .setAttributes(Attributes.newBuilder().setAttributeMap(Map
            .of("http.status_code", AttributeValue.newBuilder().setValue("200").build(),
                "http.user_agent", AttributeValue.newBuilder().setValue("").build(),
                "http.url", AttributeValue.newBuilder().setValue("http://dataservice:9394/api/timelines?uri=|%20wget%20https://iplogger.org/1pzQq7").build(),
                "http.path", AttributeValue.newBuilder().setValue("/api/timelines?uri=|%20wget%20https://iplogger.org/1pzQq7").build(),
                "FLAGS", AttributeValue.newBuilder().setValue("OK").build(),
                "status.message", AttributeValue.newBuilder().setValue("200").build(),
                Constants.getRawSpanConstant(Http.HTTP_METHOD), AttributeValue.newBuilder().setValue("GET").build(),
                "http.host", AttributeValue.newBuilder().setValue("dataservice:9394").build(),
                Constants.getEntityConstant(K8sEntityAttribute.K8S_ENTITY_ATTRIBUTE_CLUSTER_NAME), AttributeValue.newBuilder().setValue("devcluster").build(),
                Constants.getEntityConstant(K8sEntityAttribute.K8S_ENTITY_ATTRIBUTE_NAMESPACE_NAME), AttributeValue.newBuilder().setValue("mypastryshop").build()))
            .build())
        .setEventName("Sent./api/timelines").setStartTimeMillis(1566869077746L)
        .setEndTimeMillis(1566869077750L).setMetrics(Metrics.newBuilder()
            .setMetricMap(Map.of("Duration", MetricValue.newBuilder().setValue(4.0).build()))
            .build())
        .setEventRefList(Arrays.asList(
            EventRef.newBuilder().setTraceId(ByteBuffer.wrap("random_trace_id".getBytes()))
                .setEventId(ByteBuffer.wrap("random_event_id".getBytes()))
                .setRefType(EventRefType.CHILD_OF).build()))
            .setHttp(org.hypertrace.core.datamodel.eventfields.http.Http.newBuilder()
                    .setRequest(Request.newBuilder()
                            .setUrl("http://dataservice:9394/api/timelines?uri=|%20wget%20https://iplogger.org/1pzQq7")
                            .setHost("dataservice:9394")
                            .setPath("/api/timelines")
                            .setQueryString("uri=|%20wget%20https://iplogger.org/1pzQq")
                            .build())
                    .build())
            .build();

    final Entity backendEntity = backendEntityResolver.resolveEntity(e, structuredTraceGraph).get();
    assertEquals(backendEntity.getEntityName(), "dataservice.mypastryshop.devcluster:9394");
    assertEquals(3, backendEntity.getIdentifyingAttributesCount());
    assertEquals(
        backendEntity.getIdentifyingAttributesMap().get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_PROTOCOL))
            .getValue().getString(), "HTTP");
    assertEquals(
        backendEntity.getIdentifyingAttributesMap().get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_HOST)).getValue()
            .getString(), "dataservice.mypastryshop.devcluster");
    assertEquals(
        backendEntity.getIdentifyingAttributesMap().get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_PORT)).getValue()
            .getString(), "9394");
    assertEquals(
        backendEntity.getAttributesMap().get(Constants.getEnrichedSpanConstant(Backend.BACKEND_FROM_EVENT)).getValue().getString(),
        "Sent./api/timelines");
    assertEquals(
        backendEntity.getAttributesMap().get(Constants.getEnrichedSpanConstant(Backend.BACKEND_FROM_EVENT_ID)).getValue()
            .getString(), "62646630336466616266356337306638");
    assertEquals(
        backendEntity.getAttributesMap().get(Constants.getRawSpanConstant(Http.HTTP_METHOD)).getValue()
            .getString(),
        "GET");
  }

  @Test
  public void checkBackendEntityGeneratedFromRedisEvent() {
    Event e = Event.newBuilder().setCustomerId("__default")
        .setEventId(ByteBuffer.wrap("bdf03dfabf5c70f8".getBytes()))
        .setEntityIdList(Arrays.asList("4bfca8f7-4974-36a4-9385-dd76bf5c8824")).setEnrichedAttributes(
            Attributes.newBuilder().setAttributeMap(
                Map.of("SPAN_TYPE", AttributeValue.newBuilder().setValue("EXIT").build())).build())
        .setAttributes(Attributes.newBuilder().setAttributeMap(Map
            .of("redis.connection", AttributeValue.newBuilder().setValue("redis-cart:6379").build(),
                "span.kind", AttributeValue.newBuilder().setValue("client").build(), "redis.command",
                AttributeValue.newBuilder().setValue("GET").build(), "k8s.pod_id",
                AttributeValue.newBuilder().setValue("55636196-c840-11e9-a417-42010a8a0064").build(),
                "docker.container_id", AttributeValue.newBuilder()
                    .setValue("ee85cf2cfc3b24613a3da411fdbd2f3eabbe729a5c86c5262971c8d8c29dad0f").build(),
                "FLAGS", AttributeValue.newBuilder().setValue("0").build(), "redis.args",
                AttributeValue.newBuilder().setValue("key<product_5d644175551847d7408760b3>").build(),
                Constants.getEntityConstant(K8sEntityAttribute.K8S_ENTITY_ATTRIBUTE_CLUSTER_NAME),
                AttributeValue.newBuilder().setValue("devcluster").build(),
                Constants.getEntityConstant(K8sEntityAttribute.K8S_ENTITY_ATTRIBUTE_NAMESPACE_NAME),
                AttributeValue.newBuilder().setValue("mypastryshop").build()))
            .build()).setEventName("reactive.redis.exit").setStartTimeMillis(1566869077746L)
        .setEndTimeMillis(1566869077750L).setMetrics(Metrics.newBuilder()
            .setMetricMap(Map.of("Duration", MetricValue.newBuilder().setValue(4.0).build())).build())
        .setEventRefList(Arrays.asList(
            EventRef.newBuilder().setTraceId(ByteBuffer.wrap("random_trace_id".getBytes()))
                .setEventId(ByteBuffer.wrap("random_event_id".getBytes()))
                .setRefType(EventRefType.CHILD_OF).build())).build();
    final Entity backendEntity = backendEntityResolver.resolveEntity(e, structuredTraceGraph).get();
    assertEquals("redis-cart.mypastryshop.devcluster:6379", backendEntity.getEntityName());
    assertEquals(3, backendEntity.getIdentifyingAttributesCount());
    assertEquals(
        backendEntity.getIdentifyingAttributesMap().get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_PROTOCOL))
            .getValue().getString(), "REDIS");
    assertEquals(
        backendEntity.getIdentifyingAttributesMap().get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_HOST)).getValue()
            .getString(), "redis-cart.mypastryshop.devcluster");
    assertEquals(
        backendEntity.getIdentifyingAttributesMap().get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_PORT)).getValue()
            .getString(), "6379");
    assertEquals(
        backendEntity.getAttributesMap().get(Constants.getEnrichedSpanConstant(Backend.BACKEND_FROM_EVENT)).getValue().getString(),
        "reactive.redis.exit");
    assertEquals(
        backendEntity.getAttributesMap().get(Constants.getEnrichedSpanConstant(Backend.BACKEND_FROM_EVENT_ID)).getValue()
            .getString(), "62646630336466616266356337306638");
    assertEquals(backendEntity.getAttributesMap().get("redis.command").getValue().getString(),
        "GET");
    assertEquals(backendEntity.getAttributesMap().get("redis.args").getValue().getString(),
        "key<product_5d644175551847d7408760b3>");
  }

  @Test
  public void checkBackendEntityGeneratedFromUninstrumentedMongoEvent() {
    Event e = Event.newBuilder().setCustomerId("__default")
        .setEventId(ByteBuffer.wrap("bdf03dfabf5c70f8".getBytes()))
        .setEntityIdList(Arrays.asList("4bfca8f7-4974-36a4-9385-dd76bf5c8824")).setEnrichedAttributes(
            Attributes.newBuilder().setAttributeMap(
                Map.of("SPAN_TYPE", AttributeValue.newBuilder().setValue("EXIT").build())).build())
        .setAttributes(Attributes.newBuilder().setAttributeMap(Map
            .of("NAMESPACE", AttributeValue.newBuilder().setValue("sampleshop.userReview").build(),
                "span.kind", AttributeValue.newBuilder().setValue("client").build(), "OPERATION",
                AttributeValue.newBuilder().setValue("FindOperation").build(), "k8s.pod_id",
                AttributeValue.newBuilder().setValue("55636196-c840-11e9-a417-42010a8a0064").build(),
                "docker.container_id", AttributeValue.newBuilder()
                    .setValue("ee85cf2cfc3b24613a3da411fdbd2f3eabbe729a5c86c5262971c8d8c29dad0f").build(),
                "FLAGS", AttributeValue.newBuilder().setValue("0").build(), "address",
                AttributeValue.newBuilder().setValue(MONGO_URL).build(),
                Constants.getEntityConstant(K8sEntityAttribute.K8S_ENTITY_ATTRIBUTE_CLUSTER_NAME),
                AttributeValue.newBuilder().setValue("devcluster").build(),
                Constants.getEntityConstant(K8sEntityAttribute.K8S_ENTITY_ATTRIBUTE_NAMESPACE_NAME),
                AttributeValue.newBuilder().setValue("sampleshop").build())).build())
        .setEventName("mongo.async.exit").setStartTimeMillis(1566869077746L)
        .setEndTimeMillis(1566869077750L).setMetrics(Metrics.newBuilder()
            .setMetricMap(Map.of("Duration", MetricValue.newBuilder().setValue(4.0).build())).build())
        .setEventRefList(Arrays.asList(
            EventRef.newBuilder().setTraceId(ByteBuffer.wrap("random_trace_id".getBytes()))
                .setEventId(ByteBuffer.wrap("random_event_id".getBytes()))
                .setRefType(EventRefType.CHILD_OF).build())).build();
    final Entity backendEntity = backendEntityResolver.resolveEntity(e, structuredTraceGraph).get();
    assertEquals("mongo.sampleshop.devcluster:27017", backendEntity.getEntityName());
    assertEquals(3, backendEntity.getIdentifyingAttributesCount());
    assertEquals(
        backendEntity.getIdentifyingAttributesMap().get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_PROTOCOL))
            .getValue().getString(), "MONGO");
    assertEquals("mongo.sampleshop.devcluster",
        backendEntity.getIdentifyingAttributesMap().get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_HOST)).getValue()
            .getString());
    assertEquals(
        backendEntity.getIdentifyingAttributesMap().get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_PORT)).getValue()
            .getString(), "27017");
    assertEquals(backendEntity.getAttributesMap().get("NAMESPACE").getValue().getString(),
        "sampleshop.userReview");
    assertEquals(
        backendEntity.getAttributesMap().get(Constants.getEnrichedSpanConstant(Backend.BACKEND_FROM_EVENT)).getValue().getString(),
        "mongo.async.exit");
    assertEquals(
        backendEntity.getAttributesMap().get(Constants.getEnrichedSpanConstant(Backend.BACKEND_FROM_EVENT_ID)).getValue()
            .getString(), "62646630336466616266356337306638");
  }

  @Test
  public void checkBackendEntityGeneratedFromInstrumentedMongoEvent() {
    Event e = Event.newBuilder().setCustomerId("__default")
        .setEventId(ByteBuffer.wrap("bdf03dfabf5c70f8".getBytes()))
        .setEntityIdList(Arrays.asList("4bfca8f7-4974-36a4-9385-dd76bf5c8824")).setEnrichedAttributes(
            Attributes.newBuilder().setAttributeMap(
                Map.of("SPAN_TYPE", AttributeValue.newBuilder().setValue("EXIT").build())).build())
        .setAttributes(Attributes.newBuilder().setAttributeMap(Map
            .of("mongo.namespace", AttributeValue.newBuilder().setValue("sampleshop.userReview").build(),
                "span.kind", AttributeValue.newBuilder().setValue("client").build(),
                "OPERATION",
                AttributeValue.newBuilder().setValue("FindOperation").build(),
                "k8s.pod_id",
                AttributeValue.newBuilder().setValue("55636196-c840-11e9-a417-42010a8a0064").build(),
                "docker.container_id",
                AttributeValue.newBuilder()
                    .setValue("ee85cf2cfc3b24613a3da411fdbd2f3eabbe729a5c86c5262971c8d8c29dad0f").build(),
                "FLAGS", AttributeValue.newBuilder().setValue("0").build(),
                "mongo.operation", AttributeValue.newBuilder().setValue("HelloWorld").build(),
                "mongo.url",
                AttributeValue.newBuilder().setValue(MONGO_URL).build(),
                Constants.getEntityConstant(K8sEntityAttribute.K8S_ENTITY_ATTRIBUTE_CLUSTER_NAME),
                AttributeValue.newBuilder().setValue("devcluster").build(),
                Constants.getEntityConstant(K8sEntityAttribute.K8S_ENTITY_ATTRIBUTE_NAMESPACE_NAME),
                AttributeValue.newBuilder().setValue("sampleshop").build())).build())
        .setEventName("mongo.async.exit").setStartTimeMillis(1566869077746L)
        .setEndTimeMillis(1566869077750L).setMetrics(Metrics.newBuilder()
            .setMetricMap(Map.of("Duration", MetricValue.newBuilder().setValue(4.0).build())).build())
        .setEventRefList(Arrays.asList(
            EventRef.newBuilder().setTraceId(ByteBuffer.wrap("random_trace_id".getBytes()))
                .setEventId(ByteBuffer.wrap("random_event_id".getBytes()))
                .setRefType(EventRefType.CHILD_OF).build())).build();
    final Entity backendEntity = backendEntityResolver.resolveEntity(e, structuredTraceGraph).get();
    assertEquals("mongo.sampleshop.devcluster:27017", backendEntity.getEntityName());
    assertEquals(3, backendEntity.getIdentifyingAttributesCount());
    assertEquals(
        backendEntity.getIdentifyingAttributesMap().get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_PROTOCOL))
            .getValue().getString(), "MONGO");
    assertEquals("mongo.sampleshop.devcluster",
        backendEntity.getIdentifyingAttributesMap().get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_HOST)).getValue()
            .getString());
    assertEquals(
        backendEntity.getIdentifyingAttributesMap().get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_PORT)).getValue()
            .getString(), "27017");
    assertEquals(backendEntity.getAttributesMap().get(Constants.getRawSpanConstant(Mongo.MONGO_NAMESPACE)).getValue().getString(),
        "sampleshop.userReview");

    assertEquals(backendEntity.getAttributesMap().get(Constants.getRawSpanConstant(Mongo.MONGO_OPERATION)).getValue().getString(),
        "HelloWorld");
    assertEquals(
        backendEntity.getAttributesMap().get(Constants.getEnrichedSpanConstant(Backend.BACKEND_FROM_EVENT)).getValue().getString(),
        "mongo.async.exit");
    assertEquals(
        backendEntity.getAttributesMap().get(Constants.getEnrichedSpanConstant(Backend.BACKEND_FROM_EVENT_ID)).getValue()
            .getString(), "62646630336466616266356337306638");
  }

  @Test
  public void checkBackendEntityGeneratedFromGrpcEvent() {
    Map<String, AttributeValue> attributeMap = ImmutableMap.<String, AttributeValue>builder()
        .put("grpc.method", AttributeValue.newBuilder().setValue("/hipstershop.ProductCatalogService/ListProducts").build())
        .put("span.kind", AttributeValue.newBuilder().setValue("client").build())
        .put("component", AttributeValue.newBuilder().setValue("grpc").build())
        .put("k8s.pod_id",
            AttributeValue.newBuilder().setValue("55636196-c840-11e9-a417-42010a8a0064").build())
        .put("docker.container_id", AttributeValue.newBuilder()
            .setValue("ee85cf2cfc3b24613a3da411fdbd2f3eabbe729a5c86c5262971c8d8c29dad0f").build())
        .put("FLAGS", AttributeValue.newBuilder().setValue("0").build())
        .put("grpc.host_port",
            AttributeValue.newBuilder().setValue("productcatalogservice:3550").build())
        .put("grpc.response.body", AttributeValue.newBuilder().setValue(
            "products {\\n  id: \\\"5d644175551847d7408760b5\\\"\\n  name: \\\"Vintage Record Player\\\"\\n  description: \\\"It still works.\\\"\\n  picture: \\\"/static")
            .build())
        .put("grpc.request.body", AttributeValue.newBuilder().setValue("").build())
        .put(Constants.getEntityConstant(K8sEntityAttribute.K8S_ENTITY_ATTRIBUTE_CLUSTER_NAME),
            AttributeValue.newBuilder().setValue("devcluster").build())
        .put(Constants.getEntityConstant(K8sEntityAttribute.K8S_ENTITY_ATTRIBUTE_NAMESPACE_NAME),
            AttributeValue.newBuilder().setValue("hipstershop").build())
        .build();
    Event e = Event.newBuilder().setCustomerId("__default")
        .setEventId(ByteBuffer.wrap("bdf03dfabf5c70f8".getBytes()))
        .setEntityIdList(Arrays.asList("4bfca8f7-4974-36a4-9385-dd76bf5c8824")).setEnrichedAttributes(
            Attributes.newBuilder().setAttributeMap(
                Map.of("SPAN_TYPE", AttributeValue.newBuilder().setValue("EXIT").build(),
                    "PROTOCOL", AttributeValue.newBuilder().setValue("GRPC").build())).build())
        .setAttributes(Attributes.newBuilder().setAttributeMap(attributeMap).build())
        .setEventName("Sent.hipstershop.ProductCatalogService.ListProducts")
        .setStartTimeMillis(1566869077746L).setEndTimeMillis(1566869077750L).setMetrics(
            Metrics.newBuilder()
                .setMetricMap(Map.of("Duration", MetricValue.newBuilder().setValue(4.0).build())).build())
        .setEventRefList(Arrays.asList(
            EventRef.newBuilder().setTraceId(ByteBuffer.wrap("random_trace_id".getBytes()))
                .setEventId(ByteBuffer.wrap("random_event_id".getBytes()))
                .setRefType(EventRefType.CHILD_OF).build())).build();
    final Entity backendEntity = backendEntityResolver.resolveEntity(e, structuredTraceGraph).get();
    assertEquals("productcatalogservice.hipstershop.devcluster:3550", backendEntity.getEntityName());
    assertEquals(3, backendEntity.getIdentifyingAttributesCount());
    assertEquals(
        backendEntity.getIdentifyingAttributesMap().get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_PROTOCOL))
            .getValue().getString(), "GRPC");
    assertEquals(
        backendEntity.getIdentifyingAttributesMap().get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_HOST)).getValue()
            .getString(), "productcatalogservice.hipstershop.devcluster");
    assertEquals(
        backendEntity.getIdentifyingAttributesMap().get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_PORT)).getValue()
            .getString(), "3550");
    assertEquals(
        backendEntity.getAttributesMap().get(Constants.getEnrichedSpanConstant(Backend.BACKEND_FROM_EVENT)).getValue().getString(),
        "Sent.hipstershop.ProductCatalogService.ListProducts");
    assertEquals(
        backendEntity.getAttributesMap().get(Constants.getEnrichedSpanConstant(Backend.BACKEND_FROM_EVENT_ID)).getValue()
            .getString(), "62646630336466616266356337306638");
    assertEquals(
        backendEntity.getAttributesMap().get(Constants.getRawSpanConstant(Grpc.GRPC_METHOD)).getValue().getString(),
        "/hipstershop.ProductCatalogService/ListProducts");
  }

  @Test
  public void checkBackendEntityGeneratedFromJdbcEvent() {
    Event e = Event.newBuilder().setCustomerId("__default")
        .setEventId(ByteBuffer.wrap("bdf03dfabf5c70f8".getBytes()))
        .setEntityIdList(Arrays.asList("4bfca8f7-4974-36a4-9385-dd76bf5c8824")).setEnrichedAttributes(
            Attributes.newBuilder().setAttributeMap(
                Map.of("SPAN_TYPE", AttributeValue.newBuilder().setValue("EXIT").build())).build())
        .setAttributes(Attributes.newBuilder().setAttributeMap(Map
            .of("sql.url", AttributeValue.newBuilder().setValue("jdbc:mysql://mysql:3306/shop").build(),
                "span.kind", AttributeValue.newBuilder().setValue("client").build(), "sql.query",
                AttributeValue.newBuilder()
                    .setValue("insert into audit_message (message, id) values (?, ?)").build(),
                "k8s.pod_id",
                AttributeValue.newBuilder().setValue("55636196-c840-11e9-a417-42010a8a0064").build(),
                "docker.container_id", AttributeValue.newBuilder()
                    .setValue("ee85cf2cfc3b24613a3da411fdbd2f3eabbe729a5c86c5262971c8d8c29dad0f").build(),
                "FLAGS", AttributeValue.newBuilder().setValue("0").build(),
                Constants.getEntityConstant(K8sEntityAttribute.K8S_ENTITY_ATTRIBUTE_CLUSTER_NAME),
                AttributeValue.newBuilder().setValue("devcluster").build(),
                Constants.getEntityConstant(K8sEntityAttribute.K8S_ENTITY_ATTRIBUTE_NAMESPACE_NAME),
                AttributeValue.newBuilder().setValue("hipstershop").build())).build())
        .setEventName("jdbc.connection.prepare").setStartTimeMillis(1566869077746L)
        .setEndTimeMillis(1566869077750L).setMetrics(Metrics.newBuilder()
            .setMetricMap(Map.of("Duration", MetricValue.newBuilder().setValue(4.0).build())).build())
        .setEventRefList(Arrays.asList(
            EventRef.newBuilder().setTraceId(ByteBuffer.wrap("random_trace_id".getBytes()))
                .setEventId(ByteBuffer.wrap("random_event_id".getBytes()))
                .setRefType(EventRefType.CHILD_OF).build())).build();
    final Entity backendEntity = backendEntityResolver.resolveEntity(e, structuredTraceGraph).get();
    assertEquals("mysql.hipstershop.devcluster:3306", backendEntity.getEntityName());
    assertEquals(4, backendEntity.getIdentifyingAttributesCount());
    Assertions.assertEquals(BackendType.JDBC.name(),
        backendEntity.getIdentifyingAttributesMap().get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_PROTOCOL))
            .getValue().getString());
    assertEquals("mysql.hipstershop.devcluster",
        backendEntity.getIdentifyingAttributesMap().get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_HOST)).getValue()
            .getString());
    assertEquals("3306",
        backendEntity.getIdentifyingAttributesMap().get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_PORT)).getValue()
            .getString());
    assertEquals("mysql",
        backendEntity.getIdentifyingAttributesMap().get(Constants.getRawSpanConstant(Sql.SQL_DB_TYPE)).getValue().getString());
    assertEquals("jdbc.connection.prepare",
        backendEntity.getAttributesMap().get(Constants.getEnrichedSpanConstant(Backend.BACKEND_FROM_EVENT)).getValue().getString());
    assertEquals("62646630336466616266356337306638",
        backendEntity.getAttributesMap().get(Constants.getEnrichedSpanConstant(Backend.BACKEND_FROM_EVENT_ID)).getValue()
            .getString());
  }

  @Test
  public void testGetBackendEntity() {
    Event e = Event.newBuilder().setCustomerId("__default")
        .setEventId(ByteBuffer.wrap("bdf03dfabf5c70f8".getBytes()))
        .setEntityIdList(Arrays.asList("4bfca8f7-4974-36a4-9385-dd76bf5c8824")).setEnrichedAttributes(
            Attributes.newBuilder().setAttributeMap(
                Map.of("SPAN_TYPE", AttributeValue.newBuilder().setValue("EXIT").build())).build())
        .setAttributes(Attributes.newBuilder().setAttributeMap(Map
            .of("sql.url", AttributeValue.newBuilder().setValue("jdbc:mysql://mysql:3306/shop").build(),
                "span.kind", AttributeValue.newBuilder().setValue("client").build(), "sql.query",
                AttributeValue.newBuilder()
                    .setValue("insert into audit_message (message, id) values (?, ?)").build(),
                "k8s.pod_id",
                AttributeValue.newBuilder().setValue("55636196-c840-11e9-a417-42010a8a0064").build(),
                "docker.container_id", AttributeValue.newBuilder()
                    .setValue("ee85cf2cfc3b24613a3da411fdbd2f3eabbe729a5c86c5262971c8d8c29dad0f").build(),
                "FLAGS", AttributeValue.newBuilder().setValue("0").build(),
                Constants.getEntityConstant(K8sEntityAttribute.K8S_ENTITY_ATTRIBUTE_CLUSTER_NAME),
                AttributeValue.newBuilder().setValue("devcluster").build(),
                Constants.getEntityConstant(K8sEntityAttribute.K8S_ENTITY_ATTRIBUTE_NAMESPACE_NAME),
                AttributeValue.newBuilder().setValue("hipstershop").build())).build())
        .setEventName("jdbc.connection.prepare").setStartTimeMillis(1566869077746L)
        .setEndTimeMillis(1566869077750L).setMetrics(Metrics.newBuilder()
            .setMetricMap(Map.of("Duration", MetricValue.newBuilder().setValue(4.0).build())).build())
        .setEventRefList(Arrays.asList(
            EventRef.newBuilder().setTraceId(ByteBuffer.wrap("random_trace_id".getBytes()))
                .setEventId(ByteBuffer.wrap("random_event_id".getBytes()))
                .setRefType(EventRefType.CHILD_OF).build())).build();
    Entity backendEntity = backendEntityResolver.resolveEntity(e, structuredTraceGraph).get();
    Map<String, org.hypertrace.entity.data.service.v1.AttributeValue> idAttrMap = backendEntity.getIdentifyingAttributesMap();
    assertEquals("mysql.hipstershop.devcluster", idAttrMap.get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_HOST)).getValue().getString());
    assertEquals("3306", idAttrMap.get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_PORT)).getValue().getString());
    assertEquals("JDBC", idAttrMap.get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_PROTOCOL)).getValue().getString());
    assertEquals("mysql", idAttrMap.get(Constants.getRawSpanConstant(Sql.SQL_DB_TYPE)).getValue().getString());
  }

  @Test
  public void checkFallbackBackendEntityGeneratedFromClientExitSpan() {
    Event e = Event.newBuilder().setCustomerId("__default")
        .setEventId(ByteBuffer.wrap("bdf03dfabf5c70f8".getBytes()))
        .setEntityIdList(Arrays.asList("4bfca8f7-4974-36a4-9385-dd76bf5c8824")).setEnrichedAttributes(
            Attributes.newBuilder().setAttributeMap(
                Map.of("SPAN_TYPE", AttributeValue.newBuilder().setValue("EXIT").build())).build())
        .setAttributes(Attributes.newBuilder().setAttributeMap(Map
            .of(
                "span.kind", AttributeValue.newBuilder().setValue("client").build(),
                "k8s.pod_id",
                AttributeValue.newBuilder().setValue("55636196-c840-11e9-a417-42010a8a0064").build(),
                "docker.container_id", AttributeValue.newBuilder()
                    .setValue("ee85cf2cfc3b24613a3da411fdbd2f3eabbe729a5c86c5262971c8d8c29dad0f").build(),
                "FLAGS", AttributeValue.newBuilder().setValue("0").build(),
                Constants.getEntityConstant(K8sEntityAttribute.K8S_ENTITY_ATTRIBUTE_CLUSTER_NAME),
                AttributeValue.newBuilder().setValue("devcluster").build(),
                Constants.getEntityConstant(K8sEntityAttribute.K8S_ENTITY_ATTRIBUTE_NAMESPACE_NAME),
                AttributeValue.newBuilder().setValue("hipstershop").build())).build())
        .setEventName("redis::getDrivers").setStartTimeMillis(1566869077746L)
        .setEndTimeMillis(1566869077750L).setMetrics(Metrics.newBuilder()
            .setMetricMap(Map.of("Duration", MetricValue.newBuilder().setValue(4.0).build())).build())
        .setServiceName("redis")
        .setEventRefList(Arrays.asList(
            EventRef.newBuilder().setTraceId(ByteBuffer.wrap("random_trace_id".getBytes()))
                .setEventId(ByteBuffer.wrap("random_event_id".getBytes()))
                .setRefType(EventRefType.CHILD_OF).build())).build();


    Event parentEvent = Event.newBuilder().setCustomerId("__default")
        .setEventId(ByteBuffer.wrap("random".getBytes()))
        .setAttributes(Attributes.newBuilder().setAttributeMap(Map
            .of(
                "span.kind", AttributeValue.newBuilder().setValue("server").build())).build())
        .setEnrichedAttributes(Attributes.newBuilder().setAttributeMap(Map
            .of(
                SERVICE_NAME_ATTR, AttributeValue.newBuilder().setValue("customer").build()
            )).build())
        .setEventName("getDrivers").setStartTimeMillis(1566869077746L)
        .build();


    when(structuredTraceGraph.getParentEvent(e)).thenReturn(parentEvent);
    Entity backendEntity = backendEntityResolver.resolveEntity(e, structuredTraceGraph).get();
    assertEquals("redis.hipstershop.devcluster", backendEntity.getIdentifyingAttributesMap().get(Constants.getEntityConstant(BackendAttribute.BACKEND_ATTRIBUTE_HOST)).getValue().getString());
  }

  @Test
  public void checkFallbackBackendEntityGeneratedFromClientExitSpanServiceNameSameAsParentServiceName() {
    Event e = Event.newBuilder().setCustomerId("__default")
        .setEventId(ByteBuffer.wrap("bdf03dfabf5c70f8".getBytes()))
        .setEntityIdList(Arrays.asList("4bfca8f7-4974-36a4-9385-dd76bf5c8824")).setEnrichedAttributes(
            Attributes.newBuilder().setAttributeMap(
                Map.of("SPAN_TYPE", AttributeValue.newBuilder().setValue("EXIT").build())).build())
        .setAttributes(Attributes.newBuilder().setAttributeMap(Map
            .of(
                "span.kind", AttributeValue.newBuilder().setValue("client").build(),
                "k8s.pod_id",
                AttributeValue.newBuilder().setValue("55636196-c840-11e9-a417-42010a8a0064").build(),
                "docker.container_id", AttributeValue.newBuilder()
                    .setValue("ee85cf2cfc3b24613a3da411fdbd2f3eabbe729a5c86c5262971c8d8c29dad0f").build(),
                "FLAGS", AttributeValue.newBuilder().setValue("0").build(),
                Constants.getEntityConstant(K8sEntityAttribute.K8S_ENTITY_ATTRIBUTE_CLUSTER_NAME),
                AttributeValue.newBuilder().setValue("devcluster").build(),
                Constants.getEntityConstant(K8sEntityAttribute.K8S_ENTITY_ATTRIBUTE_NAMESPACE_NAME),
                AttributeValue.newBuilder().setValue("hipstershop").build())).build())
        .setEventName("redis::getDrivers").setStartTimeMillis(1566869077746L)
        .setEndTimeMillis(1566869077750L).setMetrics(Metrics.newBuilder()
            .setMetricMap(Map.of("Duration", MetricValue.newBuilder().setValue(4.0).build())).build())
        .setServiceName("redis")
        .setEventRefList(Arrays.asList(
            EventRef.newBuilder().setTraceId(ByteBuffer.wrap("random_trace_id".getBytes()))
                .setEventId(ByteBuffer.wrap("random_event_id".getBytes()))
                .setRefType(EventRefType.CHILD_OF).build())).build();


    Event parentEvent = Event.newBuilder().setCustomerId("__default")
        .setEventId(ByteBuffer.wrap("random".getBytes()))
        .setAttributes(Attributes.newBuilder().setAttributeMap(Map
            .of(
                "span.kind", AttributeValue.newBuilder().setValue("server").build())).build())
        .setEnrichedAttributes(Attributes.newBuilder().setAttributeMap(Map
            .of(
                SERVICE_NAME_ATTR, AttributeValue.newBuilder().setValue("redis").build()
            )).build())
        .setEventName("getDrivers").setStartTimeMillis(1566869077746L)
        .build();

    //Since the event serviceName is same as parentServiceName, no new service entity should be created.
    when(structuredTraceGraph.getParentEvent(e)).thenReturn(parentEvent);
    Assertions.assertTrue(backendEntityResolver.resolveEntity(e, structuredTraceGraph).isEmpty());
  }
}
