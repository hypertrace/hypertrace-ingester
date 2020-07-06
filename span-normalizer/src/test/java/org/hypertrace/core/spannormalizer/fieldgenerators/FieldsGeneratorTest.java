package org.hypertrace.core.spannormalizer.fieldgenerators;

import static org.hypertrace.core.span.constants.v1.Grpc.GRPC_HOST_PORT;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_HEADER;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_REQUEST_METHOD;
import static org.hypertrace.core.span.constants.v1.Http.HTTP_URL;
import static org.hypertrace.core.span.constants.v1.Sql.SQL_DB_TYPE;
import static org.hypertrace.core.spannormalizer.utils.TestUtils.createKeyValue;

import io.jaegertracing.api_v2.JaegerSpanInternalModel;
import java.util.HashMap;
import java.util.Map;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FieldsGeneratorTest {
  @Test
  public void testFieldsGenerator() {
    FieldsGenerator fieldsGenerator = new FieldsGenerator();
    Map<String, JaegerSpanInternalModel.KeyValue> tagsMap = new HashMap<>();
    tagsMap.put(RawSpanConstants.getValue(HTTP_REQUEST_METHOD), createKeyValue("GET"));
    tagsMap.put(RawSpanConstants.getValue(GRPC_HOST_PORT), createKeyValue("localhost:50051"));
    tagsMap.put(RawSpanConstants.getValue(SQL_DB_TYPE), createKeyValue("mysql"));
    tagsMap.put(
        RawSpanConstants.getValue(HTTP_REQUEST_HEADER) + ".authorization",
        createKeyValue("Bearer some-auth-header"));
    tagsMap.put(RawSpanConstants.getValue(HTTP_URL), createKeyValue("https://example.ai/url2"));

    Event.Builder eventBuilder = Event.newBuilder();

    // These are the two calls made to generate fields from the tags
    tagsMap.forEach(
        (tagKey, tagKeyValue) ->
            fieldsGenerator.addValueToBuilder(
                tagKey.toLowerCase(), tagKeyValue, eventBuilder, tagsMap));
    // Some http fields can be populated from other fields eg. method, scheme from url.
    fieldsGenerator.populateOtherFields(eventBuilder);

    Assertions.assertEquals("GET", eventBuilder.getHttpBuilder().getRequestBuilder().getMethod());
    Assertions.assertEquals(
        "localhost:50051", eventBuilder.getGrpcBuilder().getRequestBuilder().getHostPort());
    Assertions.assertEquals("mysql", eventBuilder.getSqlBuilder().getDbType());
    Assertions.assertEquals(
        "Bearer some-auth-header",
        eventBuilder
            .getHttpBuilder()
            .getRequestBuilder()
            .getHeadersBuilder()
            .getOtherHeaders()
            .get("authorization"));
    Assertions.assertEquals(
        "https://example.ai/url2", eventBuilder.getHttpBuilder().getRequestBuilder().getUrl());
    Assertions.assertEquals(
        "example.ai", eventBuilder.getHttpBuilder().getRequestBuilder().getHost());
    Assertions.assertEquals("https", eventBuilder.getHttpBuilder().getRequestBuilder().getScheme());
    Assertions.assertEquals("/url2", eventBuilder.getHttpBuilder().getRequestBuilder().getPath());
  }
}
