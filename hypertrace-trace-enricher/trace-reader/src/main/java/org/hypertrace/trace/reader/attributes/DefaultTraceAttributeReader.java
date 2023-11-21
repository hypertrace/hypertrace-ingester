package org.hypertrace.trace.reader.attributes;

import static org.hypertrace.trace.reader.attributes.ValueSource.TRACE_SCOPE;

import java.util.Optional;
import org.hypertrace.core.attribute.service.client.AttributeServiceCachedClient;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.LiteralValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.grpcutils.context.RequestContext;

class DefaultTraceAttributeReader implements TraceAttributeReader<StructuredTrace, Event> {

  private final AttributeServiceCachedClient attributeClient;
  private final ValueResolver valueResolver;

  DefaultTraceAttributeReader(AttributeServiceCachedClient attributeClient) {
    this.attributeClient = attributeClient;
    this.valueResolver = ValueResolver.build(this.attributeClient);
  }

  @Override
  public Optional<LiteralValue> getSpanValue(
      StructuredTrace trace, Event span, String attributeScope, String attributeKey) {
    ValueSource valueSource = ValueSourceFactory.forSpan(trace, span);
    return this.getAttribute(valueSource, attributeScope, attributeKey)
        .flatMap(definition -> this.valueResolver.resolve(valueSource, definition));
  }

  @Override
  public Optional<LiteralValue> getTraceValue(StructuredTrace trace, String attributeKey) {
    ValueSource valueSource = ValueSourceFactory.forTrace(trace);
    return this.getAttribute(valueSource, TRACE_SCOPE, attributeKey)
        .flatMap(definition -> this.valueResolver.resolve(valueSource, definition));
  }

  @Override
  public String getTenantId(Event span) {
    return span.getCustomerId();
  }

  @Override
  public RequestContext getRequestContext(Event span) {
    return RequestContext.forTenantId(span.getCustomerId());
  }

  private Optional<AttributeMetadata> getAttribute(
      ValueSource valueSource, String attributeScope, String attributeKey) {
    return this.attributeClient.get(valueSource.requestContext(), attributeScope, attributeKey);
  }
}
