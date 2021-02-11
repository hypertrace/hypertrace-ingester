package org.hypertrace.trace.reader.attributes;

import static org.hypertrace.trace.reader.attributes.ValueSource.TRACE_SCOPE;

import io.reactivex.rxjava3.core.Single;
import org.hypertrace.core.attribute.service.cachingclient.CachingAttributeClient;
import org.hypertrace.core.attribute.service.projection.AttributeProjectionRegistry;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.LiteralValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;

class DefaultTraceAttributeReader implements TraceAttributeReader<StructuredTrace, Event> {

  private final CachingAttributeClient attributeClient;
  private final ValueResolver valueResolver;
  private final AttributeProjectionRegistry projectionRegistry;

  DefaultTraceAttributeReader(CachingAttributeClient attributeClient) {
    this.attributeClient = attributeClient;
    this.projectionRegistry = new AttributeProjectionRegistry();
    this.valueResolver = ValueResolver.build(this.attributeClient, this.projectionRegistry);
  }

  @Override
  public Single<LiteralValue> getSpanValue(
      StructuredTrace trace, Event span, String attributeScope, String attributeKey) {
    ValueSource<StructuredTrace, Event> valueSource = ValueSourceFactory.forSpan(trace, span);
    return this.getAttribute(valueSource, attributeScope, attributeKey)
        .flatMap(definition -> this.valueResolver.resolve(valueSource, definition));
  }

  @Override
  public Single<LiteralValue> getTraceValue(StructuredTrace trace, String attributeKey) {
    ValueSource<StructuredTrace, Event> valueSource = ValueSourceFactory.forTrace(trace);
    return this.getAttribute(valueSource, TRACE_SCOPE, attributeKey)
        .flatMap(definition -> this.valueResolver.resolve(valueSource, definition));
  }

  @Override
  public String getCustomerId(Event span) {
    return span.getCustomerId();
  }

  private Single<AttributeMetadata> getAttribute(
      ValueSource<StructuredTrace, Event> valueSource, String attributeScope, String attributeKey) {
    return valueSource
        .executionContext()
        .wrapSingle(() -> this.attributeClient.get(attributeScope, attributeKey));
  }
}
