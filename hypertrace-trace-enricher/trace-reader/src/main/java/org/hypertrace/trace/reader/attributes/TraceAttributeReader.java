package org.hypertrace.trace.reader.attributes;

import io.reactivex.rxjava3.core.Single;
import org.hypertrace.core.attribute.service.cachingclient.CachingAttributeClient;
import org.hypertrace.core.attribute.service.v1.LiteralValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;

public interface TraceAttributeReader {
  Single<LiteralValue> getSpanValue(
      StructuredTrace trace, Event span, String attributeScope, String attributeKey);

  Single<LiteralValue> getTraceValue(StructuredTrace trace, String attributeKey);

  static TraceAttributeReader build(CachingAttributeClient attributeClient) {
    return new DefaultTraceAttributeReader(attributeClient);
  }
}
