package org.hypertrace.trace.reader;

import io.reactivex.rxjava3.core.Single;
import org.hypertrace.core.attribute.service.cachingclient.CachingAttributeClient;
import org.hypertrace.core.attribute.service.v1.LiteralValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;

public interface TraceReader {
  Single<LiteralValue> getSpanValue(
      StructuredTrace trace, Event span, String attributeScope, String attributeKey);

  Single<LiteralValue> getTraceValue(StructuredTrace trace, String attributeKey);

  static TraceReader build(CachingAttributeClient attributeClient) {
    return new DefaultTraceReader(attributeClient);
  }
}
