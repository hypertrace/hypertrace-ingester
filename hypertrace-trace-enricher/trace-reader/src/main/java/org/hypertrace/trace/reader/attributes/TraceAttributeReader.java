package org.hypertrace.trace.reader.attributes;

import io.reactivex.rxjava3.core.Single;
import org.apache.avro.generic.GenericRecord;
import org.hypertrace.core.attribute.service.v1.LiteralValue;

public interface TraceAttributeReader<T extends GenericRecord, S extends GenericRecord> {
  Single<LiteralValue> getSpanValue(T trace, S span, String attributeScope, String attributeKey);

  Single<LiteralValue> getTraceValue(T trace, String attributeKey);

  String getTenantId(S span);
}
