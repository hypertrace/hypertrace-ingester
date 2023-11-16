package org.hypertrace.trace.reader.attributes;

import java.util.Optional;
import org.apache.avro.generic.GenericRecord;
import org.hypertrace.core.attribute.service.v1.LiteralValue;

public interface TraceAttributeReader<T extends GenericRecord, S extends GenericRecord> {
  Optional<LiteralValue> getSpanValue(T trace, S span, String attributeScope, String attributeKey);

  Optional<LiteralValue> getTraceValue(T trace, String attributeKey);

  String getTenantId(S span);
}
