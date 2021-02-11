package org.hypertrace.trace.reader.attributes;

import java.util.Optional;
import org.apache.avro.generic.GenericRecord;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.attribute.service.v1.LiteralValue;
import org.hypertrace.core.grpcutils.client.rx.GrpcRxExecutionContext;

public interface ValueSource<T extends GenericRecord, S extends GenericRecord> {
  Optional<LiteralValue> getAttribute(String key, AttributeKind attributeKind);

  Optional<LiteralValue> getMetric(String key, AttributeKind attributeKind);

  Optional<ValueSource<T, S>> sourceForScope(String scope);

  GrpcRxExecutionContext executionContext();

  String TRACE_SCOPE = "TRACE";
}
