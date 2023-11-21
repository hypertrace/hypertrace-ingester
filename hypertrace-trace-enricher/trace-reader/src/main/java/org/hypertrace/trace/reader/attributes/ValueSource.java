package org.hypertrace.trace.reader.attributes;

import java.util.Optional;
import org.hypertrace.core.attribute.service.v1.AttributeDefinition.SourceField;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.attribute.service.v1.LiteralValue;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface ValueSource {
  Optional<LiteralValue> getAttribute(String key, AttributeKind attributeKind);

  Optional<LiteralValue> getMetric(String key, AttributeKind attributeKind);

  Optional<LiteralValue> getSourceField(SourceField field, AttributeKind attributeKind);

  Optional<ValueSource> sourceForScope(String scope);

  RequestContext requestContext();

  String TRACE_SCOPE = "TRACE";
}
