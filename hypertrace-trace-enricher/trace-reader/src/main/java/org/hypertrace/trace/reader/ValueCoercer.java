package org.hypertrace.trace.reader;

import java.util.Optional;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.attribute.service.v1.LiteralValue;

interface ValueCoercer {

  Optional<LiteralValue> toLiteral(String stringValue, AttributeKind attributeKind);

  Optional<LiteralValue> toLiteral(Double doubleValue, AttributeKind attributeKind);
}
