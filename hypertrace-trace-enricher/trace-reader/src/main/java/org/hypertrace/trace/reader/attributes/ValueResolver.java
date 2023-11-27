package org.hypertrace.trace.reader.attributes;

import java.util.Optional;
import org.hypertrace.core.attribute.service.client.AttributeServiceCachedClient;
import org.hypertrace.core.attribute.service.projection.AttributeProjectionRegistry;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.LiteralValue;

public interface ValueResolver {

  Optional<LiteralValue> resolve(ValueSource valueSource, AttributeMetadata attributeMetadata);

  static ValueResolver build(AttributeServiceCachedClient attributeClient) {
    return new DefaultValueResolver(attributeClient, new AttributeProjectionRegistry());
  }
}
