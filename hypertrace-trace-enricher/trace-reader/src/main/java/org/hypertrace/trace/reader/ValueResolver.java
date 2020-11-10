package org.hypertrace.trace.reader;

import io.reactivex.rxjava3.core.Single;
import org.hypertrace.core.attribute.service.cachingclient.CachingAttributeClient;
import org.hypertrace.core.attribute.service.projection.AttributeProjectionRegistry;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.LiteralValue;

interface ValueResolver {

  Single<LiteralValue> resolve(ValueSource valueSource, AttributeMetadata attributeMetadata);

  static ValueResolver build(
      CachingAttributeClient attributeClient, AttributeProjectionRegistry projectionRegistry) {
    return new DefaultValueResolver(attributeClient, projectionRegistry);
  }
}
