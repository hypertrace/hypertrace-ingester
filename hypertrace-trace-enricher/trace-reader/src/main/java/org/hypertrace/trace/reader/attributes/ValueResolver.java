package org.hypertrace.trace.reader.attributes;

import io.reactivex.rxjava3.core.Single;
import org.hypertrace.core.attribute.service.cachingclient.CachingAttributeClient;
import org.hypertrace.core.attribute.service.projection.AttributeProjectionRegistry;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.LiteralValue;
import org.hypertrace.trace.provider.AttributeProvider;

import java.util.Optional;

public interface ValueResolver {

  Optional<LiteralValue> resolve(ValueSource valueSource, AttributeMetadata attributeMetadata);

  static ValueResolver build(AttributeProvider attributeProvider) {
    return new DefaultValueResolver(attributeProvider, new AttributeProjectionRegistry());
  }
}
