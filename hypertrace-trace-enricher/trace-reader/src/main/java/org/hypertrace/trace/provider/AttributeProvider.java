package org.hypertrace.trace.provider;

import java.util.List;
import java.util.Optional;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;

public interface AttributeProvider {
  Optional<AttributeMetadata> get(String tenantId, String attributeScope, String attributeKey);

  Optional<AttributeMetadata> getById(String tenantId, String attributeId);

  Optional<List<AttributeMetadata>> getAllInScope(String tenantId, String scope);
}
