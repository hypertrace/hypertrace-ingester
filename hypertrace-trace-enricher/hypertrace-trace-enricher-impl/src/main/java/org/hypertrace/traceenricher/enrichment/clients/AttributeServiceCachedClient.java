package org.hypertrace.traceenricher.enrichment.clients;

import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.trace.provider.AttributeProvider;

import java.util.List;
import java.util.Optional;

public class AttributeServiceCachedClient implements AttributeProvider {

  Optional<AttributeMetadata> get(String attributeScope, String attributeKey) {

  }

  Optional<AttributeMetadata> getById(String attributeId) {

  }

  Optional<List<AttributeMetadata>> getAllInScope(String scope) {

  }
}
