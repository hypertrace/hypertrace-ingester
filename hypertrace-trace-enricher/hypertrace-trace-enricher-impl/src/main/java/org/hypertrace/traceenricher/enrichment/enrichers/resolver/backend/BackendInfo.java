package org.hypertrace.traceenricher.enrichment.enrichers.resolver.backend;

import java.util.Map;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.entity.data.service.v1.Entity;

public class BackendInfo {
  Entity entity;
  Map<String, AttributeValue> attributes;

  public BackendInfo(Entity entity, Map<String, AttributeValue> attributes) {
    this.entity = entity;
    this.attributes = attributes;
  }

  public Entity getEntity() {
    return entity;
  }

  public Map<String, AttributeValue> getAttributes() {
    return attributes;
  }

  @Override
  public String toString() {
    return "BackendInfo{" + "entity=" + entity + ", attributes=" + attributes + '}';
  }
}
