package org.hypertrace.trace.accessor.entities;

import static java.util.Collections.emptySet;

import java.time.Duration;
import java.util.Set;

import org.hypertrace.entity.data.service.rxclient.EntityDataClient;
import org.hypertrace.entity.type.service.rxclient.EntityTypeClient;
import org.hypertrace.trace.provider.AttributeProvider;
import org.hypertrace.trace.reader.attributes.TraceAttributeReaderFactory;

public class TraceEntityAccessorBuilder {
  private final EntityTypeClient entityTypeClient;
  private final EntityDataClient entityDataClient;
  private final AttributeProvider attributeProvider;
  private Duration entityWriteThrottleDuration = Duration.ofSeconds(15);
  private Set<String> excludedEntityTypes = emptySet();

  public TraceEntityAccessorBuilder(
      EntityTypeClient entityTypeClient,
      EntityDataClient entityDataClient,
      AttributeProvider attributeProvider) {
    this.entityTypeClient = entityTypeClient;
    this.entityDataClient = entityDataClient;
    this.attributeProvider = attributeProvider;
  }

  public TraceEntityAccessorBuilder withEntityWriteThrottleDuration(Duration duration) {
    this.entityWriteThrottleDuration = duration;
    return this;
  }

  public TraceEntityAccessorBuilder withExcludeEntityTypes(Set<String> excludedEntityTypes) {
    this.excludedEntityTypes = excludedEntityTypes;
    return this;
  }

  public TraceEntityAccessor build() {
    return new DefaultTraceEntityAccessor(
        this.entityTypeClient,
        this.entityDataClient,
        this.attributeProvider,
        TraceAttributeReaderFactory.build(this.attributeProvider),
        entityWriteThrottleDuration,
        excludedEntityTypes);
  }
}
