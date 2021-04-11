package org.hypertrace.trace.reader.entities;

import java.time.Duration;
import org.hypertrace.core.attribute.service.cachingclient.CachingAttributeClient;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.entity.data.service.rxclient.EntityDataClient;
import org.hypertrace.entity.type.service.rxclient.EntityTypeClient;
import org.hypertrace.trace.reader.attributes.TraceAttributeReaderFactory;

public class TraceEntityReaderBuilder {
  private final EntityTypeClient entityTypeClient;
  private final EntityDataClient entityDataClient;
  private final CachingAttributeClient attributeClient;
  private Duration entityWriteThrottleDuration = Duration.ofSeconds(15);

  public TraceEntityReaderBuilder(
      EntityTypeClient entityTypeClient,
      EntityDataClient entityDataClient,
      CachingAttributeClient attributeClient) {
    this.entityTypeClient = entityTypeClient;
    this.entityDataClient = entityDataClient;
    this.attributeClient = attributeClient;
  }

  public TraceEntityReaderBuilder withEntityWriteThrottleDuration(Duration duration) {
    this.entityWriteThrottleDuration = duration;
    return this;
  }

  public TraceEntityReader<StructuredTrace, Event> build() {
    return new DefaultTraceEntityReader<>(
        this.entityTypeClient,
        this.entityDataClient,
        this.attributeClient,
        TraceAttributeReaderFactory.build(this.attributeClient),
        entityWriteThrottleDuration);
  }
}
