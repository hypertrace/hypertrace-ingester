package org.hypertrace.traceenricher.enrichment.enrichers;

import com.typesafe.config.Config;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.entity.data.service.client.EntityDataServiceClientProvider;
import org.hypertrace.entity.service.client.config.EntityServiceClientConfig;
import org.hypertrace.trace.reader.entities.TraceEntityReader;
import org.hypertrace.trace.reader.entities.TraceEntityClientContext;
import org.hypertrace.trace.reader.entities.TraceEntityReaderFactory;
import org.hypertrace.traceenricher.enrichment.AbstractTraceEnricher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntitySpanEnricher extends AbstractTraceEnricher {
  private static final Logger LOG = LoggerFactory.getLogger(EntitySpanEnricher.class);
  private TraceEntityReader<StructuredTrace, Event> entityReader;

  @Override
  public void enrichEvent(StructuredTrace trace, Event event) {
    try {
      this.entityReader.getAssociatedEntitiesForSpan(trace, event).blockingSubscribe();
    } catch (Throwable t) {
      LOG.error("Failed to enrich entities on span", t);
    }
  }

  @Override
  public void init(Config enricherConfig, EntityDataServiceClientProvider provider) {
    EntityServiceClientConfig esConfig = EntityServiceClientConfig.from(enricherConfig);
    Channel esChannel =
        ManagedChannelBuilder.forAddress(esConfig.getHost(), esConfig.getPort())
            .usePlaintext()
            .build();
    Channel asChannel =
        ManagedChannelBuilder.forAddress(
                enricherConfig.getString("attribute.service.config.host"),
                enricherConfig.getInt("attribute.service.config.port"))
            .usePlaintext()
            .build();
    this.entityReader =
        TraceEntityReaderFactory.build(
            TraceEntityClientContext.usingChannels(esChannel, esChannel, asChannel));
  }
}
