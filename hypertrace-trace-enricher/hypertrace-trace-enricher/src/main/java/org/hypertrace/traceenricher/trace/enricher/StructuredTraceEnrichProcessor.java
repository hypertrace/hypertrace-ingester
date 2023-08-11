package org.hypertrace.traceenricher.trace.enricher;

import static org.hypertrace.traceenricher.trace.enricher.StructuredTraceEnricherConstants.CACHE_LOADER_THREAD_POOL_SIZE;
import static org.hypertrace.traceenricher.trace.enricher.StructuredTraceEnricherConstants.ENRICHER_CLIENTS_CONFIG_KEY;
import static org.hypertrace.traceenricher.trace.enricher.StructuredTraceEnricherConstants.ENRICHER_CONFIG_TEMPLATE;
import static org.hypertrace.traceenricher.trace.enricher.StructuredTraceEnricherConstants.ENRICHER_NAMES_CONFIG_KEY;
import static org.hypertrace.traceenricher.trace.enricher.StructuredTraceEnricherConstants.STRUCTURED_TRACES_ENRICHMENT_JOB_CONFIG_KEY;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.typesafe.config.Config;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.stream.Collectors;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.hypertrace.core.spannormalizer.TraceIdentity;
import org.hypertrace.semantic.convention.utils.span.SpanStore;
import org.hypertrace.traceenricher.enrichment.EnrichmentProcessor;
import org.hypertrace.traceenricher.enrichment.EnrichmentRegistry;
import org.hypertrace.traceenricher.enrichment.clients.DefaultClientRegistry;

public class StructuredTraceEnrichProcessor
    implements Transformer<TraceIdentity, StructuredTrace, KeyValue<String, StructuredTrace>> {

  private static EnrichmentProcessor processor = null;
  private final GrpcChannelRegistry grpcChannelRegistry;
  private DefaultClientRegistry clientRegistry;
  private SpanStore spanStore;

  public StructuredTraceEnrichProcessor(GrpcChannelRegistry grpcChannelRegistry) {
    this.grpcChannelRegistry = grpcChannelRegistry;
  }

  @Override
  public void init(ProcessorContext context) {
    if (processor == null) {
      synchronized (StructuredTraceEnrichProcessor.class) {
        if (processor == null) {
          Config jobConfig =
              (Config) context.appConfigs().get(STRUCTURED_TRACES_ENRICHMENT_JOB_CONFIG_KEY);
          Map<String, Config> enricherConfigs = getEnricherConfigs(jobConfig);
          EnrichmentRegistry enrichmentRegistry = new EnrichmentRegistry();
          enrichmentRegistry.registerEnrichers(enricherConfigs);
          Config clientsConfig = this.getClientsConfig(jobConfig);
          clientRegistry =
              new DefaultClientRegistry(
                  clientsConfig,
                  grpcChannelRegistry,
                  Executors.newFixedThreadPool(
                      getCacheLoaderExecutorThreadsPoolSize(clientsConfig),
                      this.buildThreadFactory()));
          processor =
              new EnrichmentProcessor(
                  enrichmentRegistry.getOrderedRegisteredEnrichers(), clientRegistry);

          spanStore = new SpanStore(jobConfig);
        }
      }
    }
  }

  @Override
  public KeyValue<String, StructuredTrace> transform(TraceIdentity key, StructuredTrace value) {
    List<Event> updatedEventList =
        value.getEventList().stream()
            .map(event -> spanStore.retrieveFromSpanStoreAndFillSpan(event))
            .collect(Collectors.toUnmodifiableList());
    StructuredTrace updatedValue =
        StructuredTrace.newBuilder(value).setEventList(updatedEventList).build();
    processor.process(updatedValue);
    return new KeyValue<>(null, updatedValue);
  }

  @Override
  public void close() {
    // clientRegistry.shutdown(); TODO restore cleanup once shared static instance removed
  }

  private Map<String, Config> getEnricherConfigs(Config jobConfig) {
    List<String> enrichers = jobConfig.getStringList(ENRICHER_NAMES_CONFIG_KEY);
    Map<String, Config> enricherConfigs = new LinkedHashMap<>();
    for (String enricher : enrichers) {
      Config enricherConfig = jobConfig.getConfig(getEnricherConfigPath(enricher));
      enricherConfigs.put(enricher, enricherConfig);
    }
    return enricherConfigs;
  }

  private Config getClientsConfig(Config jobConfig) {
    return jobConfig.getConfig(ENRICHER_CLIENTS_CONFIG_KEY);
  }

  private int getCacheLoaderExecutorThreadsPoolSize(Config clientsConfig) {
    return clientsConfig.hasPath(CACHE_LOADER_THREAD_POOL_SIZE)
        ? clientsConfig.getInt(CACHE_LOADER_THREAD_POOL_SIZE)
        : 3;
  }

  private String getEnricherConfigPath(String enricher) {
    return String.format(ENRICHER_CONFIG_TEMPLATE, enricher);
  }

  private ThreadFactory buildThreadFactory() {
    return new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat("async-cache-loader-%d")
        .build();
  }
}
