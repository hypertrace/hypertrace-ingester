package org.hypertrace.traceenricher.trace.enricher;

import static org.hypertrace.traceenricher.trace.enricher.StructuredTraceEnricherConstants.ENRICHER_CONFIG_TEMPLATE;
import static org.hypertrace.traceenricher.trace.enricher.StructuredTraceEnricherConstants.ENRICHER_NAMES_CONFIG_KEY;
import static org.hypertrace.traceenricher.trace.enricher.StructuredTraceEnricherConstants.STRUCTURED_TRACES_ENRICHMENT_JOB_CONFIG_KEY;

import com.typesafe.config.Config;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.DataflowMetricUtils;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.hypertrace.entity.data.service.client.DefaultEdsClientProvider;
import org.hypertrace.traceenricher.enrichment.EnrichmentProcessor;
import org.hypertrace.traceenricher.enrichment.EnrichmentRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StructuredTraceEnrichProcessor implements
    Transformer<String, StructuredTrace, KeyValue<String, StructuredTrace>> {

  private static final Logger logger = LoggerFactory.getLogger(StructuredTraceEnrichProcessor.class);
  private static EnrichmentProcessor processor = null;

  private static final String ENRICHED_TRACES_COUNTER = "hypertrace.enriched.traces";
  private static final ConcurrentMap<String, Counter> tenantToEnrichedTraceCounter = new ConcurrentHashMap<>();

  private static final String ENRICHED_TRACES_TIMER = "hypertrace.trace.enrichment.latency";
  private static final ConcurrentMap<String, Timer> tenantToEnrichmentTraceTimer = new ConcurrentHashMap<>();


  @Override
  public void init(ProcessorContext context) {
    if (processor == null) {
      synchronized (StructuredTraceEnrichProcessor.class) {
        if (processor == null) {
          Map<String, Config> enricherConfigs = getEnricherConfigs(context.appConfigs());
          EnrichmentRegistry enrichmentRegistry = new EnrichmentRegistry();
          enrichmentRegistry.registerEnrichers(enricherConfigs);
          processor = new EnrichmentProcessor(enrichmentRegistry.getOrderedRegisteredEnrichers(),
              new DefaultEdsClientProvider());
        }
      }
    }
  }

  @Override
  public KeyValue<String, StructuredTrace> transform(String key, StructuredTrace value) {
    Instant start = Instant.now();
    processor.process(value);
    long timeElapsed = Duration.between(start, Instant.now()).toMillis();

    tenantToEnrichedTraceCounter.computeIfAbsent(value.getCustomerId(),
        k -> PlatformMetricsRegistry.registerCounter(ENRICHED_TRACES_COUNTER, Map.of("tenantId", k)))
        .increment();
    tenantToEnrichmentTraceTimer.computeIfAbsent(value.getCustomerId(),
        k -> PlatformMetricsRegistry.registerTimer(ENRICHED_TRACES_TIMER, Map.of("tenantId", k)))
        .record(timeElapsed, TimeUnit.MILLISECONDS);
    return new KeyValue<>(null, value);
  }

  @Override
  public void close() {
  }

  private Map<String, Config> getEnricherConfigs(Map<String, Object> properties) {
    Config jobConfig = (Config) properties.get(STRUCTURED_TRACES_ENRICHMENT_JOB_CONFIG_KEY);
    List<String> enrichers = jobConfig.getStringList(ENRICHER_NAMES_CONFIG_KEY);
    Map<String, Config> enricherConfigs = new LinkedHashMap<>();
    for (String enricher : enrichers) {
      Config enricherConfig = jobConfig.getConfig(getEnricherConfigPath(enricher));
      enricherConfigs.put(enricher, enricherConfig);
    }
    return enricherConfigs;
  }

  private String getEnricherConfigPath(String enricher) {
    return String.format(ENRICHER_CONFIG_TEMPLATE, enricher);
  }
}
