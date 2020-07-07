package org.hypertrace.traceenricher.trace.enricher;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.entity.data.service.client.DefaultEdsClientProvider;
import org.hypertrace.traceenricher.enrichment.EnrichmentProcessor;
import org.hypertrace.traceenricher.enrichment.EnrichmentRegistry;

/**
 * The Processor function to enrich a StructuredTrace.
 */
public class StructuredTraceEnrichProcessor extends
    ProcessFunction<StructuredTrace, StructuredTrace> {

  private static EnrichmentProcessor processor = null;

  public StructuredTraceEnrichProcessor(EnrichmentRegistry registry) {
    if (processor == null) {
      synchronized (StructuredTraceEnrichProcessor.class) {
        if (processor == null) {
          processor = new EnrichmentProcessor(registry.getOrderedRegisteredEnrichers(),
              new DefaultEdsClientProvider());
        }
      }
    }
  }

  @Override
  public void processElement(StructuredTrace value, Context ctx, Collector<StructuredTrace> out) {
    processor.process(value);
    out.collect(value);
  }
}
