package org.hypertrace.traceenricher.enrichment.enrichers;

import com.typesafe.config.Config;
import java.util.HashMap;
import java.util.Map;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.MetricValue;
import org.hypertrace.core.datamodel.Metrics;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
import org.hypertrace.core.datamodel.shared.trace.MetricValueCreator;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.span.constants.v1.Error;
import org.hypertrace.core.span.constants.v1.OTSpanTag;
import org.hypertrace.entity.data.service.client.EntityDataServiceClientProvider;
import org.hypertrace.semantic.convention.utils.error.ErrorSemanticConventionUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.utils.EnrichedSpanUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.ApiStatus;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.CommonAttribute;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.ErrorMetrics;
import org.hypertrace.traceenricher.enrichment.AbstractTraceEnricher;
import org.hypertrace.traceenricher.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Trace enricher which finds if there were any errors and exceptions in the received spans and
 * enriches the spans and traces with more details around errors so that we could later derive
 * metrics.
 * <p>
 * Please note an error in a span/event is different from an error at overall trace.
 * Span errors are reported by the agents and we derive if the trace/API is an error
 * based on certain criteria, which is present in this enricher.
 * <p>
 * Trace is considered to have an error only if there is an error in the head span.
 */
public class ErrorsAndExceptionsEnricher extends AbstractTraceEnricher {

  private static final Logger LOG = LoggerFactory.getLogger(ErrorsAndExceptionsEnricher.class);

  @Override
  public void enrichEvent(StructuredTrace trace, Event event) {
    if (event.getAttributes() == null || event.getAttributes().getAttributeMap() == null) {
      return;
    }

    enrichErrorDetails(event);
    enrichExceptionDetails(event);
  }

  private void enrichExceptionDetails(Event event) {
    // Figure out if event has any exceptions in it.
    boolean hasException = ErrorSemanticConventionUtils.checkForErrorStackTrace(event);

    if (hasException) {
      if (event.getMetrics() == null) {
        event.setMetrics(Metrics.newBuilder().setMetricMap(new HashMap<>()).build());
      }

      event.getMetrics().getMetricMap().put(
          EnrichedSpanConstants.getValue(ErrorMetrics.ERROR_METRICS_EXCEPTION_COUNT),
          MetricValue.newBuilder().setValue(1.0d).build());
    }
  }

  private void enrichErrorDetails(Event event) {
    // Figure out if there are any errors in the event.
    boolean hasError = ErrorSemanticConventionUtils.checkForError(event) ||
        Constants.getEnrichedSpanConstant(ApiStatus.API_STATUS_FAIL)
            .equals(EnrichedSpanUtils.getStatus(event));

    if (hasError) {
      if (event.getMetrics() == null) {
        event.setMetrics(Metrics.newBuilder().setMetricMap(new HashMap<>()).build());
      }

      // TODO: Currently we only track the error count but we might want to enrich with additional
      //  details like kind of error, error message, etc in future.
      event.getMetrics().getMetricMap().put(
          EnrichedSpanConstants.getValue(ErrorMetrics.ERROR_METRICS_ERROR_COUNT),
          MetricValue.newBuilder().setValue(1.0d).build());
    }
  }

  @Override
  public void enrichTrace(StructuredTrace trace) {
    // TODO: There could be other cases where the client which is initiating this transaction
    //  has errored out but the entry span in transaction might be fine (server responded but
    //  client couldn't process it). Those cases should be handled in future.

    // Find the earliest Event from this trace and check if that's an ENTRY type.
    Event earliestEvent = getEarliestEvent(trace);

    if (EnrichedSpanUtils.isEntrySpan(earliestEvent)) {
      LOG.debug("Found earliest Event in this trace. It is {}", earliestEvent);
      Metrics metrics = earliestEvent.getMetrics();
      if (metrics != null && metrics.getMetricMap() != null &&
          metrics.getMetricMap().containsKey(EnrichedSpanConstants.getValue(ErrorMetrics.ERROR_METRICS_ERROR_COUNT))) {
        trace.getAttributes().getAttributeMap()
            .put(EnrichedSpanConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_TRANSACTION_HAS_ERROR),
                AttributeValueCreator.create(true));
      }
    }

    // Count the no. of errors and exceptions in this trace overall. These need not have caused
    // the trace to error out but it's a good metric to track anyways.
    // Trace is considered to have an error if there is an error in the entry span only.
    double exceptionCount = 0.0d;
    double errorCount = 0.0d;
    for (Event event : trace.getEventList()) {
      Map<String, MetricValue> metricMap = event.getMetrics().getMetricMap();
      if (metricMap != null && metricMap.containsKey(
          EnrichedSpanConstants.getValue(ErrorMetrics.ERROR_METRICS_ERROR_COUNT))) {
        errorCount += metricMap.get(EnrichedSpanConstants.getValue(ErrorMetrics.ERROR_METRICS_ERROR_COUNT)).getValue();
      }

      if (metricMap != null && metricMap.containsKey(
          EnrichedSpanConstants.getValue(ErrorMetrics.ERROR_METRICS_EXCEPTION_COUNT))) {
        exceptionCount += metricMap.get(
            EnrichedSpanConstants.getValue(ErrorMetrics.ERROR_METRICS_EXCEPTION_COUNT)).getValue();
      }
    }

    if (exceptionCount > 0.0d) {
      trace.getMetrics().getMetricMap()
          .put(EnrichedSpanConstants.getValue(ErrorMetrics.ERROR_METRICS_TOTAL_SPANS_WITH_EXCEPTIONS),
              MetricValueCreator.create(exceptionCount));
    }

    if (errorCount > 0.0d) {
      trace.getMetrics().getMetricMap()
          .put(EnrichedSpanConstants.getValue(ErrorMetrics.ERROR_METRICS_TOTAL_SPANS_WITH_ERRORS),
              MetricValueCreator.create(errorCount));
    }
  }
}
