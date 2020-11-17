package org.hypertrace.traceenricher.enrichment.enrichers;

import java.util.Map;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
import org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.utils.EnrichedSpanUtils;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.CommonAttribute;
import org.hypertrace.traceenricher.enrichment.AbstractTraceEnricher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Finds the transaction name from each trace data.
 * Go through the event list and find the one with the earliest start time and use the api name
 * as transaction name
 * If there's broken span / exit span as the start of the trace,
 * then it will be categorized as "unknown" transaction
 */
public class TransactionNameEnricher extends AbstractTraceEnricher {

  private static final Logger LOGGER = LoggerFactory.getLogger(TransactionNameEnricher.class);

  @Override
  public void enrichTrace(StructuredTrace trace) {
    /* possible if the trace is not created from Builder */
    if (trace.getAttributes() == null) {
      return;
    }

    if (trace.getAttributes().getAttributeMap() == null) {
      return;
    }

    Event earliestEvent = getEarliestEvent(trace);

    // assumes unknown if there's no Api Name set for this event
    if (EnrichedSpanUtils.isEntrySpan(earliestEvent)) {
      LOGGER.debug("Found earliest Event in this trace. It is {}", earliestEvent);
      String apiName = EnrichedSpanUtils.getApiName(earliestEvent);

      if (apiName != null) {
        Map<String, AttributeValue> attributes = trace.getAttributes().getAttributeMap();
        attributes.put(EnrichedSpanConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_TRANSACTION_NAME),
            AttributeValueCreator.create(apiName));
      }
    }
  }
}
