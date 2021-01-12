package org.hypertrace.viewgenerator.generators;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

import io.micrometer.core.instrument.Timer;
import org.apache.avro.generic.GenericRecord;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.Entity;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.EventRef;
import org.hypertrace.core.datamodel.EventRefType;
import org.hypertrace.core.datamodel.MetricValue;
import org.hypertrace.core.datamodel.StructuredTrace;
import org.hypertrace.core.datamodel.shared.DataflowMetricUtils;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.hypertrace.core.viewgenerator.JavaCodeBasedViewGenerator;
import org.hypertrace.traceenricher.enrichedspan.constants.EnrichedSpanConstants;
import org.hypertrace.traceenricher.enrichedspan.constants.v1.CommonAttribute;
import org.hypertrace.viewgenerator.generators.ViewGeneratorState.TraceState;

/**
 * Pre-processing for View Generators, for data that are mostly needed by the the view generators
 */
public abstract class BaseViewGenerator<OUT extends GenericRecord>
    implements JavaCodeBasedViewGenerator<StructuredTrace, OUT> {

  private static final String VIEW_GENERATION_ARRIVAL_TIME = "view.generation.arrival.time";
  private static final Timer viewGeneratorArrivalTimer = PlatformMetricsRegistry
      .registerTimer(DataflowMetricUtils.ARRIVAL_LAG, new HashMap<>());

  static final String EMPTY_STRING = "";

  static double getMetricValue(Event event, String metricName, double defaultValue) {
    if (event.getMetrics() == null || event.getMetrics().getMetricMap().isEmpty()) {
      return defaultValue;
    }

    MetricValue value = event.getMetrics().getMetricMap().get(metricName);
    if (value == null) {
      return defaultValue;
    }
    return value.getValue();
  }

  @Nullable
  static String getTransactionName(StructuredTrace trace) {
    Attributes attributes = trace.getAttributes();
    if (attributes == null || attributes.getAttributeMap() == null) {
      return null;
    }

    AttributeValue attr =
        attributes
            .getAttributeMap()
            .get(EnrichedSpanConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_TRANSACTION_NAME));
    return attr != null ? attr.getValue() : null;
  }

  @Override
  public List<OUT> process(StructuredTrace trace) {
    DataflowMetricUtils.reportArrivalLagAndInsertTimestamp(trace, viewGeneratorArrivalTimer,
        VIEW_GENERATION_ARRIVAL_TIME);
    TraceState traceState = ViewGeneratorState.getTraceState(trace);
    Map<String, Entity> entityMap = Collections.unmodifiableMap(traceState.getEntityMap());
    Map<ByteBuffer, Event> eventMap = Collections.unmodifiableMap(traceState.getEventMap());
    Map<ByteBuffer, List<ByteBuffer>> parentToChildrenEventIds = Collections.unmodifiableMap(traceState.getParentToChildrenEventIds());
    Map<ByteBuffer, ByteBuffer> childToParentEventIds = Collections.unmodifiableMap(traceState.getChildToParentEventIds());

    return generateView(
        trace,
        entityMap, eventMap,
        parentToChildrenEventIds,
        childToParentEventIds);
  }

  abstract List<OUT> generateView(
      StructuredTrace structuredTrace,
      final Map<String, Entity> entityMap,
      final Map<ByteBuffer, Event> eventMap,
      final Map<ByteBuffer, List<ByteBuffer>> parentToChildrenEventIds,
      final Map<ByteBuffer, ByteBuffer> childToParentEventIds);

  protected Map<String, String> getAttributeMap(Attributes attributes) {
    Map<String, String> resultMap = new HashMap<>();
    if (attributes == null || attributes.getAttributeMap() == null) {
      return resultMap;
    }
    for (Map.Entry<String, AttributeValue> entry : attributes.getAttributeMap().entrySet()) {
      resultMap.put(entry.getKey(), entry.getValue().getValue());
    }
    return resultMap;
  }
}
