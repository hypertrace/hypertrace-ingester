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
    Map<String, Entity> entityMap = new HashMap<>();
    Map<ByteBuffer, Event> eventMap = new HashMap<>();
    Map<ByteBuffer, List<ByteBuffer>> parentToChildrenEventIds = new HashMap<>();
    Map<ByteBuffer, ByteBuffer> childToParentEventIds = new HashMap<>();

    for (Entity entity : trace.getEntityList()) {
      entityMap.put(entity.getEntityId(), entity);
    }
    for (Event event : trace.getEventList()) {
      eventMap.put(event.getEventId(), event);
    }

    for (Event event : trace.getEventList()) {
      ByteBuffer childEventId = event.getEventId();
      List<EventRef> eventRefs = eventMap.get(childEventId).getEventRefList();
      if (eventRefs != null) {
        eventRefs.stream()
            .filter(eventRef -> EventRefType.CHILD_OF == eventRef.getRefType())
            .forEach(
                eventRef -> {
                  ByteBuffer parentEventId = eventRef.getEventId();
                  if (!parentToChildrenEventIds.containsKey(parentEventId)) {
                    parentToChildrenEventIds.put(parentEventId, new ArrayList<>());
                  }
                  parentToChildrenEventIds.get(parentEventId).add(childEventId);
                  childToParentEventIds.put(childEventId, parentEventId);
                });
      }
      // expected only 1 childOf relationship
    }

    return generateView(
        trace,
        Collections.unmodifiableMap(entityMap),
        Collections.unmodifiableMap(eventMap),
        Collections.unmodifiableMap(parentToChildrenEventIds),
        Collections.unmodifiableMap(childToParentEventIds));
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
