package org.hypertrace.core.spannormalizer.util;

import static org.hypertrace.core.datamodel.shared.AvroBuilderCache.fastNewBuilder;
import static org.hypertrace.core.spannormalizer.jaeger.ServiceNamer.OLD_JAEGER_SERVICENAME_KEY;

import com.google.protobuf.ProtocolStringList;
import com.google.protobuf.util.Timestamps;
import io.jaegertracing.api_v2.JaegerSpanInternalModel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.EventRef;
import org.hypertrace.core.datamodel.EventRefType;
import org.hypertrace.core.datamodel.MetricValue;
import org.hypertrace.core.datamodel.Metrics;
import org.hypertrace.core.datamodel.eventfields.jaeger.JaegerFields;
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.span.constants.v1.JaegerAttribute;
import org.hypertrace.core.spannormalizer.jaeger.ServiceNamer;

public class EventBuilder {
  public static Event buildEvent(
      String tenantId,
      JaegerSpanInternalModel.Span jaegerSpan,
      ServiceNamer serviceNamer,
      Optional<String> tenantIdKey) {
    Event.Builder eventBuilder = fastNewBuilder(Event.Builder.class);
    eventBuilder.setCustomerId(tenantId);
    eventBuilder.setEventId(jaegerSpan.getSpanId().asReadOnlyByteBuffer());
    eventBuilder.setEventName(jaegerSpan.getOperationName());

    // time related stuff
    long startTimeMillis = Timestamps.toMillis(jaegerSpan.getStartTime());
    eventBuilder.setStartTimeMillis(startTimeMillis);
    long endTimeMillis =
        Timestamps.toMillis(Timestamps.add(jaegerSpan.getStartTime(), jaegerSpan.getDuration()));
    eventBuilder.setEndTimeMillis(endTimeMillis);

    // SPAN REFS
    List<JaegerSpanInternalModel.SpanRef> referencesList = jaegerSpan.getReferencesList();
    if (referencesList.size() > 0) {
      eventBuilder.setEventRefList(new ArrayList<>());
      // Convert the reflist to a set to remove duplicate references. This has been observed in the
      // field.
      Set<JaegerSpanInternalModel.SpanRef> referencesSet = new HashSet<>(referencesList);
      for (JaegerSpanInternalModel.SpanRef spanRef : referencesSet) {
        EventRef.Builder builder = fastNewBuilder(EventRef.Builder.class);
        builder.setTraceId(spanRef.getTraceId().asReadOnlyByteBuffer());
        builder.setEventId(spanRef.getSpanId().asReadOnlyByteBuffer());
        builder.setRefType(EventRefType.valueOf(spanRef.getRefType().toString()));
        eventBuilder.getEventRefList().add(builder.build());
      }
    }

    // span attributes to event attributes
    Map<String, AttributeValue> attributeFieldMap = new HashMap<>();
    eventBuilder.setAttributesBuilder(
        fastNewBuilder(Attributes.Builder.class).setAttributeMap(attributeFieldMap));

    List<JaegerSpanInternalModel.KeyValue> tagsList = jaegerSpan.getTagsList();
    // Stop populating first class fields for - grpc, rpc, http, and sql.
    // see more details:
    // https://github.com/hypertrace/hypertrace/issues/244
    // https://github.com/hypertrace/hypertrace/issues/245
    for (JaegerSpanInternalModel.KeyValue keyValue : tagsList) {
      // Convert all attributes to lower case so that we don't have to
      // deal with the case sensitivity across different layers in the
      // platform.
      String key = keyValue.getKey().toLowerCase();
      // Do not add the tenant id to the tags.
      if (tenantIdKey.isPresent() && key.equals(tenantIdKey.get())) {
        continue;
      }
      attributeFieldMap.put(key, JaegerHTTagsConverter.createFromJaegerKeyValue(keyValue));
    }

    // Jaeger Fields - flags, warnings, logs, jaeger service name in the Process
    JaegerFields.Builder jaegerFieldsBuilder = eventBuilder.getJaegerFieldsBuilder();
    // FLAGS
    jaegerFieldsBuilder.setFlags(jaegerSpan.getFlags());

    // WARNINGS
    ProtocolStringList warningsList = jaegerSpan.getWarningsList();
    if (warningsList.size() > 0) {
      jaegerFieldsBuilder.setWarnings(warningsList);
    }

    serviceNamer
        .findServiceName(jaegerSpan, attributeFieldMap)
        .ifPresent(
            serviceName -> {
              eventBuilder.setServiceName(serviceName);
              // in case `jaeger.servicename` is present in the map, remove it
              attributeFieldMap.remove(OLD_JAEGER_SERVICENAME_KEY);
              attributeFieldMap.put(
                  RawSpanConstants.getValue(JaegerAttribute.JAEGER_ATTRIBUTE_SERVICE_NAME),
                  AttributeValueCreator.create(serviceName));
            });

    // EVENT METRICS
    Map<String, MetricValue> metricMap = new HashMap<>();
    MetricValue durationMetric =
        fastNewBuilder(MetricValue.Builder.class)
            .setValue((double) (endTimeMillis - startTimeMillis))
            .build();
    metricMap.put("Duration", durationMetric);

    eventBuilder.setMetrics(fastNewBuilder(Metrics.Builder.class).setMetricMap(metricMap).build());

    return eventBuilder.build();
  }
}
