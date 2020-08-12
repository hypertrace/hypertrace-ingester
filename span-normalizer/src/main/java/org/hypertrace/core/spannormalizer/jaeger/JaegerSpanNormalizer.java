package org.hypertrace.core.spannormalizer.jaeger;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.ProtocolStringList;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.Timestamps;
import com.typesafe.config.Config;
import io.jaegertracing.api_v2.JaegerSpanInternalModel;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.KeyValue;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.Log;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.Span;
import io.micrometer.core.instrument.Timer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.EventRef;
import org.hypertrace.core.datamodel.EventRefType;
import org.hypertrace.core.datamodel.MetricValue;
import org.hypertrace.core.datamodel.Metrics;
import org.hypertrace.core.datamodel.RawSpan;
import org.hypertrace.core.datamodel.RawSpan.Builder;
import org.hypertrace.core.datamodel.eventfields.jaeger.JaegerFields;
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.hypertrace.core.span.constants.RawSpanConstants;
import org.hypertrace.core.span.constants.v1.JaegerAttribute;
import org.hypertrace.core.spannormalizer.fieldgenerators.FieldsGenerator;
import org.hypertrace.core.spannormalizer.jaeger.tenant.DefaultTenantIdProvider;
import org.hypertrace.core.spannormalizer.jaeger.tenant.JaegerKeyBasedTenantIdProvider;
import org.hypertrace.core.spannormalizer.jaeger.tenant.TenantIdProvider;
import org.hypertrace.core.spannormalizer.processor.SpanNormalizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JaegerSpanNormalizer implements SpanNormalizer<Span, RawSpan> {
  private static final Logger LOG = LoggerFactory.getLogger(JaegerSpanNormalizer.class);

  /** Config for providing the tag key in which the tenant id will be given in the span. */
  private static final String TENANT_ID_TAG_KEY_CONFIG = "processor.tenantIdTagKey";

  /**
   * The config to provide a default static tenant id, in case the {@link #TENANT_ID_TAG_KEY_CONFIG}
   * is not given and tenant id isn't driven by span tags. These two configs are mutually exclusive.
   */
  private static final String DEFAULT_TENANT_ID_CONFIG = "processor.defaultTenantId";

  private static final String SPAN_NORMALIZATION_TIME_METRIC = "span.normalization.time";

  private static JaegerSpanNormalizer INSTANCE;
  private final FieldsGenerator fieldsGenerator;
  private final TenantIdProvider tenantIdProvider;
  private final ConcurrentMap<String, Timer> tenantToSpanConversionTimer = new ConcurrentHashMap<>();

  public static JaegerSpanNormalizer get(Config config) {
    if (INSTANCE == null) {
      synchronized (JaegerSpanNormalizer.class) {
        if (INSTANCE == null) {
          INSTANCE = new JaegerSpanNormalizer(config);
        }
      }
    }
    return INSTANCE;
  }

  public JaegerSpanNormalizer(Config config) {
    this.fieldsGenerator = new FieldsGenerator();

    // These two configs are mutually exclusive to fail if both of them exist.
    if (config.hasPath(TENANT_ID_TAG_KEY_CONFIG) && config.hasPath(DEFAULT_TENANT_ID_CONFIG)) {
      throw new RuntimeException(
          "Both "
              + TENANT_ID_TAG_KEY_CONFIG
              + " and "
              + DEFAULT_TENANT_ID_CONFIG
              + " configs shouldn't exist at same time.");
    }

    // Tag key in which the tenant id is received in the jaeger span.
    String tenantIdTagKey =
        config.hasPath(TENANT_ID_TAG_KEY_CONFIG)
            ? config.getString(TENANT_ID_TAG_KEY_CONFIG)
            : null;
    // Default static tenant id value to be used when tenant id isn't coming in the spans.
    String defaultTenantIdValue =
        config.hasPath(DEFAULT_TENANT_ID_CONFIG)
            ? config.getString(DEFAULT_TENANT_ID_CONFIG)
            : null;

    // If both the configs are null, the processor can't work so fail.
    if (tenantIdTagKey == null && defaultTenantIdValue == null) {
      throw new RuntimeException(
          "Both "
              + TENANT_ID_TAG_KEY_CONFIG
              + " and "
              + DEFAULT_TENANT_ID_CONFIG
              + " configs can't be null.");
    }

    if (tenantIdTagKey != null) {
      this.tenantIdProvider = new JaegerKeyBasedTenantIdProvider(tenantIdTagKey);
    } else {
      this.tenantIdProvider = new DefaultTenantIdProvider(defaultTenantIdValue);
    }
  }

  @Override
  @Nullable
  public RawSpan convert(Span jaegerSpan) throws Exception {
    Map<String, KeyValue> tags =
        jaegerSpan.getTagsList().stream()
            .collect(Collectors.toMap(t -> t.getKey().toLowerCase(), t -> t, (v1, v2) -> v2));

    Optional<String> tenantId = this.tenantIdProvider.getTenantId(tags);

    if (tenantId.isEmpty()) {
      tenantIdProvider.logWarning(LOG, jaegerSpan);
      return null;
    }

    // Record the time taken for converting the span, along with the tenant id tag.
    return tenantToSpanConversionTimer
        .computeIfAbsent(
            tenantId.get(),
            tenant ->
                PlatformMetricsRegistry.registerTimer(
                    SPAN_NORMALIZATION_TIME_METRIC, Map.of("tenantid", tenant), true))
        .recordCallable(
            () -> {
              Builder rawSpanBuilder = RawSpan.newBuilder();
              rawSpanBuilder.setCustomerId(tenantId.get());
              rawSpanBuilder.setTraceId(jaegerSpan.getTraceId().asReadOnlyByteBuffer());
              // Build Event
              Event event =
                  buildEvent(
                      tenantId.get(), jaegerSpan, tags, tenantIdProvider.getTenantIdTagKey());
              rawSpanBuilder.setEvent(event);
              rawSpanBuilder.setReceivedTimeMillis(System.currentTimeMillis());

              // build raw span
              RawSpan rawSpan = rawSpanBuilder.build();
              LOG.debug("Converted Jaeger span: {} to rawSpan: {} ", jaegerSpan, rawSpan);
              return rawSpan;
            });
  }

  /**
   * Builds the event object from the jaeger span. Note: tagsMap should contain keys that have
   * already been converted to lowercase by the caller.
   */
  private Event buildEvent(
      String tenantId,
      Span jaegerSpan,
      @Nonnull Map<String, KeyValue> tagsMap,
      Optional<String> tenantIdKey) {
    Event.Builder eventBuilder = Event.newBuilder();
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
    if (referencesList != null && referencesList.size() > 0) {
      eventBuilder.setEventRefList(new ArrayList<>());
      // Convert the reflist to a set to remove duplicate references. This has been observed in the
      // field.
      Set<JaegerSpanInternalModel.SpanRef> referencesSet = new HashSet<>(referencesList);
      for (JaegerSpanInternalModel.SpanRef spanRef : referencesSet) {
        EventRef.Builder builder = EventRef.newBuilder();
        builder.setTraceId(spanRef.getTraceId().asReadOnlyByteBuffer());
        builder.setEventId(spanRef.getSpanId().asReadOnlyByteBuffer());
        builder.setRefType(EventRefType.valueOf(spanRef.getRefType().toString()));
        eventBuilder.getEventRefList().add(builder.build());
      }
    }

    // span attributes to event attributes
    Map<String, AttributeValue> attributeFieldMap = new HashMap<>();
    eventBuilder.setAttributesBuilder(Attributes.newBuilder().setAttributeMap(attributeFieldMap));

    List<KeyValue> tagsList = jaegerSpan.getTagsList();
    // Add the attributes to the eventBuilder and generate fields from the attributes
    // 1. Adding all the attributes to the eventBuilder is still being done to maintain backwards
    // compatibility. This
    //    will stop once all consumers of event start using the fields.
    for (KeyValue keyValue : tagsList) {
      // Convert all attributes to lower case so that we don't have to
      // deal with the case sensitivity across different layers in the
      // platform.
      String key = keyValue.getKey().toLowerCase();
      // Do not add the tenant id to the tags.
      if ((tenantIdKey.isPresent() && key.equals(tenantIdKey.get()))) {
        continue;
      }
      attributeFieldMap.put(
          key,
          org.hypertrace.core.spannormalizer.util.AttributeValueCreator.createFromJaegerKeyValue(
              keyValue));
      // Generate a field from the keyValue
      this.fieldsGenerator.addValueToBuilder(key, keyValue, eventBuilder, tagsMap);
    }

    // Generate other fields if possible from the existing ones. eg. http scheme and query string
    // from url
    this.fieldsGenerator.populateOtherFields(eventBuilder);

    // Jaeger Fields - flags, warnings, logs, jaeger service name in the Process
    JaegerFields.Builder jaegerFieldsBuilder = eventBuilder.getJaegerFieldsBuilder();
    // FLAGS
    jaegerFieldsBuilder.setFlags(jaegerSpan.getFlags());

    // WARNINGS
    ProtocolStringList warningsList = jaegerSpan.getWarningsList();
    if (warningsList != null && warningsList.size() > 0) {
      jaegerFieldsBuilder.setWarnings(warningsList);
    }
    // LOGS - NOTE: This is modeled as a google.protobuf.Any
    List<JaegerSpanInternalModel.Log> logsList = jaegerSpan.getLogsList();
    List<String> eventLogsList = new ArrayList<>();
    if (logsList != null && logsList.size() > 0) {
      for (Log log : logsList) {
        try {
          String json = JsonFormat.printer().omittingInsignificantWhitespace().print(log);
          eventLogsList.add(json);
        } catch (InvalidProtocolBufferException e) {
          LOG.warn("Exception converting log object to JSON", e);
        }
      }
      jaegerFieldsBuilder.setLogs(eventLogsList);
    }

    // Jaeger service name
    if (jaegerSpan.getProcess() != null
        && !StringUtils.isEmpty(jaegerSpan.getProcess().getServiceName())) {
      // Keep the attribute for now due to backwards compatibility
      attributeFieldMap.put(
          RawSpanConstants.getValue(JaegerAttribute.JAEGER_ATTRIBUTE_SERVICE_NAME),
          AttributeValueCreator.create(jaegerSpan.getProcess().getServiceName()));
      eventBuilder.setServiceName(jaegerSpan.getProcess().getServiceName());
    }

    // EVENT METRICS

    Map<String, MetricValue> metricMap = new HashMap<>();
    MetricValue durationMetric =
        MetricValue.newBuilder().setValue((double) (endTimeMillis - startTimeMillis)).build();
    metricMap.put("Duration", durationMetric);

    eventBuilder.setMetrics(Metrics.newBuilder().setMetricMap(metricMap).build());

    return eventBuilder.build();
  }
}
