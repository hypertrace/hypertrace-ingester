package org.hypertrace.core.spannormalizer.jaeger;

import static org.hypertrace.core.datamodel.shared.AvroBuilderCache.fastNewBuilder;
import static org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry.registerCounter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ProtocolStringList;
import com.google.protobuf.util.Timestamps;
import com.typesafe.config.Config;
import io.jaegertracing.api_v2.JaegerSpanInternalModel;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.KeyValue;
import io.jaegertracing.api_v2.JaegerSpanInternalModel.Span;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
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
import org.hypertrace.core.spannormalizer.constants.SpanNormalizerConstants;
import org.hypertrace.core.spannormalizer.jaeger.tenant.PIIMatchType;
import org.hypertrace.core.spannormalizer.util.JaegerHTTagsConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JaegerSpanNormalizer {

  private static final Logger LOG = LoggerFactory.getLogger(JaegerSpanNormalizer.class);
  private static final Logger FILE_LOGGER = LoggerFactory.getLogger("tagsLogger");
  private static final Map<String, String> ATTRIBUTES = new ConcurrentHashMap<>();
  private static boolean HAS_LOGGED_ATTRIBUTES = false;

  /**
   * Service name can be sent against this key as well
   */
  public static final String OLD_JAEGER_SERVICENAME_KEY = "jaeger.servicename";

  private static final String SPAN_NORMALIZATION_TIME_METRIC = "span.normalization.time";
  private static final String SPAN_REDACTED_ATTRIBUTES_COUNTER = "span.redacted.attributes";
  private final Map<String, Counter> spanAttributesRedactedCounters = new ConcurrentHashMap<>();

  private static JaegerSpanNormalizer INSTANCE;
  private final ConcurrentMap<String, Timer> tenantToSpanNormalizationTimer =
      new ConcurrentHashMap<>();

  private final JaegerResourceNormalizer resourceNormalizer = new JaegerResourceNormalizer();
  private final TenantIdHandler tenantIdHandler;
  private AttributeValue redactedAttributeValue = null;
  private final Set<String> tagKeysToRedact = new HashSet<>();
  private final Set<Pattern> tagRegexPatternToRedact = new HashSet<>();

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
    try {
      if (config.hasPath(SpanNormalizerConstants.PII_KEYS_CONFIG_KEY)) {
        config.getStringList(SpanNormalizerConstants.PII_KEYS_CONFIG_KEY).stream()
            .map(String::toUpperCase)
            .forEach(tagKeysToRedact::add);
        redactedAttributeValue =
            AttributeValue.newBuilder()
                .setValue(SpanNormalizerConstants.PII_FIELD_REDACTED_VAL)
                .build();
      }
      if (config.hasPath(SpanNormalizerConstants.PII_REGEX_CONFIG_KEY)) {
        config.getStringList(SpanNormalizerConstants.PII_REGEX_CONFIG_KEY).stream()
            .map(Pattern::compile)
            .forEach(tagRegexPatternToRedact::add);
        if (redactedAttributeValue == null) {
          redactedAttributeValue =
              AttributeValue.newBuilder()
                  .setValue(SpanNormalizerConstants.PII_FIELD_REDACTED_VAL)
                  .build();
        }
      }
    } catch (Exception e) {
      LOG.error("An exception occurred while loading redaction configs: ", e);
    }
    this.tenantIdHandler = new TenantIdHandler(config);
  }

  public Timer getSpanNormalizationTimer(String tenantId) {
    return tenantToSpanNormalizationTimer.get(tenantId);
  }

  public Map<String, Counter> getSpanAttributesRedactedCounters() {
    return spanAttributesRedactedCounters;
  }

  @Nullable
  public RawSpan convert(String tenantId, Span jaegerSpan) throws Exception {
    Map<String, KeyValue> tags =
        jaegerSpan.getTagsList().stream()
            .collect(Collectors.toMap(t -> t.getKey().toLowerCase(), t -> t, (v1, v2) -> v2));

    // Record the time taken for converting the span, along with the tenant id tag.
    return tenantToSpanNormalizationTimer
        .computeIfAbsent(
            tenantId,
            tenant ->
                PlatformMetricsRegistry.registerTimer(
                    SPAN_NORMALIZATION_TIME_METRIC, Map.of("tenantId", tenant)))
        .recordCallable(getRawSpanNormalizerCallable(jaegerSpan, tags, tenantId));
  }

  @Nonnull
  private Callable<RawSpan> getRawSpanNormalizerCallable(
      Span jaegerSpan, Map<String, KeyValue> spanTags, String tenantId) {
    return () -> {
      Builder rawSpanBuilder = fastNewBuilder(RawSpan.Builder.class);
      rawSpanBuilder.setCustomerId(tenantId);
      rawSpanBuilder.setTraceId(jaegerSpan.getTraceId().asReadOnlyByteBuffer());
      // Build Event
      Event event =
          buildEvent(
              tenantId,
              jaegerSpan,
              spanTags,
              tenantIdHandler.getTenantIdProvider().getTenantIdTagKey());
      rawSpanBuilder.setEvent(event);
      rawSpanBuilder.setReceivedTimeMillis(System.currentTimeMillis());
      resourceNormalizer
          .normalize(jaegerSpan, tenantIdHandler.getTenantIdProvider().getTenantIdTagKey())
          .ifPresent(rawSpanBuilder::setResource);

      // redact PII tags, tag comparisons are case insensitive (Resource tags are skipped)
      if (redactedAttributeValue != null) {
        sanitiseSpan(rawSpanBuilder);
      }

      // build raw span
      RawSpan rawSpan = rawSpanBuilder.build();
      if (LOG.isDebugEnabled()) {
        logSpanConversion(jaegerSpan, rawSpan);
      }

      if (!HAS_LOGGED_ATTRIBUTES && FILE_LOGGER.isDebugEnabled()) {
        //Each entry = 10 bytes for keys + 50 bytes for values on an average. Total map size ~ 6M
        if (ATTRIBUTES.size() > 100_000) {
          synchronized (this) {
            ATTRIBUTES.forEach((k, v) -> FILE_LOGGER.debug("{},{}", k, v));
            ATTRIBUTES.clear();
            HAS_LOGGED_ATTRIBUTES = true;
          }
        } else {
          Map<String, AttributeValue> attributes = rawSpan.getEvent().getAttributes()
              .getAttributeMap();
          attributes.forEach((k, v) -> ATTRIBUTES.put(k, v.getValue()));
        }
      }

      return rawSpan;
    };
  }

  private void sanitiseSpan(Builder rawSpanBuilder) {

    try {
      var attributeMap = rawSpanBuilder.getEvent().getAttributes().getAttributeMap();
      var spanServiceName = rawSpanBuilder.getEvent().getServiceName();
      Set<String> tagKeys = attributeMap.keySet();

      AtomicReference<Boolean> containsPIIFields = new AtomicReference<>();
      containsPIIFields.set(false);

      try {
        tagKeys.stream()
            .filter(
                tagKey ->
                    tagKeysToRedact.contains(tagKey.toUpperCase())
                        || attributeMap
                        .get(tagKey)
                        .getValue()
                        .equals(SpanNormalizerConstants.PII_FIELD_REDACTED_VAL))
            .peek(
                tagKey -> {
                  containsPIIFields.set(true);
                  spanAttributesRedactedCounters
                      .computeIfAbsent(
                          PIIMatchType.KEY.toString(),
                          k ->
                              registerCounter(
                                  SPAN_REDACTED_ATTRIBUTES_COUNTER,
                                  Map.of("matchType", PIIMatchType.KEY.toString())))
                      .increment();
                })
            .forEach(
                tagKey -> {
                  logSpanRedaction(tagKey, spanServiceName, PIIMatchType.KEY);
                  attributeMap.put(tagKey, redactedAttributeValue);
                });
      } catch (Exception e) {
        LOG.error("An exception occurred while sanitising spans with key match: ", e);
      }

      try {
        for (Pattern pattern : tagRegexPatternToRedact) {
          for (String tagKey : tagKeys) {
            if (pattern.matcher(attributeMap.get(tagKey).getValue()).matches()) {
              containsPIIFields.set(true);
              spanAttributesRedactedCounters
                  .computeIfAbsent(
                      PIIMatchType.REGEX.toString(),
                      k ->
                          registerCounter(
                              SPAN_REDACTED_ATTRIBUTES_COUNTER,
                              Map.of("matchType", PIIMatchType.REGEX.toString())))
                  .increment();
              logSpanRedaction(tagKey, spanServiceName, PIIMatchType.REGEX);
              attributeMap.put(tagKey, redactedAttributeValue);
            }
          }
        }
      } catch (Exception e) {
        LOG.error("An exception occurred while sanitising spans with regex match: ", e);
      }

      // if the trace contains PII field, add a field to indicate this. We can later slice-and-dice
      // based on this tag
      if (containsPIIFields.get()) {
        rawSpanBuilder
            .getEvent()
            .getAttributes()
            .getAttributeMap()
            .put(
                SpanNormalizerConstants.CONTAINS_PII_TAGS_KEY,
                AttributeValue.newBuilder().setValue("true").build());
      }
    } catch (Exception e) {
      LOG.error("An exception occurred while sanitising spans: ", e);
    }
  }

  private void logSpanRedaction(String tagKey, String spanServiceName, PIIMatchType matchType) {
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            new ObjectMapper()
                .writerWithDefaultPrettyPrinter()
                .writeValueAsString(
                    Map.of(
                        "bookmark",
                        "REDACTED_KEY",
                        "key",
                        tagKey,
                        "matchtype",
                        matchType.toString(),
                        "serviceName",
                        spanServiceName)));
      }
    } catch (Exception e) {
      LOG.error("An exception occurred while logging span redaction: ", e);
    }
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

    List<KeyValue> tagsList = jaegerSpan.getTagsList();
    // Stop populating first class fields for - grpc, rpc, http, and sql.
    // see more details:
    // https://github.com/hypertrace/hypertrace/issues/244
    // https://github.com/hypertrace/hypertrace/issues/245
    for (KeyValue keyValue : tagsList) {
      // Convert all attributes to lower case so that we don't have to
      // deal with the case sensitivity across different layers in the
      // platform.
      String key = keyValue.getKey().toLowerCase();
      // Do not add the tenant id to the tags.
      if ((tenantIdKey.isPresent() && key.equals(tenantIdKey.get()))) {
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

    // Jaeger service name can come from either first class field in Span or the tag
    // `jaeger.servicename`
    String serviceName =
        !StringUtils.isEmpty(jaegerSpan.getProcess().getServiceName())
            ? jaegerSpan.getProcess().getServiceName()
            : attributeFieldMap.containsKey(OLD_JAEGER_SERVICENAME_KEY)
                ? attributeFieldMap.get(OLD_JAEGER_SERVICENAME_KEY).getValue()
                : StringUtils.EMPTY;

    if (!StringUtils.isEmpty(serviceName)) {
      eventBuilder.setServiceName(serviceName);
      // in case `jaeger.servicename` is present in the map, remove it
      attributeFieldMap.remove(OLD_JAEGER_SERVICENAME_KEY);
      attributeFieldMap.put(
          RawSpanConstants.getValue(JaegerAttribute.JAEGER_ATTRIBUTE_SERVICE_NAME),
          AttributeValueCreator.create(serviceName));
    }

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

  // Check if debug log is enabled before calling this method
  private void logSpanConversion(Span jaegerSpan, RawSpan rawSpan) {
    try {
      LOG.debug(
          "Converted Jaeger span: {} to rawSpan: {} ",
          jaegerSpan,
          convertToJsonString(rawSpan, rawSpan.getSchema()));
    } catch (IOException e) {
      LOG.warn("An exception occurred while converting avro to JSON string", e);
    }
  }

  // We should have a small library for useful short methods like this one such as this.
  public static <T extends SpecificRecordBase> String convertToJsonString(T object, Schema schema)
      throws IOException {
    JsonEncoder encoder;
    try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
      DatumWriter<T> writer = new SpecificDatumWriter<>(schema);
      encoder = EncoderFactory.get().jsonEncoder(schema, output, false);
      writer.write(object, encoder);
      encoder.flush();
      output.flush();
      return output.toString();
    }
  }
}
