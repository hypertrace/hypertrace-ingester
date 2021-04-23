package org.hypertrace.viewgenerator.generators;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.avro.Schema;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.LogEvent;
import org.hypertrace.core.datamodel.LogEvents;
import org.hypertrace.core.viewgenerator.JavaCodeBasedViewGenerator;
import org.hypertrace.viewgenerator.api.LogEventView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogEventViewGenerator implements JavaCodeBasedViewGenerator<LogEvents, LogEventView> {

  private static final Logger LOG = LoggerFactory.getLogger(LogEventViewGenerator.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  // refer following links for attribute keys in log message
  // https://github.com/opentracing/specification/blob/master/semantic_conventions.md#log-fields-table
  // https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/logs/v1/logs.proto#L108
  private static final List<String> SUMMARY_KEYS = List.of("message", "body", "event");

  @Override
  public List<LogEventView> process(LogEvents logEvents) {
    try {
      List<LogEventView> list = new ArrayList<>();
      for (LogEvent logEventRecord : logEvents.getLogEvents()) {
        LogEventView build =
            LogEventView.newBuilder()
                .setSpanId(logEventRecord.getSpanId())
                .setTraceId(logEventRecord.getTraceId())
                .setTimestampNanos(logEventRecord.getTimestampNanos())
                .setTenantId(logEventRecord.getTenantId())
                .setAttributes(convertAttributes(logEventRecord.getAttributes()))
                .setSummary(getSummary(logEventRecord.getAttributes()))
                .build();
        list.add(build);
      }
      return list;
    } catch (Exception e) {
      LOG.error("Exception processing log records", e);
      return null;
    }
  }

  private String getSummary(Attributes attributes) {
    if (isEmpty(attributes)) {
      return null;
    }
    Map<String, AttributeValue> attributeValueMap = attributes.getAttributeMap();
    Optional<String> summary =
        SUMMARY_KEYS.stream().filter(attributeValueMap::containsKey).findFirst();
    return summary.orElseGet(
        () -> attributeValueMap.entrySet().stream().findFirst().get().getValue().getValue());
  }

  private String convertAttributes(Attributes attributes) throws JsonProcessingException {
    if (isEmpty(attributes)) {
      return null;
    }
    Map<String, String> resultMap = new HashMap<>();

    for (Map.Entry<String, AttributeValue> entry : attributes.getAttributeMap().entrySet()) {
      resultMap.put(entry.getKey(), entry.getValue().getValue());
    }

    return OBJECT_MAPPER.writeValueAsString(resultMap);
  }

  private boolean isEmpty(Attributes attributes) {
    return (null == attributes
        || null == attributes.getAttributeMap()
        || attributes.getAttributeMap().isEmpty());
  }

  @Override
  public String getViewName() {
    return LogEventViewGenerator.class.getName();
  }

  @Override
  public Schema getSchema() {
    return LogEventView.getClassSchema();
  }

  @Override
  public Class<LogEventView> getViewClass() {
    return LogEventView.class;
  }
}
