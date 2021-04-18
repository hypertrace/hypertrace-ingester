package org.hypertrace.viewgenerator.generators;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.LogEvents;
import org.hypertrace.core.viewgenerator.JavaCodeBasedViewGenerator;
import org.hypertrace.viewgenerator.api.LogEventView;

public class LogEventViewGenerator implements JavaCodeBasedViewGenerator<LogEvents, LogEventView> {

  @Override
  public List<LogEventView> process(LogEvents logEvents) {
    return logEvents.getLogEvents().stream()
        .map(
            logEventRecord ->
                LogEventView.newBuilder()
                    .setSpanId(logEventRecord.getSpanId())
                    .setTraceId(logEventRecord.getTraceId())
                    .setTimestampNanos(logEventRecord.getTimestampNanos())
                    .setTenantId(logEventRecord.getTenantId())
                    .setAttributes(convertAttributes(logEventRecord.getAttributes()))
                    .build())
        .collect(Collectors.toList());
  }

  private String convertAttributes(Attributes attributes) {
    if (null == attributes
        || null == attributes.getAttributeMap()
        || attributes.getAttributeMap().isEmpty()) {
      return null;
    }
    Map<String, String> resultMap = new HashMap<>();

    for (Map.Entry<String, AttributeValue> entry : attributes.getAttributeMap().entrySet()) {
      resultMap.put(entry.getKey(), entry.getValue().getValue());
    }
    String result = null;

    try {
      result = new ObjectMapper().writeValueAsString(resultMap);
    } catch (JsonProcessingException e) {
      // ignore
    }

    return result;
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
