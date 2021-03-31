package org.hypertrace.viewgenerator.generators;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.hypertrace.core.datamodel.AttributeValue;
import org.hypertrace.core.datamodel.Attributes;
import org.hypertrace.core.datamodel.LogEventRecords;
import org.hypertrace.core.viewgenerator.JavaCodeBasedViewGenerator;
import org.hypertrace.viewgenerator.api.LogEventView;

public class LogEventViewGenerator
    implements JavaCodeBasedViewGenerator<LogEventRecords, LogEventView> {

  @Override
  public List<LogEventView> process(LogEventRecords logEventRecords) {
    return logEventRecords.getLogEventRecords().stream()
        .filter(v -> !v.getAttributes().getAttributeMap().isEmpty())
        .map(
            logEventRecord ->
                LogEventView.newBuilder()
                    .setSpanId(logEventRecord.getSpanId())
                    .setTraceId(logEventRecord.getTraceId())
                    .setTimeStamp(logEventRecord.getTimeStamp())
                    .setTenantId(logEventRecord.getCustomerId())
                    .setAttributes(convertAttributes(logEventRecord.getAttributes()))
                    .build())
        .collect(Collectors.toList());
  }

  private String convertAttributes(Attributes attributes) {
    Map<String, String> resultMap = new HashMap<>();

    for (Map.Entry<String, AttributeValue> entry : attributes.getAttributeMap().entrySet()) {
      resultMap.put(entry.getKey(), entry.getValue().getValue());
    }

    return resultMap.toString();
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
