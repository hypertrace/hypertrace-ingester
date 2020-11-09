package org.hypertrace.core.spannormalizer.fieldgenerators;

import io.jaegertracing.api_v2.JaegerSpanInternalModel;
import java.util.Collection;
import java.util.Map;
import org.hypertrace.core.datamodel.Event;

public abstract class ProtocolFieldsGenerator<T> {
  public void addValueToBuilder(
      String key,
      JaegerSpanInternalModel.KeyValue keyValue,
      Event.Builder eventBuilder,
      Map<String, JaegerSpanInternalModel.KeyValue> tagsMap) {
    FieldGenerator<T> fieldGenerator = getFieldGenerator(key);
    T protocolBuilder = getProtocolBuilder(eventBuilder);
    fieldGenerator.run(key, keyValue, protocolBuilder, tagsMap);
  }

  protected Collection<String> getProtocolCollectorSpanKeys() {
    return getFieldGeneratorMap().keySet();
  }

  protected FieldGenerator<T> getFieldGenerator(String key) {
    return getFieldGeneratorMap().get(key);
  }

  protected abstract T getProtocolBuilder(Event.Builder eventBuilder);

  protected abstract Map<String, FieldGenerator<T>> getFieldGeneratorMap();
}
