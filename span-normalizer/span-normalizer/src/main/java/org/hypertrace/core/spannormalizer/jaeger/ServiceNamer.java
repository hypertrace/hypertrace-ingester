package org.hypertrace.core.spannormalizer.jaeger;

import static java.util.function.Predicate.not;

import com.typesafe.config.Config;
import io.jaegertracing.api_v2.JaegerSpanInternalModel;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.datamodel.AttributeValue;

public class ServiceNamer {
  public static final String OLD_JAEGER_SERVICENAME_KEY = "jaeger.servicename";
  private static final String SERVICE_NAME_OVERRIDES_KEY = "processor.serviceNameOverrides";

  // Not making this configurable until needed
  private final List<String> fallbackAttributeNames = List.of(OLD_JAEGER_SERVICENAME_KEY);
  private final List<String> overrideAttributeNames;

  public ServiceNamer(Config config) {
    if (config.hasPath(SERVICE_NAME_OVERRIDES_KEY)) {
      this.overrideAttributeNames = config.getStringList(SERVICE_NAME_OVERRIDES_KEY);
    } else {
      this.overrideAttributeNames = Collections.emptyList();
    }
  }

  public Optional<String> findServiceName(
      JaegerSpanInternalModel.Span jaegerSpan, Map<String, AttributeValue> newAttributeMap) {
    return this.getFirstValueFromAttributeKeys(overrideAttributeNames, newAttributeMap)
        .or(() -> this.getProcessServiceName(jaegerSpan))
        .or(() -> this.getFirstValueFromAttributeKeys(fallbackAttributeNames, newAttributeMap));
  }

  private Optional<String> getFirstValueFromAttributeKeys(
      List<String> attributeKeys, Map<String, AttributeValue> newAttributeMap) {

    return attributeKeys.stream()
        .filter(newAttributeMap::containsKey)
        .findFirst()
        .map(newAttributeMap::get)
        .map(AttributeValue::getValue);
  }

  private Optional<String> getProcessServiceName(JaegerSpanInternalModel.Span jaegerSpan) {
    return Optional.of(jaegerSpan.getProcess().getServiceName()).filter(not(String::isEmpty));
  }
}
