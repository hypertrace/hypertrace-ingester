package org.hypertrace.core.spannormalizer.jaeger;

import static org.hypertrace.core.spannormalizer.jaeger.ServiceNamer.OLD_JAEGER_SERVICENAME_KEY;
import static org.junit.jupiter.api.Assertions.*;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.jaegertracing.api_v2.JaegerSpanInternalModel;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.datamodel.shared.trace.AttributeValueCreator;
import org.junit.jupiter.api.Test;

class ServiceNamerTest {

  @Test
  void testOverridingServiceName() {
    Config config =
        ConfigFactory.parseMap(
            Map.of(
                "processor.serviceNameOverrides",
                List.of("override.name.first", "override.name.second")));
    ServiceNamer namer = new ServiceNamer(config);
    JaegerSpanInternalModel.Span span =
        JaegerSpanInternalModel.Span.newBuilder()
            .setProcess(JaegerSpanInternalModel.Process.newBuilder().setServiceName("default-name"))
            .build();

    assertEquals(
        Optional.of("first-override"),
        namer.findServiceName(
            span,
            Map.of(
                "override.name.first",
                AttributeValueCreator.create("first-override"),
                "override.name.second",
                AttributeValueCreator.create("second-override"),
                OLD_JAEGER_SERVICENAME_KEY,
                AttributeValueCreator.create("old-name"))));

    assertEquals(
        Optional.of("second-override"),
        namer.findServiceName(
            span, Map.of("override.name.second", AttributeValueCreator.create("second-override"))));

    assertEquals(Optional.of("default-name"), namer.findServiceName(span, Map.of()));
  }

  @Test
  void testFallbackServiceName() {
    Config config = ConfigFactory.empty();
    ServiceNamer namer = new ServiceNamer(config);

    assertEquals(
        Optional.of("default-name"),
        namer.findServiceName(
            JaegerSpanInternalModel.Span.newBuilder()
                .setProcess(
                    JaegerSpanInternalModel.Process.newBuilder().setServiceName("default-name"))
                .build(),
            Map.of(OLD_JAEGER_SERVICENAME_KEY, AttributeValueCreator.create("old-name"))));
    assertEquals(
        Optional.of("old-name"),
        namer.findServiceName(
            JaegerSpanInternalModel.Span.getDefaultInstance(),
            Map.of(OLD_JAEGER_SERVICENAME_KEY, AttributeValueCreator.create("old-name"))));
  }

  @Test
  void testNoServiceName() {
    Config config = ConfigFactory.empty();
    ServiceNamer namer = new ServiceNamer(config);
    JaegerSpanInternalModel.Span span = JaegerSpanInternalModel.Span.getDefaultInstance();

    assertEquals(Optional.empty(), namer.findServiceName(span, Map.of()));
  }
}
