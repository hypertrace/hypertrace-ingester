package org.hypertrace.traceenricher.enrichment.enrichers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.hypertrace.core.datamodel.*;
import org.hypertrace.traceenricher.enrichment.clients.ClientRegistry;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ResourceAttributeEnricherTest extends AbstractAttributeEnricherTest {

  private final ResourceAttributeEnricher resourceAttributeEnricher =
      new ResourceAttributeEnricher();

  private List<String> resourceAttributesToAddList;

  @BeforeAll
  public void setup() {
    String configFilePath =
        Thread.currentThread().getContextClassLoader().getResource("enricher.conf").getPath();
    if (configFilePath == null) {
      throw new RuntimeException("Cannot find enricher config file enricher.conf in the classpath");
    }

    Config fileConfig = ConfigFactory.parseFile(new File(configFilePath));
    Config configs = ConfigFactory.load(fileConfig);
    if (!configs.hasPath("enricher.ResourceAttributeEnricher")) {
      throw new RuntimeException(
          "Cannot find enricher config for ResourceAttributeEnricher in " + configs);
    }
    Config enricherConfig = configs.getConfig("enricher.ResourceAttributeEnricher");
    resourceAttributesToAddList = enricherConfig.getStringList("attributes");
    resourceAttributeEnricher.init(enricherConfig, mock(ClientRegistry.class));
  }

  @Test
  public void noResourceInTrace() {
    // This trace has no resource attributes.
    StructuredTrace trace = getBigTrace();
    for (Event event : trace.getEventList()) {
      int attributeMapSize = 0;
      if (event.getAttributes() != null && event.getAttributes().getAttributeMap() != null) {
        attributeMapSize = event.getAttributes().getAttributeMap().size();
      }
      resourceAttributeEnricher.enrichEvent(trace, event);
      if (event.getAttributes() != null && event.getAttributes().getAttributeMap() != null) {
        assertEquals(attributeMapSize, event.getAttributes().getAttributeMap().size());
      }
    }
  }

  @Test
  public void traceWithResource() {
    StructuredTrace structuredTrace = mock(StructuredTrace.class);
    List<Resource> resourceList = new ArrayList<>();

    resourceList.add(getResource1());
    resourceList.add(getResource2());
    resourceList.add(getResource3());
    resourceList.add(getResource4());
    when(structuredTrace.getResourceList()).thenReturn(resourceList);

    Attributes attributes = Attributes.newBuilder().setAttributeMap(new HashMap<>()).build();
    Event event =
        Event.newBuilder()
            .setAttributes(attributes)
            .setEventId(createByteBuffer("event1"))
            .setCustomerId(TENANT_ID)
            .build();
    event.setResourceIndex(0);
    resourceAttributeEnricher.enrichEvent(structuredTrace, event);
    assertEquals(
        resourceAttributesToAddList.size() - 2, event.getAttributes().getAttributeMap().size());
    assertEquals(
        "test-56f5d554c-5swkj", event.getAttributes().getAttributeMap().get("pod.name").getValue());
    assertEquals(
        "01188498a468b5fef1eb4accd63533297c195a73",
        event.getAttributes().getAttributeMap().get("service.version").getValue());
    assertEquals("10.21.18.171", event.getAttributes().getAttributeMap().get("ip").getValue());
    assertEquals(
        "worker-hypertrace",
        event.getAttributes().getAttributeMap().get("node.selector").getValue());

    Event event2 =
        Event.newBuilder()
            .setAttributes(Attributes.newBuilder().setAttributeMap(new HashMap<>()).build())
            .setEventId(createByteBuffer("event2"))
            .setCustomerId(TENANT_ID)
            .build();
    event2.setResourceIndex(1);
    addAttribute(event2, "service.version", "123");
    addAttribute(event2, "cluster.name", "default");
    resourceAttributeEnricher.enrichEvent(structuredTrace, event2);
    assertEquals(
        resourceAttributesToAddList.size(), event2.getAttributes().getAttributeMap().size());
    assertEquals("123", event2.getAttributes().getAttributeMap().get("service.version").getValue());
    assertEquals(
        "default", event2.getAttributes().getAttributeMap().get("cluster.name").getValue());
    assertEquals(
        "worker-generic", event2.getAttributes().getAttributeMap().get("node.name").getValue());
    assertEquals(
        "worker-generic", event2.getAttributes().getAttributeMap().get("node.selector").getValue());

    Event event3 =
        Event.newBuilder()
            .setAttributes(Attributes.newBuilder().setAttributeMap(new HashMap<>()).build())
            .setEventId(createByteBuffer("event3"))
            .setCustomerId(TENANT_ID)
            .build();
    event3.setResourceIndex(2);
    resourceAttributeEnricher.enrichEvent(structuredTrace, event3);
    assertEquals("", event3.getAttributes().getAttributeMap().get("node.selector").getValue());

    Event event4 =
        Event.newBuilder()
            .setAttributes(Attributes.newBuilder().setAttributeMap(new HashMap<>()).build())
            .setEventId(createByteBuffer("event4"))
            .setCustomerId(TENANT_ID)
            .build();
    event4.setResourceIndex(3);
    resourceAttributeEnricher.enrichEvent(structuredTrace, event4);
    assertEquals(
        "worker-generic", event4.getAttributes().getAttributeMap().get("node.selector").getValue());
    assertEquals("pod1", event4.getAttributes().getAttributeMap().get("pod.name").getValue());
  }

  private Resource getResource4() {
    Map<String, AttributeValue> resourceAttributeMap =
        new HashMap<>() {
          {
            put("node.selector", AttributeValue.newBuilder().setValue("worker-generic").build());
            put("host.name", AttributeValue.newBuilder().setValue("pod1").build());
          }
        };
    return Resource.newBuilder()
        .setAttributes(Attributes.newBuilder().setAttributeMap(resourceAttributeMap).build())
        .build();
  }

  private Resource getResource3() {
    Map<String, AttributeValue> resourceAttributeMap =
        new HashMap<>() {
          {
            put(
                "node.selector",
                AttributeValue.newBuilder()
                    .setValue("node-role.kubernetes.io/worker-generic/")
                    .build());
          }
        };
    return Resource.newBuilder()
        .setAttributes(Attributes.newBuilder().setAttributeMap(resourceAttributeMap).build())
        .build();
  }

  private Resource getResource2() {
    Map<String, AttributeValue> resourceAttributeMap =
        new HashMap<>() {
          {
            put(
                "service.version",
                AttributeValue.newBuilder()
                    .setValue("018a468b5fef1eb4accd63533297c195a73")
                    .build());
            put("environment", AttributeValue.newBuilder().setValue("stage").build());
            put(
                "opencensus.exporterversion",
                AttributeValue.newBuilder().setValue("Jaeger-Go-2.23.1").build());
            put("host.name", AttributeValue.newBuilder().setValue("test1-56f5d554c-5swkj").build());
            put("ip", AttributeValue.newBuilder().setValue("10.21.18.1712").build());
            put("client-uuid", AttributeValue.newBuilder().setValue("53a112a715bdf86").build());
            put("node.name", AttributeValue.newBuilder().setValue("worker-generic").build());
            put(
                "cluster.name",
                AttributeValue.newBuilder().setValue("worker-generic-cluster").build());
            put(
                "node.selector",
                AttributeValue.newBuilder()
                    .setValue("node-role.kubernetes.io/worker-generic")
                    .build());
          }
        };
    return Resource.newBuilder()
        .setAttributes(Attributes.newBuilder().setAttributeMap(resourceAttributeMap).build())
        .build();
  }

  private Resource getResource1() {
    // In ideal scenarios below resource tags are present in spans.
    Map<String, AttributeValue> resourceAttributeMap =
        new HashMap<>() {
          {
            put(
                "service.version",
                AttributeValue.newBuilder()
                    .setValue("01188498a468b5fef1eb4accd63533297c195a73")
                    .build());
            put("environment", AttributeValue.newBuilder().setValue("stage").build());
            put(
                "opencensus.exporterversion",
                AttributeValue.newBuilder().setValue("Jaeger-Go-2.23.1").build());
            put("host.name", AttributeValue.newBuilder().setValue("test-56f5d554c-5swkj").build());
            put("ip", AttributeValue.newBuilder().setValue("10.21.18.171").build());
            put("client-uuid", AttributeValue.newBuilder().setValue("53a112a715bda986").build());
            put(
                "node.selector",
                AttributeValue.newBuilder()
                    .setValue("node-role.kubernetes.io/worker-hypertrace")
                    .build());
          }
        };
    return Resource.newBuilder()
        .setAttributes(Attributes.newBuilder().setAttributeMap(resourceAttributeMap).build())
        .build();
  }

  private void addAttribute(Event event, String key, String val) {
    event
        .getAttributes()
        .getAttributeMap()
        .put(key, AttributeValue.newBuilder().setValue(val).build());
  }
}
