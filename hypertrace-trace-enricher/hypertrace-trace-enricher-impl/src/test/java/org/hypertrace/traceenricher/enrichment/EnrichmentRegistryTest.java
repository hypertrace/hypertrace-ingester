package org.hypertrace.traceenricher.enrichment;

import static org.hypertrace.traceenricher.enrichment.EnricherConfigFactory.getEnricherConfigPath;
import static org.hypertrace.traceenricher.enrichment.EnrichmentRegistry.ENRICHER_CLASS_CONFIG_PATH;
import static org.hypertrace.traceenricher.enrichment.EnrichmentRegistry.ENRICHER_DEPENDENCIES_CONFIG_PATH;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class EnrichmentRegistryTest {

  @Test
  public void empty() {
    EnrichmentRegistry registry = new EnrichmentRegistry();
    registry.registerEnrichers(new HashMap<>());
    Assertions.assertEquals(registry.getOrderedRegisteredEnrichers().size(), 0);
  }

  @Test
  public void classNotFoundFailure() {
    String enricher = "TestEnricher";
    Config config = mock(Config.class);
    when(config.getString(ENRICHER_CLASS_CONFIG_PATH)).thenReturn("nonexistentclass");

    Map<String, Config> enricherConfigs = new HashMap<>();
    enricherConfigs.put(enricher, config);
    EnrichmentRegistry registry = new EnrichmentRegistry();
    registry.registerEnrichers(enricherConfigs);
    Assertions.assertEquals(registry.getOrderedRegisteredEnrichers().size(), 0);
  }

  @Test
  public void classNotEnricherSubclassFailure() {
    class TestEnricher {}

    String enricher = "TestEnricher";
    Config config = mock(Config.class);
    when(config.getString(ENRICHER_CLASS_CONFIG_PATH)).thenReturn(TestEnricher.class.getName());

    Map<String, Config> enricherConfigs = new HashMap<>();
    enricherConfigs.put(enricher, config);
    EnrichmentRegistry registry = new EnrichmentRegistry();
    registry.registerEnrichers(enricherConfigs);
    Assertions.assertEquals(registry.getOrderedRegisteredEnrichers().size(), 0);
  }

  @Test
  public void dependencyNotFoundFailure() {
    abstract class TestEnricher1 extends AbstractTraceEnricher {}

    String enricher1 = "TestEnricher";
    Config config1 = mock(Config.class);
    when(config1.getString(ENRICHER_CLASS_CONFIG_PATH)).thenReturn(TestEnricher1.class.getName());
    when(config1.hasPath(ENRICHER_DEPENDENCIES_CONFIG_PATH)).thenReturn(true);
    when(config1.getStringList(ENRICHER_DEPENDENCIES_CONFIG_PATH))
        .thenReturn(Arrays.asList("nonexistentdependency"));

    Map<String, Config> enricherConfigs = new HashMap<>();
    enricherConfigs.put(enricher1, config1);
    EnrichmentRegistry registry = new EnrichmentRegistry();
    registry.registerEnrichers(enricherConfigs);
    Assertions.assertEquals(registry.getOrderedRegisteredEnrichers().size(), 0);

    abstract class TestEnricher2 extends AbstractTraceEnricher {}

    String enricher2 = "TestEnricher2";
    Config config2 = mock(Config.class);
    when(config2.getString(ENRICHER_CLASS_CONFIG_PATH)).thenReturn(TestEnricher2.class.getName());
    when(config2.hasPath(ENRICHER_DEPENDENCIES_CONFIG_PATH)).thenReturn(true);
    when(config2.getStringList(ENRICHER_DEPENDENCIES_CONFIG_PATH))
        .thenReturn(Arrays.asList(enricher1));
    enricherConfigs.put(enricher2, config2);

    // TestEnricher2 depends on TestEnricher1, it will also fail to register because its dependency
    // fail to register
    registry.registerEnrichers(enricherConfigs);
    Assertions.assertEquals(registry.getOrderedRegisteredEnrichers().size(), 0);
  }

  @Test
  public void cyclicDependencyFailure() {
    abstract class TestEnricher1 extends AbstractTraceEnricher {}
    abstract class TestEnricher2 extends AbstractTraceEnricher {}

    String enricher1 = "TestEnricher1";
    Config config1 = mock(Config.class);
    String enricher2 = "TestEnricher2";
    Config config2 = mock(Config.class);
    when(config1.getString(ENRICHER_CLASS_CONFIG_PATH)).thenReturn(TestEnricher1.class.getName());
    when(config1.hasPath(ENRICHER_DEPENDENCIES_CONFIG_PATH)).thenReturn(true);
    when(config1.getStringList(ENRICHER_DEPENDENCIES_CONFIG_PATH))
        .thenReturn(Arrays.asList(enricher2));
    when(config2.getString(ENRICHER_CLASS_CONFIG_PATH)).thenReturn(TestEnricher2.class.getName());
    when(config2.hasPath(ENRICHER_DEPENDENCIES_CONFIG_PATH)).thenReturn(true);
    when(config2.getStringList(ENRICHER_DEPENDENCIES_CONFIG_PATH))
        .thenReturn(Arrays.asList(enricher1));

    Map<String, Config> enricherConfigs = new HashMap<>();
    enricherConfigs.put(enricher1, config1);
    enricherConfigs.put(enricher2, config2);
    EnrichmentRegistry registry = new EnrichmentRegistry();
    registry.registerEnrichers(enricherConfigs);
    Assertions.assertEquals(registry.getOrderedRegisteredEnrichers().size(), 0);
  }

  @Test
  public void enricherNoDependency() {
    abstract class TestEnricher extends AbstractTraceEnricher {}

    String enricher = "TestEnricher";
    Config config = mock(Config.class);
    when(config.getString(ENRICHER_CLASS_CONFIG_PATH)).thenReturn(TestEnricher.class.getName());
    when(config.hasPath(ENRICHER_DEPENDENCIES_CONFIG_PATH)).thenReturn(false);

    Map<String, Config> enricherConfigs = new HashMap<>();
    enricherConfigs.put(enricher, config);
    EnrichmentRegistry registry = new EnrichmentRegistry();
    registry.registerEnrichers(enricherConfigs);
    List<EnricherInfo> enrichers = registry.getOrderedRegisteredEnrichers();
    Assertions.assertEquals(enrichers.size(), 1);
    Assertions.assertEquals(
        enrichers.get(0),
        new EnricherInfo(enricher, TestEnricher.class, Collections.emptyList(), config));
  }

  @Test
  public void enricherWithDependencies() {
    abstract class TestEnricher1 extends AbstractTraceEnricher {}
    abstract class TestEnricher2 extends AbstractTraceEnricher {}
    abstract class TestEnricher3 extends AbstractTraceEnricher {}

    // TestEnricher1 depends on TestEnricher2 depends on TestEnricher3
    String enricher1 = "TestEnricher1";
    Config config1 = mock(Config.class);
    String enricher2 = "TestEnricher2";
    Config config2 = mock(Config.class);
    String enricher3 = "TestEnricher3";
    Config config3 = mock(Config.class);

    when(config1.getString(ENRICHER_CLASS_CONFIG_PATH)).thenReturn(TestEnricher1.class.getName());
    when(config1.hasPath(ENRICHER_DEPENDENCIES_CONFIG_PATH)).thenReturn(true);
    when(config1.getStringList(ENRICHER_DEPENDENCIES_CONFIG_PATH))
        .thenReturn(Arrays.asList(enricher2));
    when(config2.getString(ENRICHER_CLASS_CONFIG_PATH)).thenReturn(TestEnricher2.class.getName());
    when(config2.hasPath(ENRICHER_DEPENDENCIES_CONFIG_PATH)).thenReturn(true);
    when(config2.getStringList(ENRICHER_DEPENDENCIES_CONFIG_PATH))
        .thenReturn(Arrays.asList(enricher3));
    when(config3.getString(ENRICHER_CLASS_CONFIG_PATH)).thenReturn(TestEnricher3.class.getName());
    when(config3.hasPath(ENRICHER_DEPENDENCIES_CONFIG_PATH)).thenReturn(false);

    Map<String, Config> enricherConfigs = new HashMap<>();
    enricherConfigs.put(enricher1, config1);
    enricherConfigs.put(enricher2, config2);
    enricherConfigs.put(enricher3, config3);
    EnrichmentRegistry registry = new EnrichmentRegistry();
    registry.registerEnrichers(enricherConfigs);
    List<EnricherInfo> enrichers = registry.getOrderedRegisteredEnrichers();
    Assertions.assertEquals(enrichers.size(), 3);

    // The order should be TestEnricher3, TestEnricher2, TestEnricher1
    EnricherInfo enricherInfo3 =
        new EnricherInfo(enricher3, TestEnricher3.class, Collections.emptyList(), config3);
    EnricherInfo enricherInfo2 =
        new EnricherInfo(enricher2, TestEnricher2.class, Arrays.asList(enricherInfo3), config2);
    EnricherInfo enricherInfo1 =
        new EnricherInfo(enricher1, TestEnricher1.class, Arrays.asList(enricherInfo2), config1);

    Assertions.assertEquals(enrichers.get(0), enricherInfo3);
    Assertions.assertEquals(enrichers.get(1), enricherInfo2);
    Assertions.assertEquals(enrichers.get(2), enricherInfo1);

    // Add testEnricher4 that depends on TestEnricher2
    abstract class TestEnricher4 extends AbstractTraceEnricher {}
    String enricher4 = "TestEnricher4";
    Config config4 = mock(Config.class);
    when(config4.getString(ENRICHER_CLASS_CONFIG_PATH)).thenReturn(TestEnricher4.class.getName());
    when(config4.hasPath(ENRICHER_DEPENDENCIES_CONFIG_PATH)).thenReturn(true);
    when(config4.getStringList(ENRICHER_DEPENDENCIES_CONFIG_PATH))
        .thenReturn(Arrays.asList(enricher2));
    enricherConfigs = new HashMap<>();
    enricherConfigs.put(enricher4, config4);
    registry.registerEnrichers(enricherConfigs);
    enrichers = registry.getOrderedRegisteredEnrichers();
    Assertions.assertEquals(enrichers.size(), 4);

    EnricherInfo enricherInfo4 =
        new EnricherInfo(enricher4, TestEnricher4.class, Arrays.asList(enricherInfo2), config4);
    Assertions.assertEquals(enrichers.get(0), enricherInfo3);
    Assertions.assertEquals(enrichers.get(1), enricherInfo2);
    Assertions.assertEquals(enrichers.get(2), enricherInfo1);
    Assertions.assertEquals(enrichers.get(3), enricherInfo4);

    // Add testEnricher5 that depends on TestEnricher2 and TestEnricher3
    abstract class TestEnricher5 extends AbstractTraceEnricher {}
    String enricher5 = "TestEnricher5";
    Config config5 = mock(Config.class);
    when(config5.getString(ENRICHER_CLASS_CONFIG_PATH)).thenReturn(TestEnricher5.class.getName());
    when(config5.hasPath(ENRICHER_DEPENDENCIES_CONFIG_PATH)).thenReturn(true);
    when(config5.getStringList(ENRICHER_DEPENDENCIES_CONFIG_PATH))
        .thenReturn(Arrays.asList(enricher2, enricher3));
    enricherConfigs = new HashMap<>();
    enricherConfigs.put(enricher5, config5);
    registry.registerEnrichers(enricherConfigs);
    enrichers = registry.getOrderedRegisteredEnrichers();
    Assertions.assertEquals(enrichers.size(), 5);

    EnricherInfo enricherInfo5 =
        new EnricherInfo(
            enricher5, TestEnricher5.class, Arrays.asList(enricherInfo2, enricherInfo3), config5);
    Assertions.assertEquals(enrichers.get(0), enricherInfo3);
    Assertions.assertEquals(enrichers.get(1), enricherInfo2);
    Assertions.assertEquals(enrichers.get(2), enricherInfo1);
    Assertions.assertEquals(enrichers.get(3), enricherInfo4);
    Assertions.assertEquals(enrichers.get(4), enricherInfo5);
  }

  @Test
  public void testEnricherTopologicalOrder() {
    // Load enricher config from file
    String configFileName = "enricher.conf";
    String configFilePath =
        Thread.currentThread().getContextClassLoader().getResource(configFileName).getPath();
    Config fileConfig = ConfigFactory.parseFile(new File(configFilePath));
    Config configs = ConfigFactory.load(fileConfig);
    List<String> enrichers = configs.getStringList("enricher.names");
    Map<String, Config> enricherConfigs = new LinkedHashMap<>();
    for (String enricher : enrichers) {
      Config enricherConfig = configs.getConfig(getEnricherConfigPath(enricher));
      enricherConfigs.put(enricher, enricherConfig);
    }

    // Create enrichment register
    EnrichmentRegistry registry = new EnrichmentRegistry();
    registry.registerEnrichers(enricherConfigs);

    // Topologically sort and verify
    List<EnricherInfo> sortedEnrichers = registry.getOrderedRegisteredEnrichers();
    enricherConfigs.forEach((s, config) -> System.out.println("s=" + s));
    sortedEnrichers.forEach(e -> System.out.println(e.getName()));
    Assertions.assertEquals(enricherConfigs.size(), sortedEnrichers.size());
    sortedEnrichers.forEach(
        enricher ->
            enricher
                .getDependencies()
                .forEach(
                    dependencyEnricher ->
                        Assertions.assertTrue(
                            sortedEnrichers.indexOf(dependencyEnricher)
                                < sortedEnrichers.indexOf(enricher),
                            String.format(
                                "%s should be before %s",
                                dependencyEnricher.getName(), enricher.getName()))));
  }
}
