package org.hypertrace.traceenricher.enrichment;

import com.typesafe.config.Config;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Registry of all the Enrichers */
public class EnrichmentRegistry {

  public static final String ENRICHER_CLASS_CONFIG_PATH = "class";

  public static final String ENRICHER_DEPENDENCIES_CONFIG_PATH = "dependencies";

  private static final Logger LOGGER = LoggerFactory.getLogger(EnrichmentRegistry.class);

  private final Map<String, EnricherInfo> registeredEnrichers = new LinkedHashMap<>();

  public EnrichmentRegistry() {}

  /** Register all enrichers contained in enricher config map. */
  public void registerEnrichers(Map<String, Config> enricherConfigs) {
    for (String enricherName : enricherConfigs.keySet()) {
      if (!registeredEnrichers.containsKey(enricherName)) {
        registerEnricher(enricherName, enricherConfigs);
      }
    }
  }

  public List<EnricherInfo> getOrderedRegisteredEnrichers() {
    List<EnricherInfo> enrichers = new ArrayList<>(registeredEnrichers.values());
    return new EnricherGraph(enrichers).topologicalSort();
  }

  /**
   * Register a single enricher that comes from the enricher config map. If successful the it will
   * create an entry (enricherToRegister, EnricherInfo) in registeredEnrichers.
   *
   * @param enricherToRegister The name of enricher to register.
   * @param enricherConfigs The map that contains config of the enricher .
   * @return EnricherInfo if successful, otherwise null.
   */
  private EnricherInfo registerEnricher(
      String enricherToRegister, Map<String, Config> enricherConfigs) {

    // If this enricher has been seen before, just return its EnricherInfo
    if (registeredEnrichers.containsKey(enricherToRegister)) {
      return registeredEnrichers.get(enricherToRegister);
    }

    if (!enricherConfigs.containsKey(enricherToRegister)) {
      LOGGER.error("Failed to register enricher {}. It doesn't have a config.", enricherToRegister);
      return null;
    }

    // At the beginning, mark that the current enricher has been seen, but not yet
    // successfully registered.
    registeredEnrichers.put(enricherToRegister, null);

    Config enricherConfig = enricherConfigs.get(enricherToRegister);
    // First test that enricher class does exist.
    String enricherClassName = enricherConfig.getString(ENRICHER_CLASS_CONFIG_PATH);
    Class<? extends Enricher> enricherClass;
    try {
      enricherClass = Class.forName(enricherClassName).asSubclass(Enricher.class);
    } catch (Exception e) {
      LOGGER.error(
          "Failed to register enricher {}. Enricher class {} cannot be found. Exception: ",
          enricherToRegister,
          enricherClassName,
          e);

      registeredEnrichers.remove(enricherToRegister);
      return null;
    }

    List<EnricherInfo> registeredDependencies = new ArrayList<>();
    // If the enricher has dependencies, go and register its dependencies first.
    if (enricherConfig.hasPath(ENRICHER_DEPENDENCIES_CONFIG_PATH)) {
      List<String> dependencies = enricherConfig.getStringList(ENRICHER_DEPENDENCIES_CONFIG_PATH);

      for (String dependency : dependencies) {
        EnricherInfo registeredDependency = registerEnricher(dependency, enricherConfigs);
        if (registeredDependency == null) {
          LOGGER.error(
              "Failed to register enricher {} because of failure to register its dependency {}.",
              enricherToRegister,
              dependency);

          registeredEnrichers.remove(enricherToRegister);
          return null;
        }
        registeredDependencies.add(registeredDependency);
      }
    }

    // Only register this enricher after all its dependencies have been registered successfully.
    EnricherInfo registeredEnricher =
        new EnricherInfo(enricherToRegister, enricherClass, registeredDependencies, enricherConfig);
    registeredEnrichers.put(enricherToRegister, registeredEnricher);
    return registeredEnricher;
  }

  /**
   * Used for constructing a Graph of Enrichers based on their corresponding dependencies Has the
   * helper method to topologically sort the enrichers
   */
  private static class EnricherGraph {

    private final List<EnricherInfo> nodes;

    private EnricherGraph(List<EnricherInfo> nodes) {
      this.nodes = nodes;
    }

    public List<EnricherInfo> getNodes() {
      return nodes;
    }

    private List<EnricherInfo> topologicalSort() {

      // List where we'll be storing the topological order
      List<EnricherInfo> order = new ArrayList<>();

      // Map which indicates if a node is visited (has been processed by the algorithm)
      Map<EnricherInfo, Boolean> visited = new HashMap<>();
      for (EnricherInfo tmp : getNodes()) {
        visited.put(tmp, false);
      }

      // We go through the nodes
      for (EnricherInfo tmp : getNodes()) {
        if (!visited.get(tmp)) {
          sortUtil(tmp, visited, order);
        }
      }

      return order;
    }

    private void sortUtil(
        EnricherInfo v, Map<EnricherInfo, Boolean> visited, List<EnricherInfo> order) {
      // Mark the current node as visited
      visited.replace(v, true);

      // We reuse the algorithm on all dependencies
      for (EnricherInfo neighborId : v.getDependencies()) {
        if (!visited.get(neighborId)) {
          sortUtil(neighborId, visited, order);
        }
      }

      // Put the current node in the array
      order.add(v);
    }
  }
}
