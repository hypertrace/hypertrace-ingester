package org.hypertrace.traceenricher.enrichment;

import com.typesafe.config.Config;
import java.util.List;
import java.util.Objects;

/** Encapsulates the Enricher config along with its dependent enrichers too */
public class EnricherInfo {

  private String name;

  private Class<? extends Enricher> clazz;

  private List<EnricherInfo> dependencies;

  private Config enricherConfig;

  public EnricherInfo(
      String name,
      Class<? extends Enricher> clazz,
      List<EnricherInfo> dependencies,
      Config enricherConfig) {
    this.name = name;
    this.clazz = clazz;
    this.dependencies = dependencies;
    this.enricherConfig = enricherConfig;
  }

  public String getName() {
    return name;
  }

  public Class<? extends Enricher> getClazz() {
    return clazz;
  }

  public Config getEnricherConfig() {
    return enricherConfig;
  }

  public List<EnricherInfo> getDependencies() {
    return dependencies;
  }

  @Override
  public String toString() {
    return "EnricherInfo{"
        + "name='"
        + name
        + '\''
        + ", clazz="
        + clazz
        + ", enricherConfig="
        + enricherConfig
        + ", dependencies="
        + dependencies
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    EnricherInfo that = (EnricherInfo) o;
    return name.equals(that.name)
        && clazz.equals(that.clazz)
        && dependencies.equals(that.dependencies)
        && enricherConfig.equals(that.enricherConfig);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, clazz, dependencies, enricherConfig);
  }
}
