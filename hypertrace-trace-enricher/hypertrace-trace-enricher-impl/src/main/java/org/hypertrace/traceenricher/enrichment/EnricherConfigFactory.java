package org.hypertrace.traceenricher.enrichment;

import com.typesafe.config.Config;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class EnricherConfigFactory {
  private static final String ENRICHER_NAMES_CONFIG = "enricher.names";
  public static final String ENRICHER_CONFIG_TEMPLATE = "enricher.%s";

  private EnricherConfigFactory() {
  }

  /**
   * Creates a map of enricher name to enricher configurations
   * The configuration format matches with {@code}structured-trace-enrichment-jobs.conf{code},
   *
   * @param configs raw configuration
   * @return map of enricher name to enricher configurations. Returns empty map if there is
   * any failure, never return null
   */
  public static Map<String, Config> createEnricherConfig(Config configs) {
    Map<String, Config> result = new LinkedHashMap<>();
    if (!configs.hasPath(ENRICHER_NAMES_CONFIG)) {
      return result;
    }

    List<String> enrichers = configs.getStringList(ENRICHER_NAMES_CONFIG);

    for (String enricher : enrichers) {
      Config enricherConfig = configs.getConfig(getEnricherConfigPath(enricher));
      result.put(enricher, enricherConfig);
    }
    return result;
  }

  public static String getEnricherConfigPath(String enricher) {
    return String.format(ENRICHER_CONFIG_TEMPLATE, enricher);
  }

}
