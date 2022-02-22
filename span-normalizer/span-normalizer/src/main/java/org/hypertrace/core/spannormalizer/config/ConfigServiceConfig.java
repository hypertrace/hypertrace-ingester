package org.hypertrace.core.spannormalizer.config;

import com.typesafe.config.Config;

public class ConfigServiceConfig {
  private final Config config;

  public ConfigServiceConfig(Config config) {
    this.config = config.getConfig("clients.config.service.config");
  }

  public String getConfigServiceHost() {
    return this.config.getString("host");
  }

  public Integer getConfigServicePort() {
    return this.config.getInt("port");
  }
}
