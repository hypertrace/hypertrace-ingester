package org.hypertrace.core.semantic.convention.constants.deployment;

/** OTEL specific attributes for deployment */
public enum OTelDeploymentSemanticConventions {
  DEPLOYMENT_ENVIRONMENT("deployment.environment");

  private final String value;

  OTelDeploymentSemanticConventions(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }
}
