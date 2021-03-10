package org.hypertrace.core.semantic.convention.constants.resource;

/** Open telemetry container semantic conventions */
public enum OTelContainerSemanticConventions {
  CONTAINER_NAME("container.name"),
  CONTAINER_ID("container.id");

  private final String value;

  OTelContainerSemanticConventions(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }
}
