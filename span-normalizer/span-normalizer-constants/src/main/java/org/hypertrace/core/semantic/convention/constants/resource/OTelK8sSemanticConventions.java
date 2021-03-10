package org.hypertrace.core.semantic.convention.constants.resource;

/** Open telemetry Semantic conventions for kubernetes objects and metadata */
public enum OTelK8sSemanticConventions {
  K8s_CLUSTER_NAME("k8s.cluster.name"),
  K8s_NAMESPACE_NAME("k8s.namespace.name"),
  K8s_POD_UID("k8s.pod.uid"),
  K8s_POD_NAME("k8s.pod.name");

  private final String value;

  OTelK8sSemanticConventions(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }
}
