plugins {
  java
  application
  jacoco
  id("org.hypertrace.docker-java-application-plugin")
  id("org.hypertrace.docker-publish-plugin")
  id("org.hypertrace.jacoco-report-plugin")
}

application {
  mainClass.set("org.hypertrace.core.serviceframework.PlatformServiceLauncher")
}

hypertraceDocker {
  defaultImage {
    javaApplication {
      serviceName.set("${project.name}")
      adminPort.set(8099)
    }
  }
}

tasks.test {
  useJUnitPlatform()
}

dependencies {
  // common and framework
  implementation(libs.hypertrace.serviceFramework.framework)
  implementation(libs.hypertrace.serviceFramework.metrics)

  // open telemetry
  implementation(libs.opentelemetry.sdk.metrics)
  // TODO: Upgrade opentelemetry-exporter-prometheus to 1.8.0 release when available
  // to include time stamp related changes
  // https://github.com/open-telemetry/opentelemetry-java/pull/3700
  // For now, the exported time stamp will be the current time stamp.
  implementation(libs.opentelemetry.exporter.prometheus)
  implementation(libs.google.protobuf.java)

  // open telemetry proto
  implementation(libs.opentelemetry.proto)

  // kafka
  implementation(platform(libs.hypertrace.kafka.bom))
  implementation("org.apache.kafka:kafka-clients")

  // test
  testImplementation(libs.junit.jupiter)
  testImplementation(libs.mockito.core)
  testImplementation(libs.google.gson)
}
