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
  // internal projects
  implementation(project(":hypertrace-view-generator:hypertrace-view-generator-api"))

  // frameworks
  implementation(libs.hypertrace.serviceFramework.framework)
  implementation(libs.hypertrace.serviceFramework.metrics)
  implementation(libs.hypertrace.kafkaStreams.framework)

  // open telemetry proto
  implementation(libs.opentelemetry.proto)
  implementation(libs.google.protobuf.java)

  // test
  testImplementation(libs.junit.jupiter)
  testImplementation(libs.mockito.core)
  testImplementation(libs.junit.pioneer)
  testImplementation(libs.apache.kafka.kafkaStreamsTestUtils)
}
