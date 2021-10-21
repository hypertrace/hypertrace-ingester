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
  implementation("org.hypertrace.core.serviceframework:platform-service-framework:0.1.26")
  implementation("org.hypertrace.core.serviceframework:platform-metrics:0.1.26")
  implementation("org.hypertrace.core.kafkastreams.framework:kafka-streams-framework:0.1.21")

  // open telemetry proto
  implementation("io.opentelemetry:opentelemetry-proto:1.6.0-alpha")

  // test
  testImplementation("org.junit.jupiter:junit-jupiter:5.7.1")
  testImplementation("org.mockito:mockito-core:3.8.0")
  testImplementation("org.junit-pioneer:junit-pioneer:1.3.8")
  testImplementation("org.apache.kafka:kafka-streams-test-utils:6.0.1-ccs")
}