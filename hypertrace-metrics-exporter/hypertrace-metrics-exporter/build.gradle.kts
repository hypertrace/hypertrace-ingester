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
  implementation(project(":hypertrace-view-generator:hypertrace-view-generator-api"))
  implementation("org.hypertrace.core.serviceframework:platform-service-framework:0.1.30-SNAPSHOT")
  implementation("org.hypertrace.core.serviceframework:platform-metrics:0.1.30-SNAPSHOT")
  implementation("org.hypertrace.core.kafkastreams.framework:kafka-streams-framework:0.1.21")

  // open telemetry
  implementation("io.opentelemetry:opentelemetry-api:1.7.0-SNAPSHOT")
  implementation("io.opentelemetry:opentelemetry-api-metrics:1.7.0-alpha-SNAPSHOT")
  implementation("io.opentelemetry:opentelemetry-sdk:1.7.0-SNAPSHOT")
  implementation("io.opentelemetry:opentelemetry-exporter-otlp-common:1.7.0-SNAPSHOT")
  implementation("io.opentelemetry:opentelemetry-sdk-metrics:1.7.0-alpah-SNAPSHOT")
  implementation("io.opentelemetry:opentelemetry-exporter-otlp-metrics:1.7.0-alpha-SNAPSHOT")
  implementation("io.opentelemetry:opentelemetry-exporter-prometheus:1.7.0-alpha-SNAPSHOT")

  // open telemetry proto
  implementation("io.opentelemetry:opentelemetry-proto:1.6.0-alpha")

  // jetty server
  implementation("org.eclipse.jetty:jetty-server:9.4.42.v20210604")
  implementation("org.eclipse.jetty:jetty-servlet:9.4.42.v20210604")

  // prometheus metrics servelet
  implementation("io.prometheus:simpleclient_servlet:0.12.0")

  // kafka
  implementation("org.apache.kafka:kafka-clients:2.6.0")

  // test
  testImplementation("org.junit.jupiter:junit-jupiter:5.7.1")
  testImplementation("org.mockito:mockito-core:3.8.0")
  testImplementation("com.google.code.gson:gson:2.8.7")
}
