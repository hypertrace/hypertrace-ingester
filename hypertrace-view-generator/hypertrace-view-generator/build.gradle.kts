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
      serviceName.set("all-views")
      adminPort.set(8099)
    }
  }
}

tasks.test {
  useJUnitPlatform()
}

dependencies {
  implementation(project(":hypertrace-trace-enricher:enriched-span-constants"))
  implementation(project(":hypertrace-trace-enricher:hypertrace-trace-enricher-api"))
  implementation(project(":span-normalizer:raw-span-constants"))
  implementation(project(":hypertrace-view-generator:hypertrace-view-generator-api"))

  // TODO: migrate in core
  implementation("org.hypertrace.core.viewgenerator:view-generator-framework:0.1.25")
  implementation("org.hypertrace.core.datamodel:data-model:0.1.14")
  implementation("org.hypertrace.core.serviceframework:platform-metrics:0.1.23")

  implementation("org.hypertrace.entity.service:entity-service-api:0.6.4")

  implementation("org.apache.avro:avro:1.10.2")
  implementation("org.apache.commons:commons-lang3:3.12.0")

  testImplementation("org.junit.jupiter:junit-jupiter:5.7.1")
  testImplementation("org.mockito:mockito-core:3.8.0")
}
