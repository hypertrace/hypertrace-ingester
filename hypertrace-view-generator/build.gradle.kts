plugins {
  java
  application
  jacoco
  id("org.hypertrace.docker-java-application-plugin")
  id("org.hypertrace.docker-publish-plugin")
  id("org.hypertrace.jacoco-report-plugin")
}

application {
  mainClassName = "org.hypertrace.core.serviceframework.PlatformServiceLauncher"
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
  implementation(project(":hypertrace-view-generator-api"))
  implementation("org.hypertrace.core.viewgenerator:view-generator-framework:0.1.11")
  implementation("org.hypertrace.core.datamodel:data-model:0.1.8")
  implementation("org.hypertrace.core.serviceframework:platform-metrics:0.1.9")

  implementation("org.hypertrace.traceenricher:enriched-span-constants:0.1.3")
  implementation("org.hypertrace.traceenricher:hypertrace-trace-enricher-api:0.1.22")
  implementation("org.hypertrace.core.spannormalizer:raw-span-constants:0.1.5")
  implementation("org.hypertrace.entity.service:entity-service-api:0.1.3")

  implementation("org.apache.avro:avro:1.9.2")
  implementation("org.apache.commons:commons-lang3:3.10")

  testImplementation("org.junit.jupiter:junit-jupiter:5.6.2")
  testImplementation("org.mockito:mockito-core:3.3.3")
}
