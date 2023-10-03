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
  implementation(project(":semantic-convention-utils"))
  implementation(libs.google.guava)

  // TODO: migrate in core
  implementation(libs.hypertrace.viewCreator.framework)
  implementation(libs.hypertrace.data.model)
  implementation(libs.hypertrace.serviceFramework.metrics)

  implementation(libs.hypertrace.entityService.api)

  implementation("org.apache.avro:avro:1.11.3")
  implementation(libs.apache.commons.lang3)

  // logging
  runtimeOnly(libs.apache.log4j.slf4jImpl)

  testImplementation(libs.junit.jupiter)
  testImplementation(libs.mockito.core)
  testImplementation(libs.google.gson)
}
