plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
}

tasks.test {
  useJUnitPlatform()
}

dependencies {
  implementation(project(":hypertrace-trace-enricher:enriched-span-constants"))
  implementation(project(":hypertrace-trace-enricher:hypertrace-trace-enricher-api"))
  implementation(project(":span-normalizer:raw-span-constants"))
  implementation(project(":span-normalizer:span-normalizer-constants"))
  implementation(project(":semantic-convention-utils"))
  implementation(project(":hypertrace-trace-enricher:trace-reader"))

  implementation(libs.hypertrace.data.model)
  implementation(libs.hypertrace.entityService.client)
  implementation(libs.hypertrace.serviceFramework.metrics)
  implementation(libs.hypertrace.grpc.client.utils)
  implementation("org.hypertrace.config.service:spaces-config-service-api:0.1.52")
  implementation(libs.hypertrace.grpc.context.utils)

  implementation(globalLibs.apache.commons.lang3)
  implementation(globalLibs.slf4j.api)
  implementation(globalLibs.uadetector.resources)
  implementation(globalLibs.reactivex.rxjava3)
  implementation(globalLibs.google.guava)

  testImplementation(globalLibs.junit.jupiter)
  testImplementation("org.mockito:mockito-core:4.7.0") // Upgrade to 5.1.0 causes tests to fail
  testImplementation("org.mockito:mockito-junit-jupiter:4.7.0") // Upgrade to 5.1.0 causes tests to fail
  testImplementation(globalLibs.grpc.core)
}
