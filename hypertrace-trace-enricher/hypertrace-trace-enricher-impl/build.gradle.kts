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
  implementation(libs.hypertrace.entityService.changeEventApi)
  implementation(libs.hypertrace.attributeService.client)
  implementation(libs.hypertrace.serviceFramework.metrics)
  implementation(libs.hypertrace.grpc.client.utils)
  implementation(libs.hypertrace.spacesConfigServiceApi)
  implementation(libs.hypertrace.grpc.context.utils)
  implementation(libs.hypertrace.kafkaStreams.eventListener)

  implementation(libs.apache.commons.lang3)
  implementation(libs.slf4j.api)
  implementation(libs.uadetector.resources)
  implementation(libs.reactivex.rxjava3)
  implementation(libs.google.guava)
  implementation(libs.kafka.protobufserde)

  testImplementation(libs.junit.jupiter)
  testImplementation("org.mockito:mockito-core:4.7.0") // Upgrade to 5.1.0 causes tests to fail
  testImplementation("org.mockito:mockito-junit-jupiter:4.7.0") // Upgrade to 5.1.0 causes tests to fail
  testImplementation(libs.grpc.core)
}
