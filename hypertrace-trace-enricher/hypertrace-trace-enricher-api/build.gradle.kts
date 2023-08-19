plugins {
  `java-library`
  id("org.hypertrace.publish-plugin")
}

tasks.test {
  useJUnitPlatform()
}

dependencies {
  implementation(project(":hypertrace-trace-enricher:enriched-span-constants"))
  implementation(libs.hypertrace.data.model)

  implementation(libs.slf4j.api)
  implementation(libs.apache.commons.lang3)
  implementation(libs.google.guava)

  testImplementation(libs.junit.jupiter)
  testImplementation(libs.junit.pioneer)
  testImplementation(libs.mockito.core)
}
