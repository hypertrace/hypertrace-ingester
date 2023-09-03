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

  implementation(globalLibs.slf4j.api)
  implementation(globalLibs.apache.commons.lang3)
  implementation(globalLibs.google.guava)

  testImplementation(globalLibs.junit.jupiter)
  testImplementation(globalLibs.junit.pioneer)
  testImplementation(globalLibs.mockito.core)
}
