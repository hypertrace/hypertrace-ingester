plugins {
  `java-library`
  id("org.hypertrace.publish-plugin")
}

tasks.test {
  useJUnitPlatform()
}

dependencies {
  implementation(project(":span-normalizer:raw-span-constants"))
  implementation(project(":span-normalizer:span-normalizer-constants"))

  implementation(libs.hypertrace.data.model)
  implementation(libs.apache.commons.lang3)
  implementation(libs.google.guava)

  testImplementation(libs.junit.jupiter)
  testImplementation(libs.mockito.core)
}
