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
  implementation(globalLibs.apache.commons.lang3)
  implementation(globalLibs.google.guava)

  testImplementation(globalLibs.junit.jupiter)
  testImplementation(globalLibs.mockito.core)
}
