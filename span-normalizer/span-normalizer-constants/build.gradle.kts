plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
  id("org.hypertrace.publish-plugin")
}

dependencies {
  testImplementation(libs.junit.jupiter)
}

description = "traceable-raw-span-constants"
