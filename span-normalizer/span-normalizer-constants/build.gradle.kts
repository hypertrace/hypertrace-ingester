plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
  id("org.hypertrace.publish-plugin")
}

dependencies {
  testImplementation("org.junit.jupiter:junit-jupiter:5.7.1")
}

description = "traceable-raw-span-constants"
