plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
}

dependencies {
  testImplementation("org.junit.jupiter:junit-jupiter:5.6.2")
}

description = "traceable-raw-span-constants"
