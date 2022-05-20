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
    imageName.set("hypertrace-ingester")
    javaApplication {
      serviceName.set("${project.name}")
      adminPort.set(8099)
    }
    namespace.set("razorpay")
  }
  tag("${project.name}" + "_" + getCommitHash())
}

fun getCommitHash(): String {
  val os = com.bmuschko.gradle.docker.shaded.org.apache.commons.io.output.ByteArrayOutputStream()
  project.exec {
    commandLine = "git rev-parse --verify HEAD".split(" ")
    standardOutput = os
  }
  return String(os.toByteArray()).trim()
}

tasks.test {
  useJUnitPlatform()
}

dependencies {
  // common and framework
  implementation("org.hypertrace.core.serviceframework:platform-service-framework:0.1.33")
  implementation("org.hypertrace.core.serviceframework:platform-metrics:0.1.33")

  // open telemetry
  implementation("io.opentelemetry:opentelemetry-sdk-metrics:1.7.0-alpah")
  // TODO: Upgrade opentelemetry-exporter-prometheus to 1.8.0 release when available
  // to include time stamp related changes
  // https://github.com/open-telemetry/opentelemetry-java/pull/3700
  // For now, the exported time stamp will be the current time stamp.
  implementation("io.opentelemetry:opentelemetry-exporter-prometheus:1.7.0-alpha")

  // open telemetry proto
  implementation("io.opentelemetry:opentelemetry-proto:1.6.0-alpha")

  // kafka
  implementation("org.apache.kafka:kafka-clients:2.7.2")

  // constrains
  constraints {
    implementation("com.fasterxml.jackson.core:jackson-databind:2.13.1") {
      because("Denial of Service (DoS) " +
          "[Medium Severity][https://snyk.io/vuln/SNYK-JAVA-COMFASTERXMLJACKSONCORE-2326698] " +
          "in com.fasterxml.jackson.core:jackson-databind@2.12.2")
    }
  }

  // test
  testImplementation("org.junit.jupiter:junit-jupiter:5.7.1")
  testImplementation("org.mockito:mockito-core:3.8.0")
  testImplementation("com.google.code.gson:gson:2.8.9")
}
