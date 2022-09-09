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
    javaApplication {
      serviceName.set("${project.name}")
      adminPort.set(8099)
    }
  }
}

tasks.test {
  useJUnitPlatform()
}

dependencies {
  // common and framework
  implementation(project(":hypertrace-metrics-generator:hypertrace-metrics-generator-api"))
  implementation(project(":hypertrace-view-generator:hypertrace-view-generator-api"))
  implementation("org.hypertrace.core.serviceframework:platform-service-framework:0.1.43")
  implementation("org.hypertrace.core.serviceframework:platform-metrics:0.1.43")
  implementation("org.hypertrace.core.kafkastreams.framework:kafka-streams-framework:0.2.0")

  // open telemetry proto
  implementation("io.opentelemetry:opentelemetry-proto:1.6.0-alpha")

  // all constraints
  constraints {
    implementation("org.glassfish.jersey.core:jersey-common:2.34") {
      because("https://snyk.io/vuln/SNYK-JAVA-ORGGLASSFISHJERSEYCORE-1255637")
    }
    implementation("com.fasterxml.jackson.core:jackson-databind:2.13.2.2") {
      because(
        "Denial of Service (DoS) " +
          "[High Severity][https://snyk.io/vuln/SNYK-JAVA-COMFASTERXMLJACKSONCORE-2421244] " +
          "in com.fasterxml.jackson.core:jackson-databind@2.13.1"
      )
    }
    implementation("com.google.protobuf:protobuf-java:3.21.1") {
      because("https://snyk.io/vuln/SNYK-JAVA-COMGOOGLEPROTOBUF-2331703")
    }
  }

  // test
  testImplementation("org.junit.jupiter:junit-jupiter:5.7.1")
  testImplementation("org.mockito:mockito-core:3.8.0")
  testImplementation("org.junit-pioneer:junit-pioneer:1.3.8")
  testImplementation("org.apache.kafka:kafka-streams-test-utils:7.2.1-ccs")
}
