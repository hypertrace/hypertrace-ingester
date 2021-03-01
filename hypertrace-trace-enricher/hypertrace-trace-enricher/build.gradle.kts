plugins {
  application
  jacoco
  id("org.hypertrace.docker-java-application-plugin")
  id("org.hypertrace.docker-publish-plugin")
  id("org.hypertrace.jacoco-report-plugin")
}

repositories {
  // Needed for io.confluent:kafka-avro-serializer
  maven("http://packages.confluent.io/maven")
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

// Config for gw run to be able to run this locally. Just execute gw run here on Intellij or on the console.
tasks.run<JavaExec> {
  jvmArgs = listOf("-Dbootstrap.config.uri=file:${projectDir}/src/main/resources/configs", "-Dservice.name=${project.name}")
}

tasks.test {
  useJUnitPlatform()
}

dependencies {
  implementation(project(":hypertrace-trace-enricher:hypertrace-trace-enricher-impl"))
  implementation("org.hypertrace.core.datamodel:data-model:0.1.12")
  implementation("org.hypertrace.core.serviceframework:platform-service-framework:0.1.21")
  implementation("org.hypertrace.core.serviceframework:platform-metrics:0.1.21")
  implementation("org.hypertrace.entity.service:entity-service-client:0.1.23")

  implementation("com.typesafe:config:1.4.1")
  implementation("org.hypertrace.core.kafkastreams.framework:kafka-streams-framework:0.1.18")
  constraints {
    implementation("com.google.guava:guava:30.1-jre") {
      because("Information Disclosure [Medium Severity][https://snyk.io/vuln/SNYK-JAVA-COMGOOGLEGUAVA-1015415] in com.google.guava:guava@29.0-android")
    }
  }

  // Required for the GRPC clients.
  runtimeOnly("io.grpc:grpc-netty-shaded:1.35.0")

  // Logging
  implementation("org.slf4j:slf4j-api:1.7.30")
  runtimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.14.0")

  testImplementation(project(":hypertrace-trace-enricher:hypertrace-trace-enricher"))
  testImplementation("org.junit.jupiter:junit-jupiter:5.7.0")
  testImplementation("org.mockito:mockito-core:3.6.28")
  testImplementation("org.junit-pioneer:junit-pioneer:1.1.0")
  testImplementation("org.apache.kafka:kafka-streams-test-utils:6.0.1-ccs")
}
