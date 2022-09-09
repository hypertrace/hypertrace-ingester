plugins {
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

// Config for gw run to be able to run this locally. Just execute gw run here on Intellij or on the console.
tasks.run<JavaExec> {
  jvmArgs = listOf("-Dbootstrap.config.uri=file:$projectDir/src/main/resources/configs", "-Dservice.name=${project.name}")
}

tasks.test {
  useJUnitPlatform()
}

dependencies {
  implementation(project(":hypertrace-trace-enricher:hypertrace-trace-enricher-impl"))
  implementation(project(":span-normalizer:span-normalizer-api"))
  implementation("org.hypertrace.core.datamodel:data-model:0.1.23")
  implementation("org.hypertrace.core.serviceframework:platform-service-framework:0.1.43")
  implementation("org.hypertrace.core.serviceframework:platform-metrics:0.1.43")
  implementation("org.hypertrace.entity.service:entity-service-client:0.8.5")

  implementation("com.google.guava:guava:31.1-jre")
  implementation("org.hypertrace.core.kafkastreams.framework:kafka-streams-framework:0.2.0")

  // Required for the GRPC clients.
  runtimeOnly("io.grpc:grpc-netty:1.45.1")

  // Logging
  implementation("org.slf4j:slf4j-api:1.7.30")
  runtimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.17.1")

  testImplementation(project(":hypertrace-trace-enricher:hypertrace-trace-enricher"))
  testImplementation("org.junit.jupiter:junit-jupiter:5.9.0")
  testImplementation("org.mockito:mockito-core:3.8.0")
  testImplementation("org.junit-pioneer:junit-pioneer:1.3.8")
  testImplementation("org.apache.kafka:kafka-streams-test-utils:7.2.1-ccs")
}
