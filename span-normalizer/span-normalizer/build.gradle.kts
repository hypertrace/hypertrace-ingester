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
      adminPort.set(8050)
    }
  }
}

// Config for gw run to be able to run this locally. Just execute gw run here on Intellij or on the console.
tasks.run<JavaExec> {
  jvmArgs = listOf("-Dservice.name=${project.name}")
}

tasks.test {
  useJUnitPlatform()
}

dependencies {
  implementation(project(":span-normalizer:raw-span-constants"))
  implementation(project(":span-normalizer:span-normalizer-api"))
  implementation(project(":span-normalizer:span-normalizer-constants"))
  implementation(project(":semantic-convention-utils"))

  implementation("org.hypertrace.core.datamodel:data-model:0.1.27")
  implementation("org.hypertrace.core.serviceframework:platform-service-framework:0.1.49")
  implementation("org.hypertrace.core.serviceframework:platform-metrics:0.1.49")
  implementation("org.hypertrace.core.kafkastreams.framework:kafka-streams-framework:0.2.6")
  implementation("org.hypertrace.core.kafkastreams.framework:weighted-group-partitioner:0.2.6")
  implementation("org.hypertrace.config.service:span-processing-config-service-api:0.1.47")
  implementation("org.hypertrace.config.service:span-processing-utils:0.1.47")
  implementation("org.hypertrace.core.grpcutils:grpc-client-utils:0.11.2")
  implementation("org.hypertrace.core.grpcutils:grpc-context-utils:0.11.2")
  implementation("com.google.guava:guava:31.1-jre")

  // Required for the GRPC clients.
  runtimeOnly("io.grpc:grpc-netty:1.50.0")
  annotationProcessor("org.projectlombok:lombok:1.18.18")
  compileOnly("org.projectlombok:lombok:1.18.18")

  implementation("de.javakaffee:kryo-serializers:0.45")
  implementation("org.apache.commons:commons-lang3:3.12.0")

  // Logging
  implementation("org.slf4j:slf4j-api:1.7.30")
  runtimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.17.1")

  testImplementation("org.junit.jupiter:junit-jupiter:5.9.0")
  testImplementation("org.hypertrace.core.serviceframework:platform-metrics:0.1.49")
  testImplementation("org.junit-pioneer:junit-pioneer:1.7.1")
  testImplementation("org.mockito:mockito-core:4.7.0")
  testImplementation("org.apache.kafka:kafka-streams-test-utils:7.2.1-ccs")
}
