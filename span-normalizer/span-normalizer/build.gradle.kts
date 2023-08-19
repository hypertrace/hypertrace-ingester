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

  implementation(libs.hypertrace.data.model)
  implementation(libs.hypertrace.serviceFramework.framework)
  implementation(libs.hypertrace.serviceFramework.metrics)
  implementation(libs.hypertrace.kafkaStreams.framework)
  implementation(libs.hypertrace.kafkaStreams.weightedGroupPartitioners)
  implementation("org.hypertrace.config.service:span-processing-config-service-api:0.1.52")
  implementation("org.hypertrace.config.service:span-processing-utils:0.1.52")
  implementation(libs.hypertrace.grpc.client.utils)
  implementation(libs.hypertrace.grpc.context.utils)
  implementation(libs.google.guava)

  // Required for the GRPC clients.
  runtimeOnly(libs.grpc.netty)
  annotationProcessor(libs.projectlombok.lombok)
  compileOnly(libs.projectlombok.lombok)

  implementation("de.javakaffee:kryo-serializers:0.45")
  implementation(libs.apache.commons.lang3)

  // Logging
  implementation(libs.slf4j.api)
  runtimeOnly(libs.apache.log4j.slf4jImpl)

  testImplementation(libs.junit.jupiter)
  testImplementation(libs.hypertrace.serviceFramework.metrics)
  testImplementation(libs.junit.pioneer)
  testImplementation(libs.mockito.core)
  testImplementation(libs.apache.kafka.kafkaStreamsTestUtils)
}
