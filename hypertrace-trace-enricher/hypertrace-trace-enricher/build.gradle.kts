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
  implementation(libs.hypertrace.data.model)
  implementation(libs.hypertrace.serviceFramework.framework)
  implementation(libs.hypertrace.serviceFramework.framework)
  implementation(libs.hypertrace.entityService.client)

  implementation(libs.google.guava)
  implementation(libs.hypertrace.kafkaStreams.framework)
  implementation(platform(libs.hypertrace.kafka.bom))

  // Required for the GRPC clients.
  runtimeOnly(libs.grpc.netty)

  // Logging
  implementation(libs.slf4j.api)
  runtimeOnly(libs.apache.log4j.slf4jImpl)

  testImplementation(project(":hypertrace-trace-enricher:hypertrace-trace-enricher"))
  testImplementation(libs.junit.jupiter)
  testImplementation(libs.mockito.core)
  testImplementation(libs.junit.pioneer)
  testImplementation(libs.apache.kafka.kafkaStreamsTestUtils)
}
