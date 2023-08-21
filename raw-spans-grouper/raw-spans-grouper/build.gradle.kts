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
      adminPort.set(8051)
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
  implementation("org.glassfish.jersey.core:jersey-common:2.34") {
    because("https://snyk.io/vuln/SNYK-JAVA-ORGGLASSFISHJERSEYCORE-1255637")
  }
  implementation(project(":span-normalizer:span-normalizer-api"))
  implementation(libs.hypertrace.data.model)
  implementation(libs.hypertrace.serviceFramework.framework)
  implementation(libs.hypertrace.serviceFramework.metrics)

  implementation(libs.hypertrace.kafkaStreams.framework)
  implementation(libs.hypertrace.kafkaStreams.weightedGroupPartitioners)
  implementation("de.javakaffee:kryo-serializers:0.45")
  implementation(globalLibs.google.guava)

  // Required for the GRPC clients.
  runtimeOnly(globalLibs.grpc.netty)

  // Logging
  implementation(globalLibs.slf4j.api)
  runtimeOnly(globalLibs.apache.log4j.slf4jImpl)

  testImplementation(globalLibs.junit.jupiter)
  testImplementation(globalLibs.mockito.core)
  testImplementation(globalLibs.junit.pioneer)
  testImplementation(globalLibs.apache.kafka.kafkaStreamsTestUtils)
}
