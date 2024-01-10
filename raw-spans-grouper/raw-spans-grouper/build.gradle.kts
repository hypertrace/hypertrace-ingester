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
  implementation(project(":semantic-convention-utils"))
  implementation(libs.hypertrace.data.model)
  implementation(libs.hypertrace.serviceFramework.framework)
  implementation(libs.hypertrace.serviceFramework.metrics)
  implementation(libs.hypertrace.grpc.client.utils)

  implementation(libs.hypertrace.kafkaStreams.framework)
  implementation(libs.hypertrace.kafkaStreams.weightedGroupPartitioners)
  implementation("de.javakaffee:kryo-serializers:0.45")
  implementation(libs.google.guava)

  // Required for the GRPC clients.
  runtimeOnly(libs.grpc.netty)

  // Logging
  implementation(libs.slf4j.api)
  runtimeOnly(libs.apache.log4j.slf4jImpl)

  testImplementation(libs.junit.jupiter)
  testImplementation(libs.mockito.core)
  testImplementation(libs.junit.pioneer)
  testImplementation(libs.apache.kafka.kafkaStreamsTestUtils)
}
