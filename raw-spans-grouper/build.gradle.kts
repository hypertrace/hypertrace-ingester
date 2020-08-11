plugins {
  application
  jacoco
  id("org.hypertrace.docker-java-application-plugin") version "0.4.0"
  id("org.hypertrace.docker-publish-plugin") version "0.4.0"
  id("org.hypertrace.jacoco-report-plugin")
}

repositories {
  // Need this to fetch confluent's kafka-avro-serializer dependency
  maven("http://packages.confluent.io/maven")
}

application {
  mainClassName = "org.hypertrace.core.serviceframework.PlatformServiceLauncher"
}

// Config for gw run to be able to run this locally. Just execute gw run here on Intellij or on the console.
tasks.run<JavaExec> {
  jvmArgs = listOf("-Dbootstrap.config.uri=file:${projectDir}/src/main/resources/configs", "-Dservice.name=${project.name}")
}

tasks.test {
  useJUnitPlatform()
}

dependencies {
  implementation("org.hypertrace.core.datamodel:data-model:0.1.2")
  implementation("org.hypertrace.core.flinkutils:flink-utils:0.1.1")
  implementation("org.hypertrace.core.serviceframework:platform-service-framework:0.1.2")

  implementation("com.typesafe:config:1.4.0")
  implementation("de.javakaffee:kryo-serializers:0.45")
  implementation("org.apache.flink:flink-avro:1.7.0")
  implementation("org.apache.flink:flink-streaming-java_2.11:1.7.0")
  implementation("io.confluent:kafka-avro-serializer:5.5.0")

  // Logging
  implementation("org.slf4j:slf4j-api:1.7.30")
  runtimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.13.3")

  testImplementation("org.junit.jupiter:junit-jupiter:5.6.2")
  testImplementation("org.mockito:mockito-core:3.3.3")
}
