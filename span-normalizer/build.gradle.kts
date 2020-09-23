plugins {
  java
  application
  jacoco
  id("org.hypertrace.docker-java-application-plugin") version "0.7.1"
  id("org.hypertrace.docker-publish-plugin") version "0.7.1"
  id("org.hypertrace.jacoco-report-plugin")
}

repositories {
  // Need this to fetch confluent's kafka-avro-serializer dependency
  maven("http://packages.confluent.io/maven")
}

application {
  mainClassName = "org.hypertrace.core.serviceframework.PlatformServiceLauncher"
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
  implementation(project(":raw-span-constants"))
  implementation(project(":span-normalizer-api"))

  implementation("org.hypertrace.core.datamodel:data-model:0.1.7")
  implementation("org.hypertrace.core.serviceframework:platform-service-framework:0.1.9")
  implementation("org.hypertrace.core.serviceframework:platform-metrics:0.1.8")
  implementation("org.hypertrace.core.kafkastreams.framework:kafka-streams-framework:0.1.7")


  // Required for the GRPC clients.
  runtimeOnly("io.grpc:grpc-netty:1.31.1")
  implementation("com.typesafe:config:1.4.0")
  implementation("de.javakaffee:kryo-serializers:0.45")

  implementation("io.confluent:kafka-avro-serializer:5.5.1")
  implementation("org.apache.commons:commons-lang3:3.10")

  // Logging
  implementation("org.slf4j:slf4j-api:1.7.30")
  runtimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.13.3")

  testImplementation("org.junit.jupiter:junit-jupiter:5.6.2")
  testImplementation("org.hypertrace.core.serviceframework:platform-metrics:0.1.8")
  testImplementation("org.junit-pioneer:junit-pioneer:0.9.0")
  testImplementation("org.apache.kafka:kafka-streams-test-utils:5.5.1-ccs")
}
