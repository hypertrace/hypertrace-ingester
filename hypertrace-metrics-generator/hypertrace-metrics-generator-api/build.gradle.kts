import com.google.protobuf.gradle.protobuf
import com.google.protobuf.gradle.protoc

plugins {
  `java-library`
  id("com.google.protobuf") version "0.8.15"
}

protobuf {
  protoc {
    artifact = "com.google.protobuf:protoc:3.21.1"
  }
}

sourceSets {
  main {
    java {
      srcDirs("src/main/java", "build/generated/source/proto/main/java")
    }
  }
}

dependencies {
  implementation("com.google.protobuf:protobuf-java:3.23.3")
  implementation(platform("org.hypertrace.core.kafkastreams.framework:kafka-bom:0.2.14"))
  implementation("org.apache.kafka:kafka-clients")
  implementation("io.opentelemetry:opentelemetry-proto:1.7.1-alpha")

  // Logging
  implementation("org.slf4j:slf4j-api:1.7.30")
  runtimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.17.1")
}
