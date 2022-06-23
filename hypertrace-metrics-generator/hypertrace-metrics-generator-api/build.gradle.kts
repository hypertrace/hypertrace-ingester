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
  implementation("com.google.protobuf:protobuf-java:3.21.1")
  implementation("org.apache.kafka:kafka-clients:6.0.1-ccs")
  implementation("io.opentelemetry:opentelemetry-proto:1.6.0-alpha")
}
