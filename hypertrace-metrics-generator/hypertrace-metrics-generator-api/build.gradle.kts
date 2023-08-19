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
  implementation(libs.google.protobuf.java)
  implementation(platform(libs.hypertrace.kafka.bom))
  implementation("org.apache.kafka:kafka-clients")
  implementation(libs.opentelemetry.proto)

  // Logging
  implementation(libs.slf4j.api)
  runtimeOnly(libs.apache.log4j.slf4jImpl)
}
