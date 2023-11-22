import com.google.protobuf.gradle.id

plugins {
  `java-library`
  id("com.google.protobuf") version "0.9.2"
  id("org.hypertrace.publish-plugin")
  id("org.hypertrace.avro-plugin")
}

protobuf {
  protoc {
    artifact = "com.google.protobuf:protoc:3.21.12"
  }
  plugins {
    id("grpc") {
      artifact = "io.grpc:protoc-gen-grpc-java:1.57.2"
    }
  }
  generateProtoTasks {
    ofSourceSet("main").configureEach {
      plugins {
        // Apply the "grpc" plugin whose spec is defined above, without options.
        id("grpc")
      }
    }
  }
}

sourceSets {
  main {
    java {
      srcDirs("build/generated/source/proto/main/java", "build/generated/source/proto/main/grpc_java")
    }
  }
}

tasks.test {
  useJUnitPlatform()
}

dependencies {
  api("com.google.api.grpc:proto-google-common-protos:2.14.1")
  api("org.apache.avro:avro:1.11.3")
  constraints {
    api("com.fasterxml.jackson.core:jackson-databind:2.15.2") {
      because("cpe:/a:fasterxml:jackson-databind")
    }
    api("org.apache.commons:commons-compress:1.24.0") {
      because("https://nvd.nist.gov/vuln/detail/CVE-2023-42503")
    }
  }
}
