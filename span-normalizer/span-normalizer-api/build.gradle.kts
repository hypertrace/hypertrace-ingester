import com.google.protobuf.gradle.generateProtoTasks
import com.google.protobuf.gradle.id
import com.google.protobuf.gradle.ofSourceSet
import com.google.protobuf.gradle.plugins
import com.google.protobuf.gradle.protobuf
import com.google.protobuf.gradle.protoc

plugins {
  `java-library`
  id("com.google.protobuf") version "0.8.15"
  id("org.hypertrace.publish-plugin")
  id("org.hypertrace.avro-plugin")
}

val generateLocalGoGrpcFiles = false

protobuf {
  protoc {
    artifact = "com.google.protobuf:protoc:3.21.1"
  }
  plugins {
    id("grpc_java") {
      artifact = "io.grpc:protoc-gen-grpc-java:1.56.0"
    }

    if (generateLocalGoGrpcFiles) {
      id("grpc_go") {
        path = "<go-path>/bin/protoc-gen-go"
      }
    }
  }
  generateProtoTasks {
    ofSourceSet("main").forEach {
      it.plugins {
        // Apply the "grpc" plugin whose spec is defined above, without options.
        id("grpc_java")

        if (generateLocalGoGrpcFiles) {
          id("grpc_go")
        }
      }
      it.builtins {
        java
        if (generateLocalGoGrpcFiles) {
          id("go")
        }
      }
    }
  }
}
sourceSets {
  main {
    java {
      srcDirs("src/main/java", "build/generated/source/proto/main/java", "build/generated/source/proto/main/grpc_java")
    }
  }
}
dependencies {
  api("com.google.api.grpc:proto-google-common-protos:2.14.1")
  api("org.apache.avro:avro:1.11.1")
  constraints {
    api("com.fasterxml.jackson.core:jackson-databind:2.15.2") {
      because("cpe:/a:fasterxml:jackson-databind")
    }
  }
}
