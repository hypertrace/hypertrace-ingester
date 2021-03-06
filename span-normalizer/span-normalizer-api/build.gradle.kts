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
    artifact = "com.google.protobuf:protoc:3.15.7"
  }
  plugins {
    id("grpc_java") {
      artifact = "io.grpc:protoc-gen-grpc-java:1.36.1"
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
  api("com.google.api.grpc:proto-google-common-protos:2.1.0")
  api("org.apache.avro:avro:1.10.2")
  constraints {
    api("org.apache.commons:commons-compress:1.21") {
      because("Multiple vulnerabilities in avro-declared version")
    }
  }
}
