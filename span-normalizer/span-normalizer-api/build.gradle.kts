import com.google.protobuf.gradle.*

plugins {
  `java-library`
  id("com.google.protobuf") version "0.8.8"
  id("org.hypertrace.publish-plugin")
  id("org.hypertrace.avro-plugin")
}
//"org.hypertrace.core.spannormalizer"
val generateLocalGoGrpcFiles = false

protobuf {
  protoc {
    artifact = "com.google.protobuf:protoc:3.6.1"
  }
  plugins {
    id("grpc_java") {
      artifact = "io.grpc:protoc-gen-grpc-java:1.15.1"
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
  api("com.google.api.grpc:proto-google-common-protos:1.12.0")
  api( "org.apache.avro:avro:1.9.2")
}

// handle groupId change
hypertraceAvro {
  previousArtifact.set("org.hypertrace.core.spannormalizer:${project.name}:0.1.24")
}

hypertracePublish {
  license.set(org.hypertrace.gradle.publishing.License.APACHE_2_0)
}
