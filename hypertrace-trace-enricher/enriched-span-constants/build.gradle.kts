import com.google.protobuf.gradle.generateProtoTasks
import com.google.protobuf.gradle.id
import com.google.protobuf.gradle.ofSourceSet
import com.google.protobuf.gradle.plugins
import com.google.protobuf.gradle.protobuf
import com.google.protobuf.gradle.protoc

plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
  id("com.google.protobuf") version "0.8.15"
  id("org.hypertrace.publish-plugin")
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

tasks.test {
  useJUnitPlatform()
}

sourceSets {
  main {
    java {
      srcDirs("src/main/java", "build/generated/source/proto/main/java", "build/generated/source/proto/main/grpc_java")
    }
  }
}

dependencies {
  api("com.google.protobuf:protobuf-java-util:3.15.7")

  implementation("org.hypertrace.core.datamodel:data-model:0.1.14")
  implementation(project(":span-normalizer:raw-span-constants"))
  implementation(project(":span-normalizer:span-normalizer-constants"))
  implementation("org.hypertrace.entity.service:entity-service-api:0.6.4")

  testImplementation("org.junit.jupiter:junit-jupiter:5.7.1")
  testImplementation("org.mockito:mockito-core:3.8.0")
}
