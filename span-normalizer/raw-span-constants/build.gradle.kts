import com.google.protobuf.gradle.id

plugins {
  `java-library`
  id("com.google.protobuf") version "0.9.2"
  id("org.hypertrace.publish-plugin")
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
  api("com.google.protobuf:protobuf-java-util:3.23.3")
  constraints {
    implementation(libs.google.guava)
  }
  implementation(libs.slf4j.api)
}
