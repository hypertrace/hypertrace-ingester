import com.google.protobuf.gradle.id

plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
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
  api(libs.google.protobuf.java)

  implementation(libs.hypertrace.data.model)
  implementation(project(":span-normalizer:raw-span-constants"))
  implementation(project(":span-normalizer:span-normalizer-constants"))
  implementation(project(":semantic-convention-utils"))
  implementation(libs.hypertrace.entityService.api)
  implementation(libs.google.guava)

  testImplementation(libs.junit.jupiter)
  testImplementation(libs.mockito.core)
}
