plugins {
  java
  application
  jacoco
  id("org.hypertrace.docker-java-application-plugin")
  id("org.hypertrace.docker-publish-plugin")
  id("org.hypertrace.jacoco-report-plugin")
}

application {
  mainClass.set("org.hypertrace.core.serviceframework.PlatformServiceLauncher")
}

hypertraceDocker {
  defaultImage {
    imageName.set("hypertrace-service")
    javaApplication {
      serviceName.set("${project.name}")
      adminPort.set(8099)
    }
    namespace.set("razorpay")
  }
  tag("${project.name}" + "_" + getCommitHash())
}

tasks.test {
  useJUnitPlatform()
}

dependencies {
  implementation(project(":hypertrace-trace-enricher:enriched-span-constants"))
  implementation(project(":hypertrace-trace-enricher:hypertrace-trace-enricher-api"))
  implementation(project(":span-normalizer:raw-span-constants"))
  implementation(project(":hypertrace-view-generator:hypertrace-view-generator-api"))
  implementation(project(":semantic-convention-utils"))

  // TODO: migrate in core
  implementation("org.hypertrace.core.viewgenerator:view-generator-framework:0.3.1")
  implementation("org.hypertrace.core.datamodel:data-model:0.1.19")
  implementation("org.hypertrace.core.serviceframework:platform-metrics:0.1.26")

  implementation("org.hypertrace.entity.service:entity-service-api:0.8.5")

  implementation("org.apache.avro:avro:1.10.2")
  implementation("org.apache.commons:commons-lang3:3.12.0")
  implementation("com.fasterxml.jackson.core:jackson-databind:2.12.2")

  testImplementation("org.junit.jupiter:junit-jupiter:5.7.1")
  testImplementation("org.mockito:mockito-core:3.8.0")
  testImplementation("com.google.code.gson:gson:2.8.7")
}

fun getCommitHash(): String {
  val os = com.bmuschko.gradle.docker.shaded.org.apache.commons.io.output.ByteArrayOutputStream()
  project.exec {
    commandLine = "git rev-parse --verify HEAD".split(" ")
    standardOutput = os
  }
  return String(os.toByteArray()).trim()
}
