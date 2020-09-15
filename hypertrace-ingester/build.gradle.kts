plugins {
  java
  application
  jacoco
  id("org.hypertrace.docker-java-application-plugin")
  id("org.hypertrace.docker-publish-plugin")
  id("org.hypertrace.jacoco-report-plugin")
}

application {
  mainClassName = "org.hypertrace.core.serviceframework.PlatformServiceLauncher"
}

hypertraceDocker {
  defaultImage {
    javaApplication {
      serviceName.set("${project.name}")
      adminPort.set(9099)
    }
  }
}

dependencies {
  implementation("org.hypertrace.core.kafkastreams.framework:kafka-streams-framework:0.1.5")
  implementation("org.hypertrace.core.serviceframework:platform-service-framework:0.1.9")
  implementation("org.hypertrace.core.serviceframework:platform-metrics:0.1.8")
  implementation("org.hypertrace.core.datamodel:data-model:0.1.7")

  implementation("org.hypertrace.core.spannormalizer:span-normalizer")
  implementation("org.hypertrace.core.rawspansgrouper:raw-spans-grouper")
  implementation("org.hypertrace.traceenricher:hypertrace-trace-enricher")
}

// Config for gw run to be able to run this locally. Just execute gw run here on Intellij or on the console.
tasks.run<JavaExec> {
  jvmArgs = listOf("-Dbootstrap.config.uri=file:${project.buildDir}/resources/main/configs", "-Dservice.name=${project.name}")
}

tasks.processResources {
  dependsOn("copyServiceConfigs");
}

tasks.register<Copy>("copyServiceConfigs") {
  with(
      createCopySpec("span-normalizer", "span-normalizer"),
      createCopySpec("raw-spans-grouper", "raw-spans-grouper"),
      createCopySpec("hypertrace-trace-enricher", "hypertrace-trace-enricher")
  ).into("./build/resources/main/configs/")
}

fun createCopySpec(projectName: String, serviceName: String): CopySpec {
  return copySpec {
    from("../${projectName}/${serviceName}/src/main/resources/configs/common") {
      include("application.conf")
      into("${serviceName}")
    }
  }
}
