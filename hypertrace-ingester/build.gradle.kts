plugins {
  java
  application
  jacoco
  id("org.hypertrace.docker-java-application-plugin")
  id("org.hypertrace.docker-publish-plugin")
  id("org.hypertrace.jacoco-report-plugin")
}

subprojects {
  group = "org.hypertrace.ingester"
}

application {
  mainClass.set("org.hypertrace.core.serviceframework.PlatformServiceLauncher")
}

hypertraceDocker {
  defaultImage {
    javaApplication {
      serviceName.set("${project.name}")
      adminPort.set(8099)
    }
  }
}

dependencies {
  implementation("org.hypertrace.core.kafkastreams.framework:kafka-streams-framework:0.1.21")
  implementation("org.hypertrace.core.serviceframework:platform-service-framework:0.1.30")
  implementation("org.hypertrace.core.serviceframework:platform-metrics:0.1.30")
  implementation("org.hypertrace.core.datamodel:data-model:0.1.20")
  implementation("org.hypertrace.core.viewgenerator:view-generator-framework:0.3.1")
  implementation("com.typesafe:config:1.4.1")
  implementation("org.apache.commons:commons-lang3:3.12.0")

  implementation(project(":span-normalizer:span-normalizer-constants"))
  implementation(project(":span-normalizer:raw-span-constants"))
  implementation(project(":semantic-convention-utils"))
  implementation(project(":span-normalizer:span-normalizer"))
  implementation(project(":raw-spans-grouper:raw-spans-grouper"))
  implementation(project(":hypertrace-trace-enricher:hypertrace-trace-enricher"))
  implementation(project(":hypertrace-view-generator:hypertrace-view-generator"))
  implementation(project(":hypertrace-metrics-processor:hypertrace-metrics-processor"))
  implementation(project(":hypertrace-metrics-exporter:hypertrace-metrics-exporter"))

  testImplementation("org.junit.jupiter:junit-jupiter:5.7.1")
  testImplementation("org.mockito:mockito-core:3.8.0")
  testImplementation("org.junit-pioneer:junit-pioneer:1.3.8")
  testImplementation("org.apache.kafka:kafka-streams-test-utils:6.0.1-ccs")
  testImplementation(project(":hypertrace-view-generator:hypertrace-view-generator-api"))
  testImplementation(project(":span-normalizer:span-normalizer-api"))
}

// Config for gw run to be able to run this locally. Just execute gw run here on Intellij or on the console.
tasks.run<JavaExec> {
  jvmArgs = listOf("-Dbootstrap.config.uri=file:${project.buildDir}/resources/main/configs", "-Dservice.name=${project.name}")
}

tasks.processResources {
  dependsOn("copyServiceConfigs")
  dependsOn("createCopySpecForSubJob")
}

tasks.register<Copy>("copyServiceConfigs") {
  with(
      createCopySpec("span-normalizer",
        "span-normalizer",
        "main",
        "common"),
      createCopySpec("raw-spans-grouper",
        "raw-spans-grouper",
        "main",
        "common"),
      createCopySpec("hypertrace-trace-enricher",
        "hypertrace-trace-enricher",
        "main",
        "common"),
      createCopySpec("hypertrace-view-generator",
        "hypertrace-view-generator",
        "main",
        "common"),
      createCopySpec("hypertrace-metrics-processor",
        "hypertrace-metrics-processor",
        "main",
        "common"),
      createCopySpec("hypertrace-metrics-exporter",
      "hypertrace-metrics-exporter",
      "main",
      "common")
  ).into("./build/resources/main/configs/")
}

fun createCopySpec(projectName: String, serviceName: String, srcFolder: String, configFolder: String): CopySpec {
  return copySpec {
    from("../$projectName/$serviceName/src/$srcFolder/resources/configs/$configFolder") {
      include("application.conf")
      into("$serviceName")
    }
  }
}

tasks.register<Copy>("createCopySpecForSubJob") {
  with(
      createCopySpecForSubJob("hypertrace-view-generator", "hypertrace-view-generator", "main")
  ).into("./build/resources/main/configs/")
}

fun createCopySpecForSubJob(projectName: String, serviceName: String, srcFolder: String): CopySpec {
  return copySpec {
    from("../$projectName/$serviceName/src/$srcFolder/resources/configs/") {
    }
  }
}

tasks.test {
  useJUnitPlatform()
  /**
   * Copy config for respective kafka streams topology under resource
   */
  dependsOn("copyServiceConfigsTest")
  dependsOn("createCopySpecForSubJobTest")
}

tasks.register<Copy>("copyServiceConfigsTest") {
  with(
      createCopySpec("span-normalizer",
        "span-normalizer",
        "test",
        "span-normalizer"),
      createCopySpec("raw-spans-grouper",
        "raw-spans-grouper",
        "test",
        "raw-spans-grouper"),
      createCopySpec("hypertrace-trace-enricher",
        "hypertrace-trace-enricher",
        "test",
        "hypertrace-trace-enricher"),
      createCopySpec("hypertrace-view-generator",
        "hypertrace-view-generator",
        "test",
        "hypertrace-view-generator"),
      createCopySpec("hypertrace-metrics-processor",
        "hypertrace-metrics-processor",
        "test",
        "hypertrace-metrics-processor"),
      createCopySpec("hypertrace-metrics-exporter",
      "hypertrace-metrics-exporter",
      "test",
      "hypertrace-metrics-exporter")
  ).into("./build/resources/test/configs/")
}

tasks.register<Copy>("createCopySpecForSubJobTest") {
  with(
          createCopySpecForSubJob("hypertrace-view-generator", "hypertrace-view-generator", "test")
  ).into("./build/resources/test/configs/")
}
