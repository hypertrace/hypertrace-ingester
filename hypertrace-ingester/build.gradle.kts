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
      adminPort.set(8099)
    }
  }
}

dependencies {
  implementation("org.hypertrace.core.kafkastreams.framework:kafka-streams-framework:0.1.9")
  implementation("org.hypertrace.core.serviceframework:platform-service-framework:0.1.9")
  implementation("org.hypertrace.core.serviceframework:platform-metrics:0.1.8")
  implementation("org.hypertrace.core.datamodel:data-model:0.1.9")

  implementation("org.hypertrace.core.spannormalizer:span-normalizer")
  implementation("org.hypertrace.core.rawspansgrouper:raw-spans-grouper")
  implementation("org.hypertrace.traceenricher:hypertrace-trace-enricher")
  implementation("org.hypertrace.viewgenerator:hypertrace-view-generator")
  implementation("org.hypertrace.core.viewgenerator:view-generator-framework:0.1.14")

  runtimeOnly("io.netty:netty-codec-http2:4.1.53.Final") {
    because("https://snyk.io/vuln/SNYK-JAVA-IONETTY-1020439")
  }
  runtimeOnly("io.netty:netty-handler-proxy:4.1.53.Final") {
    because("https://snyk.io/vuln/SNYK-JAVA-IONETTY-1020439")
  }
  runtimeOnly("com.google.guava:guava:30.0-android") {
    because("https://snyk.io/vuln/SNYK-JAVA-COMGOOGLEGUAVA-1015415")
  }
  runtimeOnly("org.apache.httpcomponents:httpclient:4.5.13") {
    because("https://snyk.io/vuln/SNYK-JAVA-ORGAPACHEHTTPCOMPONENTS-1016906")
  }
}

// Config for gw run to be able to run this locally. Just execute gw run here on Intellij or on the console.
tasks.run<JavaExec> {
  jvmArgs = listOf("-Dbootstrap.config.uri=file:${project.buildDir}/resources/main/configs", "-Dservice.name=${project.name}")
}

tasks.processResources {
  dependsOn("copyServiceConfigs");
  dependsOn("createCopySpecForSubJob");
}

tasks.register<Copy>("copyServiceConfigs") {
  with(
      createCopySpec("span-normalizer", "span-normalizer"),
      createCopySpec("raw-spans-grouper", "raw-spans-grouper"),
      createCopySpec("hypertrace-trace-enricher", "hypertrace-trace-enricher"),
      createCopySpec("hypertrace-view-generator", "hypertrace-view-generator")
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

tasks.register<Copy>("createCopySpecForSubJob") {
  with(
      createCopySpecForSubJob("hypertrace-view-generator", "hypertrace-view-generator")
  ).into("./build/resources/main/configs/")
}

fun createCopySpecForSubJob(projectName: String, serviceName: String): CopySpec {
  return copySpec {
    from("../${projectName}/${serviceName}/src/main/resources/configs/") {
    }
  }
}
