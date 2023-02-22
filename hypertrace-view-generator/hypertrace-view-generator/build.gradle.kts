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
    javaApplication {
      serviceName.set("all-views")
      adminPort.set(8099)
    }
  }
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
  implementation("com.google.guava:guava:31.1-jre")

  // TODO: migrate in core
  implementation("org.hypertrace.core.viewgenerator:view-generator-framework:0.4.10")
  implementation("org.hypertrace.core.datamodel:data-model:0.1.27")
  implementation("org.hypertrace.core.serviceframework:platform-metrics:0.1.49")

  implementation("org.hypertrace.entity.service:entity-service-api:0.8.5")

  implementation("org.apache.avro:avro:1.11.1")
  constraints {
    implementation("com.fasterxml.jackson.core:jackson-databind:2.14.2") {
      because("version 2.12.7.1 has a vulnerability https://snyk.io/vuln/SNYK-JAVA-COMFASTERXMLJACKSONCORE-3038424")
    }
  }
  implementation("org.apache.commons:commons-lang3:3.12.0")

  // logging
  runtimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.17.1")

  testImplementation("org.junit.jupiter:junit-jupiter:5.9.0")
  testImplementation("org.mockito:mockito-core:4.7.0")
  testImplementation("com.google.code.gson:gson:2.9.0")
}
