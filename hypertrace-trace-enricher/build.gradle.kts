plugins {
  application
  jacoco
  id("org.hypertrace.docker-java-application-plugin")
  id("org.hypertrace.docker-publish-plugin")
  id("org.hypertrace.jacoco-report-plugin")
}

repositories {
  // Needed for io.confluent:kafka-avro-serializer
  maven("http://packages.confluent.io/maven")
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

// Config for gw run to be able to run this locally. Just execute gw run here on Intellij or on the console.
tasks.run<JavaExec> {
  jvmArgs = listOf("-Dbootstrap.config.uri=file:${projectDir}/src/main/resources/configs", "-Dservice.name=${project.name}")
}

tasks.test {
  useJUnitPlatform()
}

dependencies {
  constraints {
    implementation("org.hibernate.validator:hibernate-validator:6.1.5.Final") {
      because("Cross-site Scripting (XSS) [Medium Severity][https://snyk.io/vuln/SNYK-JAVA-ORGHIBERNATEVALIDATOR-541187] in org.hibernate.validator:hibernate-validator@6.0.17.Final\n" +
          "   introduced by io.confluent:kafka-avro-serializer@5.5.0 > io.confluent:kafka-schema-registry-client@5.5.0 > org.glassfish.jersey.ext:jersey-bean-validation@2.30 > org.hibernate.validator:hibernate-validator@6.0.17.Final")
    }
    implementation("org.yaml:snakeyaml:1.26") {
      because("Denial of Service (DoS) [Medium Severity][https://snyk.io/vuln/SNYK-JAVA-ORGYAML-537645] in org.yaml:snakeyaml@1.23\n" +
          "   introduced by io.confluent:kafka-avro-serializer@5.5.0 > io.confluent:kafka-schema-registry-client@5.5.0 > io.swagger:swagger-core@1.5.3 > com.fasterxml.jackson.dataformat:jackson-dataformat-yaml@2.4.5 > org.yaml:snakeyaml@1.12")
    }
  }

  implementation(project(":hypertrace-trace-enricher-impl"))
  implementation("org.hypertrace.core.datamodel:data-model:0.1.4")
  implementation("org.hypertrace.core.flinkutils:flink-utils:0.1.6")
  implementation("org.hypertrace.core.serviceframework:platform-service-framework:0.1.9")
  implementation("org.hypertrace.entity.service:entity-service-client:0.1.20")

  implementation("com.typesafe:config:1.4.0")
  implementation("de.javakaffee:kryo-serializers:0.45")
  implementation("org.apache.flink:flink-avro:1.7.0")
  implementation("org.apache.flink:flink-streaming-java_2.11:1.7.0")
  implementation("io.confluent:kafka-avro-serializer:5.5.0")

  // Required for the GRPC clients.
  runtimeOnly("io.grpc:grpc-netty-shaded:1.31.1")
  // Logging
  implementation("org.slf4j:slf4j-api:1.7.30")
  runtimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.13.3")

  testImplementation("org.junit.jupiter:junit-jupiter:5.6.2")
  testImplementation("org.mockito:mockito-core:3.3.3")
}
