plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
}

tasks.test {
  useJUnitPlatform()
}

dependencies {
  implementation(project(":hypertrace-trace-enricher:enriched-span-constants"))
  implementation(project(":hypertrace-trace-enricher:hypertrace-trace-enricher-api"))
  implementation(project(":span-normalizer:raw-span-constants"))
  implementation(project(":span-normalizer:span-normalizer-constants"))
  implementation(project(":semantic-convention-utils"))
  implementation(project(":hypertrace-trace-enricher:trace-reader"))

  implementation("org.hypertrace.core.datamodel:data-model:0.1.18")
  implementation("org.hypertrace.entity.service:entity-service-client:0.8.4")
  implementation("org.hypertrace.core.serviceframework:platform-metrics:0.1.28")
  implementation("org.hypertrace.core.grpcutils:grpc-client-utils:0.5.2")
  implementation("org.hypertrace.config.service:spaces-config-service-api:0.1.0")
  implementation("org.hypertrace.core.grpcutils:grpc-context-utils:0.5.2")

  implementation("com.typesafe:config:1.4.1")
  implementation("org.apache.httpcomponents:httpclient:4.5.13")
  implementation("org.apache.commons:commons-lang3:3.12.0")
  implementation("org.slf4j:slf4j-api:1.7.30")
  implementation("net.sf.uadetector:uadetector-resources:2014.10")
  implementation("io.reactivex.rxjava3:rxjava:3.0.11")

  testImplementation("org.junit.jupiter:junit-jupiter:5.7.1")
  testImplementation("org.mockito:mockito-core:3.8.0")
  testImplementation("org.mockito:mockito-junit-jupiter:3.8.0")
  testImplementation("io.grpc:grpc-core:1.36.1")
}
