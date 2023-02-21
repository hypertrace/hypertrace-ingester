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

  implementation("org.hypertrace.core.datamodel:data-model:0.1.25")
  implementation("org.hypertrace.entity.service:entity-service-client:0.8.5")
  implementation("org.hypertrace.core.serviceframework:platform-metrics:0.1.48")
  implementation("org.hypertrace.core.grpcutils:grpc-client-utils:0.11.2")
  implementation("org.hypertrace.config.service:spaces-config-service-api:0.1.47")
  implementation("org.hypertrace.core.grpcutils:grpc-context-utils:0.11.2")

  implementation("org.apache.commons:commons-lang3:3.12.0")
  implementation("org.slf4j:slf4j-api:1.7.30")
  implementation("net.sf.uadetector:uadetector-resources:2014.10")
  implementation("io.reactivex.rxjava3:rxjava:3.0.11")
  implementation("com.google.guava:guava:31.1-jre")

  testImplementation("org.junit.jupiter:junit-jupiter:5.9.0")
  testImplementation("org.mockito:mockito-core:4.7.0")
  testImplementation("org.mockito:mockito-junit-jupiter:4.7.0")
  testImplementation("io.grpc:grpc-core:1.50.0")
}
