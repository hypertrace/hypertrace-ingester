plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
}

tasks.test {
  useJUnitPlatform()
}

dependencies {
  implementation(project(":enriched-span-constants"))
  implementation(project(":hypertrace-trace-enricher-api"))

  implementation("org.hypertrace.core.datamodel:data-model:0.1.9")
  implementation("org.hypertrace.core.spannormalizer:raw-span-constants:0.1.2")
  implementation("org.hypertrace.entity.service:entity-service-client:0.1.20")
  implementation("org.hypertrace.core.serviceframework:platform-metrics:0.1.9")

  implementation("com.typesafe:config:1.4.0")
  implementation("org.apache.httpcomponents:httpclient:4.5.12")
  implementation("org.apache.commons:commons-lang3:3.10")
  implementation("org.slf4j:slf4j-api:1.7.30")
  implementation("net.sf.uadetector:uadetector-resources:2014.10")

  testImplementation("org.junit.jupiter:junit-jupiter:5.6.2")
  testImplementation("org.mockito:mockito-core:3.3.3")
}
