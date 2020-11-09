plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
  id("org.hypertrace.publish-plugin")
}

dependencies {
  implementation("org.hypertrace.core.datamodel:data-model:0.1.9")
  implementation("org.hypertrace.core.attribute.service:attribute-service-api:0.6.1")
  implementation("org.hypertrace.core.attribute.service:caching-attribute-service-client:0.6.1")
  implementation("org.hypertrace.core.attribute.service:attribute-projection-registry:0.6.1")
  implementation("io.reactivex.rxjava3:rxjava:3.0.6")

  testImplementation("org.junit.jupiter:junit-jupiter:5.6.2")
  testImplementation("org.mockito:mockito-core:3.3.3")
  testImplementation("org.mockito:mockito-junit-jupiter:3.3.3")
  testRuntimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.13.3")

  tasks.test {
    useJUnitPlatform()
  }
}