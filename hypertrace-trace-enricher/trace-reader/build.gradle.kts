plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
  id("org.hypertrace.publish-plugin")
}

dependencies {
  api("org.hypertrace.core.attribute.service:attribute-service-api:0.14.26")
  api("org.hypertrace.core.attribute.service:caching-attribute-service-client:0.14.26")
  api("org.hypertrace.entity.service:entity-type-service-rx-client:0.8.75")
  api("org.hypertrace.entity.service:entity-data-service-rx-client:0.8.75")
  api("org.hypertrace.core.datamodel:data-model:0.1.27")
  implementation("org.hypertrace.core.attribute.service:attribute-projection-registry:0.14.26")
  implementation("org.hypertrace.core.grpcutils:grpc-client-rx-utils:0.12.2")
  implementation("org.hypertrace.core.grpcutils:grpc-context-utils:0.12.2")
  implementation("org.hypertrace.core.grpcutils:grpc-client-utils:0.12.2")
  implementation("io.reactivex.rxjava3:rxjava:3.0.11")
  implementation("com.google.guava:guava:32.0.1-jre")

  annotationProcessor("org.projectlombok:lombok:1.18.20")
  compileOnly("org.projectlombok:lombok:1.18.20")

  testImplementation("org.junit.jupiter:junit-jupiter:5.9.0")
  testImplementation("org.mockito:mockito-inline:4.7.0")
  testImplementation("org.mockito:mockito-junit-jupiter:4.7.0")
  testRuntimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.17.1")

  tasks.test {
    useJUnitPlatform()
  }
}
