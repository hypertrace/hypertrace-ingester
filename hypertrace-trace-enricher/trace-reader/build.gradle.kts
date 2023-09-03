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
  api(libs.hypertrace.data.model)
  implementation("org.hypertrace.core.attribute.service:attribute-projection-registry:0.14.26")
  implementation(libs.hypertrace.grpc.client.rxUtils)
  implementation(libs.hypertrace.grpc.context.utils)
  implementation(libs.hypertrace.grpc.client.utils)
  implementation(globalLibs.reactivex.rxjava3)
  implementation(globalLibs.google.guava)

  annotationProcessor(globalLibs.projectlombok.lombok)
  compileOnly(globalLibs.projectlombok.lombok)

  testImplementation(globalLibs.junit.jupiter)
  testImplementation(globalLibs.mockito.core)
  testImplementation(globalLibs.mockito.junit.jupiter)
  testRuntimeOnly(globalLibs.apache.log4j.slf4jImpl)

  tasks.test {
    useJUnitPlatform()
  }
}
