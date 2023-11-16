plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
  id("org.hypertrace.publish-plugin")
}

dependencies {
  api("org.hypertrace.core.attribute.service:attribute-service-api:0.14.26")
  api("org.hypertrace.entity.service:entity-type-service-rx-client:0.8.75")
  api("org.hypertrace.entity.service:entity-data-service-rx-client:0.8.75")
  api(libs.hypertrace.data.model)
  implementation("org.hypertrace.core.attribute.service:attribute-projection-registry:0.14.26")
  implementation(libs.hypertrace.grpc.client.rxUtils)
  implementation(libs.hypertrace.grpc.context.utils)
  implementation(libs.hypertrace.grpc.client.utils)
  implementation(libs.reactivex.rxjava3)
  implementation(libs.google.guava)

  annotationProcessor(libs.projectlombok.lombok)
  compileOnly(libs.projectlombok.lombok)

  testImplementation(libs.junit.jupiter)
  testImplementation(libs.mockito.core)
  testImplementation(libs.mockito.junit.jupiter)
  testRuntimeOnly(libs.apache.log4j.slf4jImpl)

  tasks.test {
    useJUnitPlatform()
  }
}
