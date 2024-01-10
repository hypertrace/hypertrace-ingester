plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
  id("org.hypertrace.publish-plugin")
}

dependencies {
  api(libs.hypertrace.attributeService.api)
  api(libs.hypertrace.entityTypeService.rxClient)
  api(libs.hypertrace.entityDataService.rxClient)
  api(libs.hypertrace.data.model)

  implementation(libs.hypertrace.attributeService.client)
  implementation(libs.hypertrace.attributeService.attributeProjectionRegistry)
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
