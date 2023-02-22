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

tasks.test {
  useJUnitPlatform()
}

dependencies {
  constraints {
    implementation("commons-collections:commons-collections:3.2.2") {
      because("https://nvd.nist.gov/vuln/detail/CVE-2015-6420")
    }
  }

  implementation(project(":hypertrace-view-generator:hypertrace-view-generator-api"))
  implementation("org.hypertrace.core.viewcreator:view-creator-framework:0.4.10") {
    // excluding unused but vulnerable tpls
    exclude("org.apache.calcite.avatica")
    exclude("org.apache.calcite")
    exclude("org.apache.pinot", "pinot-avro")
    exclude("org.apache.pinot", "pinot-orc")
    exclude("org.apache.pinot", "pinot-thrift")
    exclude("org.apache.logging.log4j", "log4j-1.2-api")
  }
  // replacement for log4j-1.2
  implementation("ch.qos.reload4j:reload4j:1.2.22")
  testImplementation("org.junit.jupiter:junit-jupiter:5.9.0")
  testImplementation("org.mockito:mockito-core:4.7.0")
}

description = "view creator for Pinot"
