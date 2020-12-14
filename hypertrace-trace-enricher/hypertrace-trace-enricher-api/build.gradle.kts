plugins {
  `java-library`
  id("org.hypertrace.publish-plugin")
}

tasks.test {
  useJUnitPlatform()
}

dependencies {
  implementation(project(":hypertrace-trace-enricher:enriched-span-constants"))
  implementation("org.hypertrace.core.datamodel:data-model:0.1.12")

  implementation("org.slf4j:slf4j-api:1.7.30")
  implementation("org.apache.commons:commons-lang3:3.11")
  constraints {
    implementation("com.google.guava:guava:30.0-jre") {
      because("https://snyk.io/vuln/SNYK-JAVA-COMGOOGLEGUAVA-1015415")
    }
  }

  testImplementation("org.junit.jupiter:junit-jupiter:5.7.0")
}
