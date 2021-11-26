plugins {
  `java-library`
  id("org.hypertrace.publish-plugin")
  id("org.hypertrace.avro-plugin")
}

dependencies {
  api("org.apache.avro:avro:1.11.0")

  constraints {
    implementation("org.apache.commons:commons-compress:1.21") {
      because("https://snyk.io/vuln/SNYK-JAVA-ORGAPACHECOMMONS-1316638, " +
          "https://snyk.io/vuln/SNYK-JAVA-ORGAPACHECOMMONS-1316639, " +
          "https://snyk.io/vuln/SNYK-JAVA-ORGAPACHECOMMONS-1316640, " +
          "https://snyk.io/vuln/SNYK-JAVA-ORGAPACHECOMMONS-1316641")
    }
  }
}

tasks.named<org.hypertrace.gradle.avro.CheckAvroCompatibility>("avroCompatibilityCheck") {
  enabled = false
}
