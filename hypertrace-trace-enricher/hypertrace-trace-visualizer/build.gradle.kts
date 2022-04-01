plugins {
  `java-library`
}

dependencies {
  implementation("org.hypertrace.core.datamodel:data-model:0.1.20")

  implementation("org.json:json:20210307")
  implementation("org.apache.commons:commons-lang3:3.12.0")

  constraints {
    implementation("com.fasterxml.jackson.core:jackson-databind:2.13.2.1") {
      because("Denial of Service (DoS) " +
          "[High Severity][https://snyk.io/vuln/SNYK-JAVA-COMFASTERXMLJACKSONCORE-2421244] in " +
          "com.fasterxml.jackson.core:jackson-databind@2.13.1")
    }
  }
}

description = "Trace Visualizer to help visualize a structured trace."
