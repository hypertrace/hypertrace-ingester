plugins {
  `java-library`
}

dependencies {
  implementation("org.hypertrace.core.datamodel:data-model:0.1.9")

  implementation("org.json:json:20200518")
  implementation("org.apache.commons:commons-lang3:3.10")
  constraints {
    implementation("com.google.guava:guava:30.0-jre") {
      because("https://snyk.io/vuln/SNYK-JAVA-COMGOOGLEGUAVA-1015415")
    }
  }
}

description = "Trace Visualizer to help visualize a structured trace."
