plugins {
  `java-library`
}

dependencies {
  implementation("org.hypertrace.core.datamodel:data-model:0.1.12")

  implementation("org.json:json:20201115")
  implementation("org.apache.commons:commons-lang3:3.11")
  constraints {
    implementation("com.google.guava:guava:30.0-jre") {
      because("https://snyk.io/vuln/SNYK-JAVA-COMGOOGLEGUAVA-1015415")
    }
  }
}

description = "Trace Visualizer to help visualize a structured trace."
