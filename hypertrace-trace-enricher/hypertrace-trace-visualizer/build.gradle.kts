plugins {
  `java-library`
}

dependencies {
  implementation("org.hypertrace.core.datamodel:data-model:0.1.27")
  constraints {
    implementation("com.google.guava:guava:32.0.1-jre")
  }
  implementation("org.json:json:20230618")
  implementation("org.apache.commons:commons-lang3:3.12.0")
}

description = "Trace Visualizer to help visualize a structured trace."
