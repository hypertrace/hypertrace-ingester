plugins {
  `java-library`
}

dependencies {
  implementation(libs.hypertrace.data.model)
  constraints {
    implementation(libs.google.guava)
  }
  implementation(libs.json.json)
  implementation(libs.apache.commons.lang3)
}

description = "Trace Visualizer to help visualize a structured trace."
