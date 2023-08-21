plugins {
  `java-library`
}

dependencies {
  implementation(libs.hypertrace.data.model)
  constraints {
    implementation(globalLibs.google.guava)
  }
  implementation(globalLibs.json.json)
  implementation(globalLibs.apache.commons.lang3)
}

description = "Trace Visualizer to help visualize a structured trace."
