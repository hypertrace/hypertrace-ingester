plugins {
  `java-library`
}

dependencies {
  implementation("org.hypertrace.core.datamodel:data-model:0.1.22")

  implementation("org.json:json:20210307")
  implementation("org.apache.commons:commons-lang3:3.12.0")
}

description = "Trace Visualizer to help visualize a structured trace."
