plugins {
  `java-library`
}

dependencies {
  implementation("org.hypertrace.core.datamodel:data-model:0.1.12")

  implementation("org.json:json:20201115")
  implementation("org.apache.commons:commons-lang3:3.11")
}

description = "Trace Visualizer to help visualize a structured trace."
