plugins {
  `java-library`
}

dependencies {
  implementation("org.hypertrace.core.datamodel:data-model:0.1.3")

  implementation("org.json:json:20200518")
  implementation("org.apache.commons:commons-lang3:3.10")
}

description = "Trace Visualizer to help visualize a structured trace."
