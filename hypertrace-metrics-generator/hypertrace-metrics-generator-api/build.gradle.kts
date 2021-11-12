plugins {
  `java-library`
  id("org.hypertrace.publish-plugin")
  id("org.hypertrace.avro-plugin")
}

dependencies {
  api("org.apache.avro:avro:1.10.2")
}

hypertraceAvro {
  previousArtifact.set("${project.group}/${project.name}:latest.release")
  relocatedToArtifact.set("${project.group}/${project.name}:latest.release")
}