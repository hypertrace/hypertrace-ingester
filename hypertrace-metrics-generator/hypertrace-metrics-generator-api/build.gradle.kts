plugins {
  `java-library`
  id("org.hypertrace.publish-plugin")
  id("org.hypertrace.avro-plugin")
}

dependencies {
  api("org.apache.avro:avro:1.10.2")
}
