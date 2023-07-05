plugins {
  `java-library`
  id("org.hypertrace.publish-plugin")
  id("com.github.davidmc24.gradle.plugin.avro")
}

sourceSets {
  main {
    java {
      srcDirs("src/main/java", "build/generated-main-avro-java")
    }
  }
}

dependencies {
  api("org.apache.avro:avro")
  api(platform("org.hypertrace.core.kafkastreams.framework:kafka-bom:0.2.14"))
}
