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
  api("org.apache.avro:avro:1.11.1")
  constraints {
    api("com.fasterxml.jackson.core:jackson-databind:2.15.2") {
      because("cpe:/a:fasterxml:jackson-databind")
    }
  }
}
