plugins {
  `java-library`
  id("com.commercehub.gradle.plugin.avro") version "0.9.1"
}

sourceSets {
  main {
    java {
      srcDirs("src/main/java", "build/generated-main-avro-java")
    }
  }
}

dependencies {
  api( "org.apache.avro:avro:1.9.2")
}

