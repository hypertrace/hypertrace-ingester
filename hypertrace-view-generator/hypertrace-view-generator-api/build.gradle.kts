plugins {
  `java-library`
  id("com.commercehub.gradle.plugin.avro")
}

sourceSets {
  main {
    java {
      srcDirs("src/main/java", "build/generated-main-avro-java")
    }
  }
}

dependencies {
  api( "org.apache.avro:avro:1.10.1")
}

