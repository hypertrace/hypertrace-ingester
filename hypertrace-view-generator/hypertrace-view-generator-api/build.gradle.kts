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
  api("org.apache.avro:avro:1.11.3")
  constraints {
    api("com.fasterxml.jackson.core:jackson-databind:2.15.2") {
      because("cpe:/a:fasterxml:jackson-databind")
    }
    api("org.apache.commons:commons-compress:1.24.0") {
      because("https://nvd.nist.gov/vuln/detail/CVE-2023-42503")
    }
  }
}
