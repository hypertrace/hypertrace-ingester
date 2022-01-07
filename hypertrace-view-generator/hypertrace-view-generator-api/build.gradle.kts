plugins {
  `java-library`
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
  api("org.apache.avro:avro:1.10.2")
  constraints {
    api("org.apache.commons:commons-compress:1.21") {
      because("Multiple vulnerabilities in avro-declared version")
    }
    implementation("com.fasterxml.jackson.core:jackson-databind:2.13.1") {
      because("Denial of Service (DoS) " +
          "[Medium Severity][https://snyk.io/vuln/SNYK-JAVA-COMFASTERXMLJACKSONCORE-2326698] " +
          "in com.fasterxml.jackson.core:jackson-databind@2.12.2")
    }
  }
}
