plugins {
  id("org.hypertrace.repository-plugin") version "0.4.0"
  id("org.hypertrace.ci-utils-plugin") version "0.3.0"
  id("org.hypertrace.docker-java-application-plugin") version "0.9.0" apply false
  id("org.hypertrace.docker-publish-plugin") version "0.9.0" apply false
  id("org.hypertrace.jacoco-report-plugin") version "0.2.0" apply false
  id("org.hypertrace.publish-plugin") version "1.0.2" apply false
  id("org.hypertrace.avro-plugin") version "0.3.1" apply false
  id("org.hypertrace.code-style-plugin") version "1.1.0" apply false
}

subprojects {
  group = "org.hypertrace.ingester"

  pluginManager.withPlugin("org.hypertrace.publish-plugin") {
    configure<org.hypertrace.gradle.publishing.HypertracePublishExtension> {
      license.set(org.hypertrace.gradle.publishing.License.TRACEABLE_COMMUNITY)
    }
  }

  pluginManager.withPlugin("java") {
    configure<JavaPluginExtension> {
      sourceCompatibility = JavaVersion.VERSION_11
      targetCompatibility = JavaVersion.VERSION_11

      apply(plugin = "org.hypertrace.code-style-plugin")
    }
  }
}

