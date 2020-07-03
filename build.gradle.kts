plugins {
  id("org.hypertrace.repository-plugin") version "0.1.2"
  id("org.hypertrace.ci-utils-plugin") version "0.1.1"
  id("org.hypertrace.publish-plugin") version "0.2.1" apply false
  id("org.hypertrace.jacoco-report-plugin") version "0.1.0" apply false
}

subprojects {
  group = "org.hypertrace.core.rawspansgrouper"
}

subprojects {
  group = "org.hypertrace.core.rawspansgrouper"
  pluginManager.withPlugin("org.hypertrace.publish-plugin") {
    configure<org.hypertrace.gradle.publishing.HypertracePublishExtension> {
      license.set(org.hypertrace.gradle.publishing.License.AGPL_V3)
    }
  }
}