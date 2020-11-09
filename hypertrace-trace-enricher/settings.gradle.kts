rootProject.name = "hypertrace-trace-enricher"

pluginManagement {
  repositories {
    mavenLocal()
    gradlePluginPortal()
    maven("https://dl.bintray.com/hypertrace/maven")
  }
}

plugins {
  id("org.hypertrace.version-settings") version "0.1.1"
}

include(":enriched-span-constants")
include(":hypertrace-trace-visualizer")
include(":hypertrace-trace-enricher-api")
include(":hypertrace-trace-enricher-impl")
include(":hypertrace-trace-enricher")
include(":trace-reader")
