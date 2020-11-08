pluginManagement {
  repositories {
    mavenLocal()
    gradlePluginPortal()
    maven("https://dl.bintray.com/hypertrace/maven")
  }
}

plugins {
  id("org.hypertrace.version-settings") version "0.1.2"
}

rootProject.name = "hypertrace-ingester"

include("hypertrace-trace-enricher:enriched-span-constants")
include("hypertrace-trace-enricher:hypertrace-trace-visualizer")
include("hypertrace-trace-enricher:hypertrace-trace-enricher-api")
include("hypertrace-trace-enricher:hypertrace-trace-enricher-impl")
include("hypertrace-trace-enricher:hypertrace-trace-enricher")

include("hypertrace-view-generator:hypertrace-view-generator-api")
include("hypertrace-view-generator:hypertrace-view-generator")
include("hypertrace-view-generator:hypertrace-view-creator")

include("raw-spans-grouper")

include("span-normalizer:span-normalizer-api")
include("span-normalizer:span-normalizer")
include("span-normalizer:raw-span-constants")
include("span-normalizer:span-normalizer-constants")

include("hypertrace-ingester")
