rootProject.name = "span-normalizer"


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

include(":span-normalizer-api")
include(":span-normalizer")
include(":raw-span-constants")
include(":span-normalizer-constants")
