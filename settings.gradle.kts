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

includeBuild("./span-normalizer")
includeBuild("./raw-spans-grouper")
include(":hypertrace-ingester")
