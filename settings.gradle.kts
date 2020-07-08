rootProject.name = "hypertrace-view-generator"

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

include(":hypertrace-view-generator-api")
include(":hypertrace-view-generator")
include(":hypertrace-view-creator")
