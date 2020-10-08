plugins {
  `java-library`
  id("org.hypertrace.publish-plugin")
}

tasks.test {
  useJUnitPlatform()
}

dependencies {
  implementation(project(":enriched-span-constants"))
  implementation("org.hypertrace.core.datamodel:data-model:0.1.9")

  implementation("org.slf4j:slf4j-api:1.7.30")
  implementation("org.apache.commons:commons-lang3:3.10")

  testImplementation("org.junit.jupiter:junit-jupiter:5.6.2")
}
