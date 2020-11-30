plugins {
    `java-library`
    jacoco
    id("org.hypertrace.avro-plugin")
    id("org.hypertrace.publish-plugin")
    id("org.hypertrace.jacoco-report-plugin")
}

tasks.test {
    useJUnitPlatform()
}

dependencies {
    api("org.apache.kafka:kafka-streams:5.5.1-ccs")
    implementation("org.apache.avro:avro:1.9.2")
    implementation("org.apache.kafka:kafka-clients:5.5.1-ccs")
    testImplementation("org.junit.jupiter:junit-jupiter:5.6.2")
}

// Disabling compatibility check for the test avro definitions.
tasks.named<org.hypertrace.gradle.avro.CheckAvroCompatibility>("avroCompatibilityCheck") {
    enabled = false
}