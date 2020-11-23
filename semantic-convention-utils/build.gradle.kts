plugins {
    `java-library`
}

tasks.test {
    useJUnitPlatform()
}

dependencies {
    implementation(project(":hypertrace-trace-enricher:enriched-span-constants"))
    implementation(project(":span-normalizer:raw-span-constants"))

    implementation("org.hypertrace.core.datamodel:data-model:0.1.9")
    implementation("org.hypertrace.entity.service:entity-service-client:0.1.23")

    implementation("org.apache.commons:commons-lang3:3.10")

    testImplementation("org.junit.jupiter:junit-jupiter:5.6.2")
    testImplementation("org.mockito:mockito-core:3.3.3")
}
