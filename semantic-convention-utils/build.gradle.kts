plugins {
    `java-library`
}

tasks.test {
    useJUnitPlatform()
}

dependencies {
    implementation(project(":hypertrace-trace-enricher:enriched-span-constants"))
    implementation(project(":span-normalizer:raw-span-constants"))
    implementation(project(":span-normalizer:span-normalizer-constants"))

    implementation("org.hypertrace.core.datamodel:data-model:0.1.14")
    implementation("org.hypertrace.entity.service:entity-service-client:0.6.4")

    implementation("org.apache.commons:commons-lang3:3.12.0")

    testImplementation("org.junit.jupiter:junit-jupiter:5.7.1")
    testImplementation("org.mockito:mockito-core:3.8.0")
}
