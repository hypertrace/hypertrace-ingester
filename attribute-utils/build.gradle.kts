plugins {
    java
}

dependencies {
    implementation("org.hypertrace.core.datamodel:data-model:0.1.9")
    implementation(project(":span-normalizer:raw-span-constants"))
    implementation("org.hypertrace.entity.service:entity-service-client:0.1.23")
    implementation(project(":hypertrace-trace-enricher:enriched-span-constants"))
    implementation("org.apache.commons:commons-lang3:3.10")
}
