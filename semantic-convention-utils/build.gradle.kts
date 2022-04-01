plugins {
    `java-library`
    id("org.hypertrace.publish-plugin")
}

tasks.test {
    useJUnitPlatform()
}

dependencies {
    implementation(project(":span-normalizer:raw-span-constants"))
    implementation(project(":span-normalizer:span-normalizer-constants"))

    implementation("org.hypertrace.core.datamodel:data-model:0.1.20")
    implementation("org.apache.commons:commons-lang3:3.12.0")

    constraints {
        implementation("com.fasterxml.jackson.core:jackson-databind:2.13.2.1") {
            because("Denial of Service (DoS) " +
                "[High Severity][https://snyk.io/vuln/SNYK-JAVA-COMFASTERXMLJACKSONCORE-2421244] in " +
                "com.fasterxml.jackson.core:jackson-databind@2.13.1")
        }
    }

    testImplementation("org.junit.jupiter:junit-jupiter:5.7.1")
    testImplementation("org.mockito:mockito-core:3.8.0")
}
