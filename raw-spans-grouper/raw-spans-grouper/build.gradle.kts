plugins {
    application
    jacoco
    id("org.hypertrace.docker-java-application-plugin")
    id("org.hypertrace.docker-publish-plugin")
    id("org.hypertrace.jacoco-report-plugin")
}

application {
    mainClass.set("org.hypertrace.core.serviceframework.PlatformServiceLauncher")
}

hypertraceDocker {
    defaultImage {
        javaApplication {
            serviceName.set("${project.name}")
            adminPort.set(8099)
        }
    }
}

// Config for gw run to be able to run this locally. Just execute gw run here on Intellij or on the console.
tasks.run<JavaExec> {
    jvmArgs = listOf("-Dbootstrap.config.uri=file:$projectDir/src/main/resources/configs", "-Dservice.name=${project.name}")
}

tasks.test {
    useJUnitPlatform()
}

dependencies {
    implementation("org.glassfish.jersey.core:jersey-common:2.34") {
        because("https://snyk.io/vuln/SNYK-JAVA-ORGGLASSFISHJERSEYCORE-1255637")
    }
    implementation(project(":span-normalizer:span-normalizer-api"))
    implementation("org.hypertrace.core.datamodel:data-model:0.1.22")
    implementation("org.hypertrace.core.serviceframework:platform-service-framework:0.1.33")
    implementation("org.hypertrace.core.serviceframework:platform-metrics:0.1.33")

    implementation("org.hypertrace.core.kafkastreams.framework:kafka-streams-framework:0.1.25")
    implementation("com.typesafe:config:1.4.1")
    implementation("de.javakaffee:kryo-serializers:0.45")
    implementation("io.confluent:kafka-avro-serializer:5.5.0")
    implementation("com.google.guava:guava:30.1.1-jre")

    // Logging
    implementation("org.slf4j:slf4j-api:1.7.30")
    runtimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.17.1")

    testImplementation("org.junit.jupiter:junit-jupiter:5.7.1")
    testImplementation("org.mockito:mockito-core:3.8.0")
    testImplementation("org.junit-pioneer:junit-pioneer:1.3.8")
    testImplementation("org.apache.kafka:kafka-streams-test-utils:6.0.1-ccs")
}
