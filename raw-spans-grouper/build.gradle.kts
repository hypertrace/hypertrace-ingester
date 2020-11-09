subprojects {
    group = "org.hypertrace.core.rawspansgrouper"

    pluginManager.withPlugin("org.hypertrace.publish-plugin") {
        configure<org.hypertrace.gradle.publishing.HypertracePublishExtension> {
            license.set(org.hypertrace.gradle.publishing.License.APACHE_2_0)
        }
    }
}