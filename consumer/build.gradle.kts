// The Java consumer group (SPEC.md Phase 3). Build: gradle jar -> build/libs/deadhead-consumer.jar (all dependencies inside).
plugins {
    java
    application
}

repositories { mavenCentral() }

dependencies {
    implementation("org.apache.kafka:kafka-clients:3.9.1")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.18.3")
    implementation("org.slf4j:slf4j-simple:2.0.16")
    testImplementation("org.junit.jupiter:junit-jupiter:5.11.4")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

java { toolchain { languageVersion.set(JavaLanguageVersion.of(21)) } }

application { mainClass.set("deadhead.Worker") }

tasks.jar {
    archiveFileName.set("deadhead-consumer.jar")
    manifest { attributes["Main-Class"] = "deadhead.Worker" }
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    exclude("META-INF/*.SF", "META-INF/*.DSA", "META-INF/*.RSA")
    from(configurations.runtimeClasspath.get().map { if (it.isDirectory) it else zipTree(it) })
}

tasks.test { useJUnitPlatform() }
