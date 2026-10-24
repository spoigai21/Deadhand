# The Java consumer group, built and run from a clean machine. Measurements in NUMBERS.md were taken from host JVMs.
FROM gradle:9.0-jdk21 AS build
WORKDIR /src
COPY build.gradle.kts settings.gradle.kts ./
COPY src ./src
RUN gradle jar -q --no-daemon

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /src/build/libs/deadhead-consumer.jar .
ENTRYPOINT ["java", "-Xmx2g", "-jar", "deadhead-consumer.jar"]
