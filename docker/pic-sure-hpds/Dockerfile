# Stage 1: Pull the dependencies from the existing image
FROM hms-dbmi/pic-sure-api:LATEST AS dependencies

# Stage 2: Build the new project
FROM maven:3.9.4-amazoncorretto-21 AS build
WORKDIR /app

# Copy source code and Maven settings
COPY . .
COPY --from=dependencies /root/.m2 /root/.m2

# Build the project and skip tests
RUN mvn clean install -DskipTests

# Stage 2: Create the runtime image
FROM amazoncorretto:21.0.1-alpine3.18

# Set working directory in runtime container
WORKDIR /app

# Copy the built JAR from the build stage
COPY --from=build /app/service/target/service-*-SNAPSHOT.jar /service.jar

# Define entry point
ENTRYPOINT ["sh", "-c", "java ${JAVA_OPTS} -jar /service.jar"]