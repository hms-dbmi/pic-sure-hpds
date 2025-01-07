# Stage 1: Build the application using Maven
FROM maven:3.9.4-amazoncorretto-21 AS build
WORKDIR /app

# Copy the entire project into the container
COPY ../../ .

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