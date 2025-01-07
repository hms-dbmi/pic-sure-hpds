# Stage 1: Pull the dependencies from the existing image
FROM hms-dbmi/pic-sure-api AS dependencies

# Stage 2: Build the new project
FROM maven:3.9.4-amazoncorretto-21 AS build
WORKDIR /app

# Copy source code and Maven settings
COPY . .
COPY --from=dependencies /root/.m2 /root/.m2

# Build the project and skip tests
RUN mvn clean install -DskipTests

# Stage 2: Create the runtime image
FROM eclipse-temurin:21-alpine

# Install necessary packages
RUN apk add --no-cache --purge -uU bash curl wget unzip gnupg openssl && \
    rm -rf /var/cache/apk/* /tmp/*

# Copy only the specified JAR files and the script from the build stage
WORKDIR /app
COPY --from=build /app/target/SQLLoader-jar-with-dependencies.jar .
COPY --from=build /app/target/CSVLoader-jar-with-dependencies.jar .
COPY --from=build /app/target/CSVLoaderNewSearch-jar-with-dependencies.jar .
COPY --from=build /app/target/CSVDumper-jar-with-dependencies.jar .
COPY --from=build /app/target/VCFLocalLoader-jar-with-dependencies.jar .
COPY --from=build /app/target/VariantMetadataLoader-jar-with-dependencies.jar .
COPY --from=build /app/target/UnifiedVCFLocalLoader-jar-with-dependencies.jar .
COPY --from=build /app/target/MultialleleCounter-jar-with-dependencies.jar .
COPY --from=build /app/target/RekeyDataset-jar-with-dependencies.jar .
COPY --from=build /app/target/RemoveConceptFromMetadata-jar-with-dependencies.jar .
COPY --from=build /app/target/HideAnnotationCategoryValue-jar-with-dependencies.jar .
COPY --from=build /app/target/SequentialLoader-jar-with-dependencies.jar .
COPY create_key.sh .

# Define entry point
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -Xmx${HEAPSIZE:-2048}m -jar ${LOADER_NAME:-CSVLoader}-jar-with-dependencies.jar"]
