FROM hms-dbmi/pic-sure-api:release_083e420 AS dependencies

FROM maven:3.9.4-amazoncorretto-21 AS build
WORKDIR /app

COPY . .
COPY --from=dependencies /root/.m2 /root/.m2

RUN mvn clean install -DskipTests

FROM amazoncorretto:21.0.1-alpine3.18

WORKDIR /app

COPY --from=build /app/service/target/service-*-SNAPSHOT.jar /service.jar

ENTRYPOINT ["sh", "-c", "java ${JAVA_OPTS} -jar /service.jar"]