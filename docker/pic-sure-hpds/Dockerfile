FROM maven:3.9.9-amazoncorretto-24 AS build

RUN yum update -y && yum install -y git && yum clean all

WORKDIR /app

RUN --mount=type=cache,target=/root/.m2 mvn clean install -DskipTests

FROM amazoncorretto:24-alpine

WORKDIR /app

COPY --from=build /app/service/target/service-*-SNAPSHOT.jar /service.jar

ENTRYPOINT ["sh", "-c", "java ${JAVA_OPTS} -jar /service.jar"]