ARG PIC-SURE-API-BUILD-VERSION
FROM hms-dbmi/pic-sure-build:${PIC-SURE-API-BUILD-VERSION} as PSB

FROM maven:3.9.9-amazoncorretto-24 AS build


## do we need this?
RUN yum update -y && yum install -y git && yum clean all

WORKDIR /app

COPY . .
# Extract the pic-sure-
COPY --from=PSB /app/pic-sure-api-war/target/pic-sure-api-war/WEB-INF/lib/pic-sure-resource-api-*-SNAPSHOT.jar \
    /root/.m2/repository/edu/harvard/hms/dbmi/avillach/pic-sure-resources/

RUN mvn clean install -DskipTests