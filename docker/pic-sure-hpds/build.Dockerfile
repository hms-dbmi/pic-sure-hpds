FROM maven:3.9.9-amazoncorretto-24 AS build

COPY ./settings.xml /root/.m2/settings.xml

## do we need this?
RUN yum update -y && yum install -y git && yum clean all

WORKDIR /app

COPY . .

RUN mvn clean install -DskipTests -U