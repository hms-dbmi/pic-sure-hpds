FROM maven:3.9.9-amazoncorretto-24 AS build

RUN mkdir -p /root/.m2
COPY settings.xml /root/.m2/settings.xml

RUN echo "===== settings.xml =====" \
 && cat /root/.m2/settings.xml \
 && echo "========================"

## do we need this?
RUN yum update -y && yum install -y git && yum clean all

WORKDIR /app

COPY . .

RUN mvn -s /root/.m2/settings.xml clean install -DskipTests -U