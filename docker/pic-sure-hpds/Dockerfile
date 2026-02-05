FROM amazoncorretto:24-alpine

WORKDIR /app

COPY /app/service/target/service-*-SNAPSHOT.jar /service.jar

ENTRYPOINT ["sh", "-c", "java ${JAVA_OPTS} -jar /service.jar"]