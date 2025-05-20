FROM maven:3.9.6-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
COPY wallet-query-service/pom.xml ./wallet-query-service/pom.xml
COPY wallet-query-service/src ./wallet-query-service/src
RUN mvn clean package -f wallet-query-service/pom.xml -DskipTests

FROM openjdk:17-jdk-slim
WORKDIR /app
COPY --from=build /app/wallet-query-service/target/wallet-query-service-1.0.0-SNAPSHOT.jar app.jar
EXPOSE 8082
ENTRYPOINT ["java", "-jar", "app.jar"]