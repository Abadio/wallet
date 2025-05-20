FROM maven:3.9.6-eclipse-temurin-17 AS build
WORKDIR /app
# Copia todo o projeto multi-módulo
COPY . .
# Compila todos os módulos no nível do pom.xml raiz
RUN mvn clean package -DskipTests

FROM openjdk:17-jdk-slim
WORKDIR /app
# Copia o .jar gerado para o wallet-command-service
COPY --from=build /app/wallet-command-service/target/wallet-command-service-1.0.0-SNAPSHOT.jar app.jar
EXPOSE 8081
ENTRYPOINT ["java", "-jar", "app.jar"]