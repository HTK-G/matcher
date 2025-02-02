FROM openjdk:11-jre-slim
WORKDIR /app
COPY target/matcher-shaded.jar matcher.jar
CMD ["java", "-jar", "matcher.jar"]
ENV AWS_REGION=us-east-1

