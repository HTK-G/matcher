# 使用 OpenJDK 17 作为基础镜像
FROM openjdk:17-jdk-slim

# 设置工作目录
WORKDIR /app

# 复制 Maven 构建的 JAR 文件到容器中
COPY target/matcher-1.0-SNAPSHOT.jar matcher.jar

# 暴露端口（如果需要外部访问）
EXPOSE 8080

# 启动 Matcher
CMD ["java", "-jar", "matcher.jar"]
