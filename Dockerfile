# Build stage
FROM maven:3.9.4-eclipse-temurin-11 AS builder

WORKDIR /app

# Copy pom.xml first for better layer caching
COPY pom.xml .

# Download dependencies
RUN mvn dependency:go-offline -B

# Copy source code
COPY src ./src

# Build the project
RUN mvn clean package -DskipTests

# Test stage
FROM maven:3.9.4-eclipse-temurin-11 AS tester

WORKDIR /app

COPY pom.xml .
RUN mvn dependency:go-offline -B

COPY src ./src

# Run tests
RUN mvn test

# Runtime stage - Kafka Connect with custom SMT
FROM confluentinc/cp-kafka-connect:7.5.0 AS runtime

# Copy the built JAR
COPY --from=builder /app/target/kafka-custom-smt-*.jar /usr/share/java/kafka-connect-plugins/

# Development stage - for interactive development
FROM maven:3.9.4-eclipse-temurin-11 AS development

WORKDIR /app

# Install useful tools
RUN apt-get update && apt-get install -y \
    curl \
    vim \
    less \
    && rm -rf /var/lib/apt/lists/*

# Copy project files
COPY pom.xml .
COPY src ./src

# Keep container running for development
CMD ["tail", "-f", "/dev/null"]
