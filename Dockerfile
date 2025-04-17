# Use the official Ubuntu base image
FROM ubuntu:22.04

# Set environment variables to avoid interactive prompts during installation
ENV DEBIAN_FRONTEND=noninteractive

# Update the package repository and install OpenJDK and Maven
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk maven && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Update PATH
ENV PATH="$JAVA_HOME/bin:$PATH"

# Verify installations
RUN java -version && mvn -version

# Set the working directory
WORKDIR /app

# Command to run when starting the container
CMD ["bash"]

RUN chmod -R 777 /app