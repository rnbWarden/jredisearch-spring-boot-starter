# We're using the official OpenJDK image from the Docker Hub (https://hub.docker.com/_/java/).
# Take a look at the available versions so you can specify the Java version you want to use.
FROM openjdk:11-jdk

# Set the WORKDIR. All following commands will be run in this directory.
WORKDIR /app

## Copying all gradle files necessary to install gradle with gradlew
COPY gradle gradle
COPY build.gradle build.gradle
COPY gradle.properties gradle.properties
COPY gradlew gradlew
COPY gradlew.bat gradlew.bat
COPY settings.gradle settings.gradle

# Install the gradle version used in the repository through gradlew
RUN ./gradlew clean build

## Run gradle assemble to install dependencies before adding the whole repository
#RUN gradle assemble
#
#ADD . ./