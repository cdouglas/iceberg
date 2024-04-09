FROM mcr.microsoft.com/devcontainers/java:1-17-bullseye
#FROM mcr.microsoft.com/devcontainers/java:17-bullseye

# jdk not installed/managed by sdkman
ENV JAVA_HOME=/usr/lib/jvm/msopenjdk-current

RUN mkdir /usr/local/sdkman/candidates/java

# Create a Symlink from /usr/local/sdkman/candidates/java/current to /usr/lib/jvm/msopenjdk-current
RUN ln -s /usr/lib/jvm/msopenjdk-current /usr/local/sdkman/candidates/java/current

RUN apt-get update && \
    TZ=Etc/UTC DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      sudo \
      curl \
      vim \
      unzip \
      build-essential \
      software-properties-common \
      docker \
      ssh \
      ps \
      libxext6 \
      libxrender-dev \
      libxtst6 \
      libxi-dev \
      freetype* \
      procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
