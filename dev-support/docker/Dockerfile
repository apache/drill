#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Dockerfile for installing the necessary dependencies for building Drill.
# See BUILDING.txt.

FROM ubuntu:20.04

ARG DEBIAN_FRONTEND=noninteractive

WORKDIR /root

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

#####
# Disable suggests/recommends
#####
RUN echo APT::Install-Recommends "0"\; > /etc/apt/apt.conf.d/10disableextras
RUN echo APT::Install-Suggests "0"\; >>  /etc/apt/apt.conf.d/10disableextras

ENV DEBIAN_FRONTEND noninteractive
ENV DEBCONF_TERSE true


# hadolint ignore=DL3008
RUN apt -q update \
    && apt install -y software-properties-common apt-utils apt-transport-https \
    # Repo for different Python versions
    && add-apt-repository -y ppa:deadsnakes/ppa \
    && apt-get -q install -y --no-install-recommends \
        ant \
        bats \
        bash-completion \
        build-essential \
        bzip2 \
        ca-certificates \
        clang \
        cmake \
        curl \
        docker.io \
        doxygen \
        findbugs \
        fuse \
        g++ \
        gcc \
        git \
        gnupg-agent \
        libaio1 \
        libbcprov-java \
        libbz2-dev \
        libcurl4-openssl-dev \
        libfuse-dev \
        libnuma-dev \
        libncurses5 \
        libprotobuf-dev \
        libprotoc-dev \
        libsasl2-dev \
        libsnappy-dev \
        libssl-dev \
        libtool \
        libzstd-dev \
        locales \
        make \
        maven \
#        openjdk-11-jdk \
        openjdk-8-jdk \
        pinentry-curses \
        pkg-config \
        python \
        python2.7 \
#        python-pip \
        python-pkg-resources \
        python-setuptools \
#        python-wheel \
        python3-setuptools \
        python3-pip \
        python3.5 \
        python3.6 \
        python3.7 \
        python2.7 \
        virtualenv \
        tox \
        rsync \
        shellcheck \
        software-properties-common \
        sudo \
        valgrind \
        vim \
        wget \
        zlib1g-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

###
# Install grpcio-tools mypy-protobuf for `python3 sdks/python/setup.py sdist` to work
###
RUN pip3 install grpcio-tools mypy-protobuf

###
# Install Go
###
RUN mkdir -p /goroot \
    && curl https://dl.google.com/go/go1.15.2.linux-amd64.tar.gz | tar xvzf - -C /goroot --strip-components=1 \
    && chmod a+rwX -R /goroot

# Set environment variables for Go
ENV GOROOT /goroot
ENV GOPATH /gopath
ENV PATH $GOROOT/bin:$GOPATH/bin:$PATH
CMD go get github.com/linkedin/goavro

###
# Miscelaneous fixes...
###
# Turns out some build tools use 'time' and in this docker image this is no longer
# an executable but ONLY an internal command of bash
COPY time.sh /usr/bin/time
RUN chmod 755 /usr/bin/time

# Force the complete use of Java 8
RUN apt remove -y openjdk-11-jre openjdk-11-jre-headless
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/


###
# Avoid out of memory errors in builds
###
ENV MAVEN_OPTS -Xmx4g -XX:MaxPermSize=512m

###
# Add a welcome message and environment checks.
###
RUN mkdir /scripts
COPY drill_env_checks.sh /scripts/drill_env_checks.sh
COPY bashcolors.sh      /scripts/bashcolors.sh
RUN chmod 755 /scripts /scripts/drill_env_checks.sh /scripts/bashcolors.sh

# hadolint ignore=SC2016
RUN echo '. /etc/bash_completion'        >> /root/.bash_aliases
RUN echo '. /scripts/drill_env_checks.sh' >> /root/.bash_aliases
