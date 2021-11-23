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

# This Dockerfile is used for automated builds in DockerHub. It adds
# project sources into the build image, builds Drill and copies built
# binaries into the target image.

# Example usage:
#
# {docker|podman} build \
#    --build-arg BUILD_BASE_IMAGE=maven:3.8.2-openjdk-11 \
#    --build-arg BASE_IMAGE=openjdk:11 \
#    -t apache/drill-openjdk-11 

# Unless otherwise specified, the intermediate container image will be 
# based on the following default.
ARG BUILD_BASE_IMAGE=maven:3.8.2-openjdk-8

# Unless otherwise specified, the final container image will be based on
# the following default.
ARG BASE_IMAGE=openjdk:8

# Uses intermediate image for building Drill to reduce target image size
FROM $BUILD_BASE_IMAGE as build

WORKDIR /src

# Copy project sources into the container
COPY . .

# Builds Drill
RUN mvn -Dmaven.artifact.threads=5 -T1C clean install -DskipTests

# Get project version and copy built binaries into /opt/drill directory
RUN VERSION=$(mvn -q -Dexec.executable=echo -Dexec.args='${project.version}' --non-recursive exec:exec) \
 && mkdir /opt/drill \
 && mv distribution/target/apache-drill-${VERSION}/apache-drill-${VERSION}/* /opt/drill

# Target image

# Set the BASE_IMAGE build arg when you invoke docker build.  
FROM $BASE_IMAGE

ENV DRILL_HOME=/opt/drill DRILL_USER=drilluser

RUN mkdir $DRILL_HOME

RUN groupadd -g 999 $DRILL_USER \
 && useradd -r -u 999 -g $DRILL_USER $DRILL_USER -m -d /var/lib/drill \
 && chown -R $DRILL_USER: $DRILL_HOME

USER $DRILL_USER

COPY --from=build --chown=$DRILL_USER /opt/drill $DRILL_HOME

# Starts Drill in embedded mode and connects to Sqlline
ENTRYPOINT $DRILL_HOME/bin/drill-embedded

