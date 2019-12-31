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

# This Dockerfile is used for automated builds in DockerHub. It adds project sources into the build image, builds
# Drill and copies built binaries into the target image based on openjdk:8u232-jdk image.

# Uses intermediate image for building Drill to reduce target image size
FROM maven:3.6-jdk-8 as build

# Copy project sources into the container
COPY . /src

WORKDIR /src

# Builds Drill
RUN  mvn clean install -DskipTests -q

# Get project version and copy built binaries into /opt/drill directory
RUN VERSION=$(mvn -q -Dexec.executable=echo -Dexec.args='${project.version}' --non-recursive exec:exec) \
 && mkdir /opt/drill \
 && mv distribution/target/apache-drill-${VERSION}/apache-drill-${VERSION}/* /opt/drill

# Target image
FROM openjdk:8u232-jdk

RUN mkdir /opt/drill

COPY --from=build /opt/drill /opt/drill

# Starts Drill in embedded mode and connects to Sqlline
ENTRYPOINT /opt/drill/bin/drill-embedded
