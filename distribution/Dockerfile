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

FROM centos:7

# Project version defined in pom.xml is passed as an argument
ARG VERSION

# JDK 8 is a pre-requisite to run Drill ; 'which' package is needed for drill-config.sh
RUN yum install -y java-1.8.0-openjdk-devel which ; yum clean all ; rm -rf /var/cache/yum

# The drill tarball is generated upon building the Drill project
COPY target/apache-drill-$VERSION.tar.gz /tmp

# Drill binaries are extracted into the '/opt/drill' directory
RUN mkdir /opt/drill
RUN tar -xvzf /tmp/apache-drill-$VERSION.tar.gz --directory=/opt/drill --strip-components 1

# Starts Drill in embedded mode and connects to Sqlline
ENTRYPOINT /opt/drill/bin/drill-embedded
