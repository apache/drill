# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Set up the environment for the Drill-on-YARN client. Run only on the
# client machine; this is NOT used for launching the Drill Application Master
# or drillbit under YARN.

# Set the path to your Hadoop (and thus YARN) installation. This must include
# your valid Hadoop and YARN configuration files.
# This is equivalent to the --hadoop option of drill-on-yarn.sh; if you've set
# HADOOP_HOME you can omit the --hadoop option. You can also set
# HADOOP_HOME in the environment.

# export HADOOP_HOME=/path/to/hadoop
