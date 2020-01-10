#!/bin/bash
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

#!/bin/bash
ELASTIC_HOST="http://localhost:9200"
INDEX="employee"
MANAGERTYPEMAPPING="manager"
DEVELOPERTYPEMMAPPING="developer"

function createIndex() {
IDXNAME=$1

# Creating Index
    curl -XPUT -u elastic:changeme "${ELASTIC_HOST}/${IDXNAME}?pretty" -H 'Content-Type: application/json' -d " { \"settings\" : { \"index\" : { \"number_of_shards\" : 3,
    \"number_of_replicas\" : 1 } } }"
}

function createManager() {
    ID=$1
    NAME=$2
    EMPLOYEEID=$3
    DEPARTMENT=$4

    curl -XPUT -u elastic:changeme "${ELASTIC_HOST}/${INDEX}/${MANAGERTYPEMAPPING}/${ID}?pretty" -H 'Content-Type: application/json' -d" { \"name\" : \"${NAME}\",
    \"employeeId\" : ${EMPLOYEEID}, \"department\" : \"${DEPARTMENT}\" }"
}

function createDeveloper() {
    ID=$1
    NAME=$2
    EMPLOYEEID=$3
    DEPARTMENT=$4
    REPORTSTO=$5
    curl -XPUT -u elastic:changeme "${ELASTIC_HOST}/${INDEX}/${DEVELOPERTYPEMMAPPING}/${ID}?pretty" -H 'Content-Type: application/json' -d" { \"name\" : \"${NAME}\",
    \"employeeId\" : ${EMPLOYEEID}, \"department\" : \"${DEPARTMENT}\", \"reportsTo\" : \"${REPORTSTO}\" }"
}

#Clean Index
curl -XDELETE -u elastic:changeme "${ELASTIC_HOST}/${INDEX}?pretty"

# Creating Index
createIndex $INDEX

# Creating some content
createManager "manager1" "manager1" 1 "IT"
createDeveloper "developer01" "developer1" 2 "IT" "manager1"
createDeveloper "developer02" "developer2" 3 "IT" "manager1"
createDeveloper "developer04" "developer4" 4 "IT" "manager1"
createDeveloper "developer05" "developer5" 5 "IT" "manager1"
createDeveloper "developer06" "developer6" 6 "IT" "manager1"
createDeveloper "developer07" "developer7" 7 "IT" "manager1"
createDeveloper "developer08" "developer8" 8 "IT" "manager1"
createDeveloper "developer09" "developer9" 9 "IT" "manager1"
createDeveloper "developer10" "developer10" 10 "IT" "manager1"
createDeveloper "developer11" "developer11" 11 "IT" "manager1"
createDeveloper "developer12" "developer12" 12 "IT" "manager1"
createDeveloper "developer13" "developer13" 13 "IT" "manager1"
createDeveloper "developer14" "developer14" 14 "IT" "manager1"
createDeveloper "developer15" "developer15" 15 "IT" "manager1"
createManager "manager2" "manager2" 16 "IT"
createDeveloper "developer16" "developer16" 17 "IT" "manager2"
createDeveloper "developer17" "developer17" 18 "IT" "manager2"
createDeveloper "developer18" "developer18" 19 "IT" "manager2"
createDeveloper "developer19" "developer19" 20 "IT" "manager2"
createDeveloper "developer20" "developer20" 21 "IT" "manager2"
