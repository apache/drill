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
set -e

function pause(){
    read -rsp $'Press any key to continue...\n' -n1 key
}

function runCmd(){
    echo " ----------------- "  >> ${DRILL_RELEASE_OUTFILE}
    echo " ----------------- $1 "
    echo " ----------------- $1 " >> ${DRILL_RELEASE_OUTFILE}
    echo " ----------------- "  >> ${DRILL_RELEASE_OUTFILE}
    shift
    # run the command, send output to out file
    "$@" >> ${DRILL_RELEASE_OUTFILE} 2>&1
    if [ $? -ne 0 ]; then
        echo FAILED to run $1
        echo FAILED to run $1 >> ${DRILL_RELEASE_OUTFILE}
        exit 1
    fi
    # Wait for user to verify and continue
    pause
}

function copyFiles(){
    rm -rf ${LOCAL_RELEASE_STAGING_DIR}
    mkdir -p ${LOCAL_RELEASE_STAGING_DIR}/${DRILL_RELEASE_VERSION}
    cp ${DRILL_SRC}/target/apache-drill-${DRILL_RELEASE_VERSION}-src.tar.gz* ${LOCAL_RELEASE_STAGING_DIR}/${DRILL_RELEASE_VERSION}/ && \
    cp ${DRILL_SRC}/target/apache-drill-${DRILL_RELEASE_VERSION}-src.zip* ${LOCAL_RELEASE_STAGING_DIR}/${DRILL_RELEASE_VERSION}/ && \
    cp ${DRILL_SRC}/distribution/target/apache-drill-${DRILL_RELEASE_VERSION}.tar.gz* ${LOCAL_RELEASE_STAGING_DIR}/${DRILL_RELEASE_VERSION}/

}

function checkPassphrase(){
    echo "1234" | gpg2 --batch --passphrase "${GPG_PASSPHRASE}" -o /dev/null -as -
    if [ $? -ne 0 ]; then
        echo "Invalid passphrase. Make sure the default key is set to the key you want to use (or make it the first key in the keyring)."
        exit 1
    fi
}

function createDirectoryIfAbsent() {
    DIR_NAME="$1"
    if [ ! -d "${DIR_NAME}" ]; then
        echo "Creating directory ${DIR_NAME}"
        mkdir -p "${DIR_NAME}"
    fi
}


function readInputAndSetup(){

    read -p "Drill Working Directory : " WORK_DIR
    createDirectoryIfAbsent "${WORK_DIR}"

    read -p "Drill Release Version (eg. 1.4.0) : " DRILL_RELEASE_VERSION

    read -p "Drill Development Version (eg. 1.5.0-SNAPSHOT) : " DRILL_DEV_VERSION

    read -p "Release Commit SHA : " RELEASE_COMMIT_SHA

    read -p "Write output to (directory) : " DRILL_RELEASE_OUTDIR
    createDirectoryIfAbsent "${DRILL_RELEASE_OUTDIR}"

    read -p "Staging (personal) repo : " MY_REPO

    read -p "Local release staging directory : " LOCAL_RELEASE_STAGING_DIR
    createDirectoryIfAbsent "${LOCAL_RELEASE_STAGING_DIR}"

    read -p "Apache login : " APACHE_LOGIN

    read -p "Release candidate attempt : " RELEASE_ATTEMPT

    read -s -p "GPG Passphrase (Use quotes around a passphrase with spaces) : " GPG_PASSPHRASE

    DRILL_RELEASE_OUTFILE="${DRILL_RELEASE_OUTDIR}/drill_release.out.txt"
    DRILL_SRC=${WORK_DIR}/drill-release

    echo ""
    echo "-----------------"
    echo "Drill Working Directory : " ${WORK_DIR}
    echo "Drill Src Directory : " ${DRILL_SRC}
    echo "Drill Release Version : " ${DRILL_RELEASE_VERSION}
    echo "Drill Development Version : " ${DRILL_DEV_VERSION}
    echo "Release Commit SHA : " ${RELEASE_COMMIT_SHA}
    echo "Write output to : " ${DRILL_RELEASE_OUTFILE}
    echo "Staging (personal) repo : " ${MY_REPO}
    echo "Local release staging dir : " ${LOCAL_RELEASE_STAGING_DIR}

    touch ${DRILL_RELEASE_OUTFILE}
}

checkPassphraseNoSpace(){
    # The checkPassphrase function does not work for passphrases with embedded spaces.
    if [ -z "${GPG_PASSPHRASE##* *}" ] ;then
        echo "Passphrase contains a space - No Validation performed."
    else
        echo -n "Validating passphrase .... "
        checkPassphrase && echo "Passphrase accepted" || echo "Invalid passphrase"
    fi
}

cloneRepo(){
    cd ${WORK_DIR}
    rm -rf ./drill-release
    git clone https://github.com/apache/drill.git drill-release  >& ${DRILL_RELEASE_OUTFILE}
    cd ${DRILL_SRC}
    git checkout ${RELEASE_COMMIT_SHA}
}

###### BEGIN  #####
# Location of checksum.sh
export CHKSMDIR=`pwd`

readInputAndSetup
checkPassphraseNoSpace

runCmd "Cloning the repo" cloneRepo

runCmd "Checking the build" mvn install -DskipTests

export MAVEN_OPTS=-Xmx2g
runCmd "Clearing release history" mvn release:clean -Papache-release -DpushChanges=false -DskipTests

export MAVEN_OPTS='-Xmx4g -XX:MaxPermSize=512m'
runCmd "Preparing the release " mvn -X release:prepare -Papache-release -DpushChanges=false -DskipTests -Darguments="-Dgpg.passphrase=${GPG_PASSPHRASE}  -DskipTests=true -Dmaven.javadoc.skip=false" -DreleaseVersion=${DRILL_RELEASE_VERSION} -DdevelopmentVersion=${DRILL_DEV_VERSION} -Dtag=drill-${DRILL_RELEASE_VERSION}

runCmd "Pushing to private repo ${MY_REPO}" git push ${MY_REPO} drill-${DRILL_RELEASE_VERSION}

runCmd "Performing the release to ${MY_REPO}" mvn release:perform -DconnectionUrl=scm:git:${MY_REPO} -DskipTests -Darguments="-Dgpg.passphrase=${GPG_PASSPHRASE} -DskipTests=true -DconnectionUrl=scm:git:${MY_REPO}"

runCmd "Checking out release commit" git checkout drill-${DRILL_RELEASE_VERSION}

# Remove surrounding quotes
tempGPG_PASSPHRASE="${GPG_PASSPHRASE%\"}"
tempGPG_PASSPHRASE="${tempGPG_PASSPHRASE#\"}"
runCmd "Deploying ..." mvn deploy -Papache-release -DskipTests -Dgpg.passphrase="${tempGPG_PASSPHRASE}"

runCmd "Copying" copyFiles

runCmd "Verifying artifacts are signed correctly" ${CHKSMDIR}/checksum.sh ${DRILL_SRC}/distribution/target/apache-drill-${DRILL_RELEASE_VERSION}.tar.gz
pause

runCmd "Copy release files to home.apache.org" sftp ${APACHE_LOGIN}@home.apache.org <<EOF
  mkdir public_html
  cd public_html
  mkdir drill
  cd drill
  mkdir releases
  cd releases
  mkdir ${DRILL_RELEASE_VERSION}
  cd ${DRILL_RELEASE_VERSION}
  mkdir rc${RELEASE_ATTEMPT}
  cd rc${RELEASE_ATTEMPT}
  put ${LOCAL_RELEASE_STAGING_DIR}/${DRILL_RELEASE_VERSION}/* .
  bye
EOF

echo "Go to the Apache maven staging repo and close the new jar release"
pause

echo "Start the vote \(good luck\)\n"
