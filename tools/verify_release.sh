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

DRILL_CHECKUM_ALOGRITHMS="sha1 md5"
DRILL_SIGNATURE_EXTENSION=asc
DRILL_DOWNLOAD_EXTENSIONS="tar.gz,zip,xml,asc,jar,md5,pom,sha1"
DRILL_ARTIFACTS="*.tar.gz *.zip *.pom *.jar"
DRILL_ARTIFACT_DIR=.

TERSE=false
CMD_REDIRECT="/dev/stdout"
DOWNLOAD_URL=
ARTIFACT_COUNT=0

ERROR='\e[0;31m'
SUCCESS='\e[0;32m'
DEBUG='\e[1;30m'
INFO='\e[0;33m'
RESET='\e[0m'

#
# Helper methods
#
info_msg()   { echo -e $INFO$@$RESET; }
debug_msg()  { if [ "$TERSE" == "false" ]; then echo -e $DEBUG$@$RESET; fi }
error_msg()  { echo -e $ERROR$@$RESET; }
error_exit() { echo -e $ERROR$@$RESET; popd &> /dev/null; exit 1; }

header_msg() {
  MSG=$@
  info_msg ${MSG}
  if [ "$TERSE" == "false" ]; then
    info_msg $(eval printf "=%.0s" {1..${#MSG}});
  fi
}

test_dir()   {
  if [ ! -e $1 ]; then
    mkdir -p $1
  elif [ ! -d $1 ]; then
    error_msg "The specified path '$1' is not a directory."
    usage
  fi
}

usage() {
  echo -e "Usage:\n `basename $0` <path_of_drill_artifacts>\n `basename $0` <url_of_drill_artifacts> [path_of_download_directory]"
  exit 1
}

download_files() {
  if [[ $# == 2 ]]; then
    test_dir $2
    DRILL_ARTIFACT_DIR=$2
  else
    DRILL_ARTIFACT_DIR=`mktemp -d`
  fi

  info_msg "Downloading files from '$1' into '$DRILL_ARTIFACT_DIR'"
  pushd ${DRILL_ARTIFACT_DIR} &> /dev/null
  wget $1 -r -np -A ${DRILL_DOWNLOAD_EXTENSIONS} -l0 -e robots=off -nH -nv &> $CMD_REDIRECT
  popd &> /dev/null
}

verify_directory() {
  pushd $1 &> /dev/null

  for ARTIFACT in ${DRILL_ARTIFACTS}; do
    ARTIFACT_COUNT=$(($ARTIFACT_COUNT+1))
    header_msg "Verifying artifact #$ARTIFACT_COUNT '$ARTIFACT' in $PWD"

    debug_msg "Verifying signature..."
    SIGNATURE_FILE=${ARTIFACT}.${DRILL_SIGNATURE_EXTENSION}
    if ! [ -f ${SIGNATURE_FILE} ]; then error_exit "Signature file '${SIGNATURE_FILE}' was not found"; fi
    if gpg --verify ${SIGNATURE_FILE} &> $CMD_REDIRECT; then
      debug_msg "Signature verified (${SIGNATURE_FILE})."
    else
      error_exit "Signature verification failed for '${ARTIFACT}'!"
    fi
    debug_msg ""

    debug_msg Verifying checksums...
    for ALGO in $DRILL_CHECKUM_ALOGRITHMS; do
      CHECKSUM_FILE=${ARTIFACT}.${ALGO}
      if ! [ -f ${CHECKSUM_FILE} ]; then error_exit "Checksum file '${CHECKSUM_FILE}' was not found"; fi
      COMPUTED_SUM=`${ALGO}sum ${ARTIFACT} | awk '{print $1}'`
      FILE_SUM=`cat ${CHECKSUM_FILE}`
      if [ "${FILE_SUM}" == "${COMPUTED_SUM}" ]; then
        debug_msg "Verified ${ALGO} checksum (${CHECKSUM_FILE})."
      else
        error_exit "Computed ${ALGO} checksum did not match the one in '${CHECKSUM_FILE}'"
      fi
    done
    debug_msg "Checksums verified.\n"
  done
  
  # recurse through the sub-directories
  for SUB_DIR in *; do
    if [ -d ${SUB_DIR} ]; then
      verify_directory ${SUB_DIR}
    fi
  done

  popd &> /dev/null
}

#
# Parse the command line arguments
#
if [ $# -eq 0 ]; then
  usage
fi
while [ $# -gt 0 ]; do
  case "$1" in
  -nv)
    TERSE=true
    CMD_REDIRECT="/dev/null"
  ;;
  -*)
    echo "Unknown option '$1'."
    usage
  ;;
  *)
    if [[ $1 == https://* ]] || [[ $1 == http://* ]] || [[ $1 == ftp://* ]]; then
      DOWNLOAD_URL=$1
    else
      test_dir $1; DRILL_ARTIFACT_DIR=$1
    fi
  ;;
  esac
  shift
done

#
# Download files if requested
#
if [ "$DOWNLOAD_URL" != "" ]; then
  download_files ${DOWNLOAD_URL} ${DRILL_ARTIFACT_DIR}
fi

#
# Begin validation
#
shopt -s nullglob
verify_directory ${DRILL_ARTIFACT_DIR}
info_msg ""

# All GOOD
if [ "$DOWNLOAD_URL" != "" ]; then
  info_msg "Files downloaded to '${DRILL_ARTIFACT_DIR}'"
fi

if [ "$ARTIFACT_COUNT" == "0" ]; then
  error_exit "No artifact found in '$DRILL_ARTIFACT_DIR'"
else
  echo -e $SUCCESS"All verifications passed on ${ARTIFACT_COUNT} artifacts."$RESET
fi
