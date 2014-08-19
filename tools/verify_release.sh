#!/usr/bin/env bash
#
#/**
# * Copyright 2014 The Apache Software Foundation
# *
# * Licensed to the Apache Software Foundation (ASF) under one
# * or more contributor license agreements.  See the NOTICE file
# * distributed with this work for additional information
# * regarding copyright ownership.  The ASF licenses this file
# * to you under the Apache License, Version 2.0 (the
# * "License"); you may not use this file except in compliance
# * with the License.  You may obtain a copy of the License at
# *
# *     http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# */
#

DRILL_CHECKUM_ALOGRITHMS="sha1 md5"
DRILL_SIGNATURE_EXTENSION=asc
DRILL_ARTIFACT_EXTENSION=tar.gz
DRILL_ARTIFACTS=*.${DRILL_ARTIFACT_EXTENSION}
DRILL_ARTIFACT_DIR=.

ERROR='\e[0;31m'
SUCCESS='\e[0;32m'
DEBUG='\e[1;30m'
INFO='\e[0;33m'
RESET='\e[0m'

#
# Helper methods
#
error_exit() { echo -e $ERROR$@$RESET;  popd &> /dev/null; exit 1; }
info_msg  () { echo -e $INFO$@$RESET; }
debug_msg () { echo -e $DEBUG$@$RESET; }

#
# Check the command line argument
#
case $# in
  0) ;;
  1)
    if ! [ -d $1 ] ; then
      pushd . &> /dev/null
      error_exit "'$1' is not a directory."
    fi
    DRILL_ARTIFACT_DIR=$1
  ;;
  *)
    echo "Usage: $0 <path_to_drill_artifacts>"
    exit 1
esac

#
# Begin validation
#
ARTIFACT_COUNT=0
pushd $DRILL_ARTIFACT_DIR &> /dev/null
shopt -s nullglob
for ARTIFACT in ${DRILL_ARTIFACTS} ; do
  MSG="Verifying artifact \"$ARTIFACT\"."
  info_msg ${MSG}
  info_msg $(eval printf "=%.0s" {1..${#MSG}})

  debug_msg Verifying signature...
  SIGNATURE_FILE=${ARTIFACT}.${DRILL_SIGNATURE_EXTENSION}
  if  ! [ -f ${SIGNATURE_FILE} ] ; then error_exit "Signature file '${SIGNATURE_FILE}' was not found"; fi
  if gpg --verify ${SIGNATURE_FILE} ; then
    debug_msg "Signature verified (${SIGNATURE_FILE})."
  else
    error_exit "Signature verification failed for '${ARTIFACT}'!"
  fi
  echo

  debug_msg Verifying checksums...
  for ALGO in $DRILL_CHECKUM_ALOGRITHMS ; do
    CHECKSUM_FILE=${ARTIFACT}.${ALGO}
    if  ! [ -f ${CHECKSUM_FILE} ] ; then error_exit "Checksum file '${CHECKSUM_FILE}' was not found"; fi
    COMPUTED_SUM=`${ALGO}sum ${ARTIFACT} | awk '{print $1}'`
    FILE_SUM=`cat ${CHECKSUM_FILE}`
    if [ "${FILE_SUM}" == "${COMPUTED_SUM}" ] ; then
      debug_msg "Verified ${ALGO} checksum (${CHECKSUM_FILE})."
    else
      error_exit "Computed ${ALGO} checksum did not match the one in '${CHECKSUM_FILE}'"
    fi
  done
  debug_msg "Checksums verified.\n"
  ARTIFACT_COUNT=$(($ARTIFACT_COUNT+1))
done

# All GOOD
if [ "$ARTIFACT_COUNT" == "0" ] ; then
  error_exit "No artifact found in '$DRILL_ARTIFACT_DIR'"
else
  echo -e $SUCCESS"All verifications passed on ${ARTIFACT_COUNT} artifacts."$RESET
fi
popd &> /dev/null