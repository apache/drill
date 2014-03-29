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
#
# included in all the drill scripts with source command
# should not be executable directly
# also should not be passed any arguments, since we need original $*
# Modelled after $HADOOP_HOME/bin/hadoop-config.sh
#
# Environment Variables:
#
#   JAVA_HOME        The java implementation to use.
#
#   DRILL_CLASSPATH  Extra Java CLASSPATH entries.
#
#   HADOOP_HOME      Hadoop home

# resolve links - "${BASH_SOURCE-$0}" may be a softlink
this="${BASH_SOURCE-$0}"
while [ -h "$this" ]; do
  ls=`ls -ld "$this"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '.*/.*' > /dev/null; then
    this="$link"
  else
    this=`dirname "$this"`/"$link"
  fi
done

# convert relative path to absolute path
bin=`dirname "$this"`
script=`basename "$this"`
home=`cd "$bin/..">/dev/null; pwd`
this="$home/bin/$script"

# the root of the drill installation
if [ -z "$DRILL_HOME" ]; then
  DRILL_HOME="$home"
fi

#check to see if the conf dir or drill home are given as an optional arguments
while [ $# -gt 1 ]; do
  if [ "--config" = "$1" ]; then
    shift
    confdir=$1
    shift
    DRILL_CONF_DIR=$confdir
  else
    # Presume we are at end of options and break
    break
  fi
done

# Allow alternate drill conf dir location.
DRILL_CONF_DIR="${DRILL_CONF_DIR:-/etc/drill/conf}"

if [ ! -d $DRILL_CONF_DIR ]; then
  DRILL_CONF_DIR=$DRILL_HOME/conf
fi

# Source drill-env.sh for any user configured values
. "${DRILL_CONF_DIR}/drill-env.sh"

# get log directory
if [ "$DRILL_LOG_DIR" = "" ]; then
  DRILL_LOG_DIR=/var/log/drill
fi

# If HADOOP_HOME is specified, add all Hadoop jars except those
# specifically excluded in $DRILL_HOME/bin/hadoop-excludes.txt
if [ "${HADOOP_HOME}x" != "x" ] ; then
  HADOOP_CLASSPATH=""
  for jar in `ls $HADOOP_HOME/lib/*jar` ; do
    echo $jar | grep -v -f $DRILL_HOME/bin/hadoop-excludes.txt >/dev/null
    if [ "$?" -eq "0" ] ; then
      HADOOP_CLASSPATH=$jar:$HADOOP_CLASSPATH
    fi
  done
  HADOOP_CLASSPATH=$HADOOP_HOME/conf:$HADOOP_CLASSPATH
fi
if [ "${HBASE_HOME}x" != "x" ]
then
  HBASE_CLASSPATH=""
  for jar in `ls $HBASE_HOME/*jar`
  do
    echo $jar | grep -v -f $DRILL_HOME/bin/hadoop-excludes.txt >/dev/null
    if [ "$?" -eq "0" ]
    then
      HBASE_CLASSPATH=$jar:$HBASE_CLASSPATH
    fi
  done
  HBASE_CLASSPATH=$HBASE_HOME/conf:$HBASE_CLASSPATH
  export HBASE_CLASSPATH=$HBASE_CLASSPATH
fi

# Setup Drill classpath entries
CP=$DRILL_HOME/jars/*:$DRILL_CLASSPATH
CP=$DRILL_HOME/lib/*:$CP
CP=$DRILL_HOME/contrib/*:$CP
if [ "${HADOOP_CLASSPATH}x" != "x" ]; then
  CP=$HADOOP_CLASSPATH:$CP
fi
CP=$DRILL_CONF_DIR:$CP

# Newer versions of glibc use an arena memory allocator that causes virtual
# memory usage to explode. Tune the variable down to prevent vmem explosion.
export MALLOC_ARENA_MAX=${MALLOC_ARENA_MAX:-4}

# Test for cygwin
is_cygwin=false
case "`uname`" in
CYGWIN*) is_cygwin=true;;
esac

# Test for or find JAVA_HOME
if [ -z "$JAVA_HOME" ]; then
  if [ -e `which java` ]; then
    SOURCE=`which java`
    while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
      DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
      SOURCE="$(readlink "$SOURCE")"
      [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
    done
    JAVA_HOME="$( cd -P "$( dirname "$SOURCE" )" && cd .. && pwd )"
  fi
  # if we didn't set it
  if [ -z "$JAVA_HOME" ]; then
    cat 1>&2 <<EOF
+======================================================================+
|      Error: JAVA_HOME is not set and Java could not be found         |
+----------------------------------------------------------------------+
| Apache Drill requires Java 1.7 or later.                             |
+======================================================================+
EOF
    exit 1
  fi
fi
# Now, verify that 'java' binary exists and is suitable for Drill.
if $is_cygwin; then
  JAVA_BIN="java.exe"
else
  JAVA_BIN="java"
fi
JAVA=`find -L "$JAVA_HOME" -name $JAVA_BIN | head -n 1`
if [ ! -e "$JAVA" ]; then
  echo "Java not found at JAVA_HOME=$JAVA_HOME."
  exit 1
fi
# Ensure that Java version is at least 1.7
"$JAVA" -version 2>&1 | grep "version" | egrep -e "1.4|1.5|1.6" > /dev/null
if [ $? -eq 0 ]; then
  echo "Java 1.7 or later is required to run Apache Drill."
  exit 1
fi

# Adjust paths for CYGWIN
if $is_cygwin; then
  DRILL_HOME=`cygpath -w "$DRILL_HOME"`
  DRILL_CONF_DIR=`cygpath -w "$DRILL_CONF_DIR"`
  DRILL_LOG_DIR=`cygpath -w "$DRILL_LOG_DIR"`
  CP=`cygpath -w -p "$CP"`
fi

# Variables exported form this script
export is_cygwin
export DRILL_HOME
export DRILL_CONF_DIR
export DRILL_LOG_DIR
export CP
