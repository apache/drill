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
#   DRILL_HOME                 Drill home (defaults based on this
#                              script's path.)
#
#   JAVA_HOME                  The java implementation to use.
#
#   DRILL_CLASSPATH            Extra Java CLASSPATH entries.
#
#   DRILL_CLASSPATH_PREFIX     Extra Java CLASSPATH entries that should
#                              be prefixed to the system classpath.
#
#   HADOOP_HOME                Hadoop home
#
#   HBASE_HOME                 HBase home
#
# Variables may be set in one of four places:
#
#   Environment (per run)
#   drill-env.sh (per site)
#   distrib-env.sh (per distribution)
#   drill-config.sh (this file, Drill defaults)
#
# Properties "inherit" from items lower on the list, and may be "overridden" by items
# higher on the list. In the environment, just set the variable:
#
#   export FOO=value
#
# The three files must set values as shown below:
#
#   export FOO=${FOO:-"value"}
#

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
DRILL_HOME=${DRILL_HOME:-$home}

# Standardize error messages

fatal_error() {
  echo "ERROR: $@" 1>&2
  exit 1
}

# Check to see if the conf dir or drill home are given as an optional arguments
# Arguments may appear anywhere on the command line. --site is an alias, better
# specifies that the location contains all site-specific files, not just config.
#
# Remaining arguments go into the args array - use that instead of $@.

args=()
while [[ $# > 0 ]]
do
  arg="$1"
  case "$arg" in
  --site|--config)
    shift
    DRILL_CONF_DIR=$1
    shift
    ;;
  *)
    args+=("$1")
    shift
    ;;
  esac
done
export args

# If config dir is given, it must exist.

if [ -n "$DRILL_CONF_DIR" ]; then
  if [[ ! -d "$DRILL_CONF_DIR" ]]; then
    fatal_error "Config dir does not exist:" $DRILL_CONF_DIR
  fi
else

  # Allow alternate drill conf dir location.
  DRILL_CONF_DIR="/etc/drill/conf"

  # Otherwise, use the default
  if [[ ! -d "$DRILL_CONF_DIR" ]]; then
    DRILL_CONF_DIR="$DRILL_HOME/conf"
  fi
fi

# However we got the config dir, it must contain a config
# file, and that file must be readable.
# Most files are optional, so check the one that is required:
# drill-override.conf.

testFile="$DRILL_CONF_DIR/drill-override.conf"
if [[ ! -a "$testFile" ]]; then
  fatal_error "Drill config file missing: $testFile -- Wrong config dir?"
fi
if [[ ! -r "$testFile" ]]; then
  fatal_error "Drill config file not readable: $testFile - Wrong user?"
fi

# Set Drill-provided defaults here. Do not put Drill defaults
# in the distribution or user environment config files.

# The SQLline client does not need the code cache.

export SQLLINE_JAVA_OPTS=${SQLLINE_JAVA_OPTS:-"-XX:MaxPermSize=512M"}

# Class unloading is disabled by default in Java 7
# http://hg.openjdk.java.net/jdk7u/jdk7u60/hotspot/file/tip/src/share/vm/runtime/globals.hpp#l1622
export SERVER_GC_OPTS="$SERVER_GC_OPTS -XX:+CMSClassUnloadingEnabled -XX:+UseG1GC"

# No GC options by default for SQLLine
export CLIENT_GC_OPTS=${CLIENT_GC_OPTS:-""}

# Source the optional drill-env.sh for any user configured values.
# We read the file only in the $DRILL_CONF_DIR, which might be a
# site-specific folder. By design, we do not search both the site
# folder and the $DRILL_HOME/conf folder; we look in just the one
# identified by $DRILL_CONF_DIR.
#
# Note: the env files must set properties as follows for "inheritance"
# to work correctly:
#
# export FOO=${FOO:-"value"}

drillEnv="$DRILL_CONF_DIR/drill-env.sh"
if [ -r "$drillEnv" ]; then
  . "$drillEnv"
fi

# Source distrib-env.sh for any distribution-specific settings.
# distrib-env.sh is optional; it is created by some distribution installers
# that need distribution-specific settings.
# Because installers write site-specific values into the file, the file
# should be moved into the site directory, if the user employs one.

distribEnv="$DRILL_CONF_DIR/distrib-env.sh"
if [ -r "$distribEnv" ]; then
  . "$distribEnv"
else
  distribEnv="$DRILL_HOME/conf/distrib-env.sh"
  if [ -r "$distribEnv" ]; then
    . "$distribEnv"
  fi
fi

# Default memory settings if none provided by the environment or
# above config files.
# The Drillbit needs a large code cache.

export DRILL_MAX_DIRECT_MEMORY=${DRILL_MAX_DIRECT_MEMORY:-"8G"}
export DRILL_HEAP=${DRILL_HEAP:-"4G"}
export DRILLBIT_MAX_PERM=${DRILLBIT_MAX_PERM:-"512M"}
export DRILLBIT_CODE_CACHE_SIZE=${DRILLBIT_CODE_CACHE_SIZE:-"1G"}

export DRILLBIT_OPTS="-Xms$DRILL_HEAP -Xmx$DRILL_HEAP -XX:MaxDirectMemorySize=$DRILL_MAX_DIRECT_MEMORY"
export DRILLBIT_OPTS="$DRILLBIT_OPTS -XX:ReservedCodeCacheSize=$DRILLBIT_CODE_CACHE_SIZE -Ddrill.exec.enable-epoll=false"
export DRILLBIT_OPTS="$DRILLBIT_OPTS -XX:MaxPermSize=$DRILLBIT_MAX_PERM"

# Under YARN, the log directory is usually YARN-provided. Replace any
# value that may have been set in drill-env.sh.

if [ -n "$DRILL_YARN_LOG_DIR" ]; then
  DRILL_LOG_DIR="$DRILL_YARN_LOG_DIR"
fi

# get log directory
if [ -z "$DRILL_LOG_DIR" ]; then
  # Try the optional location
  DRILL_LOG_DIR=/var/log/drill
  if [[ ! -d "$DRILL_LOG_DIR" || ! -w "$DRILL_LOG_DIR" ]]; then
    # Default to the drill home folder. Create the directory
    # if not present.

    DRILL_LOG_DIR=$DRILL_HOME/log
  fi
fi

# Regardless of how we got the directory, it must exist
# and be writable.

mkdir -p "$DRILL_LOG_DIR"
if [[ ! -d "$DRILL_LOG_DIR" || ! -w "$DRILL_LOG_DIR" ]]; then
  fatal_error "Log directory does not exist or is not writable: $DRILL_LOG_DIR"
fi

# Store the pid file in Drill home by default, else in the location
# provided in drill-env.sh.

export DRILL_PID_DIR=${DRILL_PID_DIR:-$DRILL_HOME}

# Prepare log file prefix and the main Drillbit log file.

export DRILL_LOG_PREFIX="$DRILL_LOG_DIR/drillbit"
export DRILLBIT_LOG_PATH="${DRILL_LOG_PREFIX}.log"

# Class path construction.

# Add Drill conf folder at the beginning of the classpath
CP="$DRILL_CONF_DIR"

# If both user and YARN-provided Java lib paths exist,
# combine them.

if [ -n "$DOY_JAVA_LIB_PATH" ]; then
  if [ -z "$DRILL_JAVA_LIB_PATH" ]; then
    export DRILL_JAVA_LIB_PATH="$DOY_JAVA_LIB_PATH"
  else
    export DRILL_JAVA_LIB_PATH="$DOY_JAVA_LIB_PATH:$DRILL_JAVA_LIB_PATH"
  fi
fi

# Add the lib directory to the library path, if it exists.

libDir="$DRILL_CONF_DIR/lib"
if [ -d "$libDir" ]; then
  if [ -z "$DRILL_JAVA_LIB_PATH" ]; then
    export DRILL_JAVA_LIB_PATH="$libDir"
  else
    export DRILL_JAVA_LIB_PATH="$libDir:$DRILL_JAVA_LIB_PATH"
  fi
fi

# Add $DRILL_HOME/conf if the user has provided their own
# site configuration directory.
# Ensures we pick up the default logback.xml, etc. if the
# user does not provide their own.
# Also, set a variable to remember that the config dir
# is non-default, which is needed later.

if [[ ! "$DRILL_CONF_DIR" -ef "$DRILL_HOME/conf" ]]; then
  export DRILL_SITE_DIR="$DRILL_CONF_DIR"
  CP="$CP:$DRILL_HOME/conf"
fi

# Followed by any user specified override jars
if [ -n "$DRILL_CLASSPATH_PREFIX" ]; then
  CP="$CP:$DRILL_CLASSPATH_PREFIX"
fi

# Next Drill core jars
if [ -n "$DRILL_TOOL_CP" ]; then
  CP="$CP:$DRILL_TOOL_CP"
fi
CP="$CP:$DRILL_HOME/jars/*"

# Followed by Drill override dependency jars
CP="$CP:$DRILL_HOME/jars/ext/*"

# Followed by Hadoop's jar
if [ -n "$HADOOP_CLASSPATH" ]; then
  CP="$CP:$HADOOP_CLASSPATH"
fi

# Followed by HBase's jar
if [ -n "$HBASE_CLASSPATH" ]; then
  CP="$CP:$HBASE_CLASSPATH"
fi

# Generalized extension path (use this for new deployments instead
# of the specialized HADOOP_ and HBASE_CLASSPATH variables.)
# Drill-on-YARN uses this variable to avoid the need to add more
# XYX_CLASSPATH variables as we integrate with other external
# systems.

if [ -n "$EXTN_CLASSPATH" ]; then
  CP="$CP:$EXTN_CLASSPATH"
fi

# Followed by Drill's other dependency jars

CP="$CP:$DRILL_HOME/jars/3rdparty/*"
CP="$CP:$DRILL_HOME/jars/classb/*"

# Finally any user specified
# Allow user jars to appear in $DRILL_CONF_DIR/jars to avoid mixing
# user and Drill distribution jars.

if [ -d "$DRILL_CONF_DIR/jars" ]; then
  CP="$CP:$DRILL_CONF_DIR/jars/*"
fi

# The Drill classpath is a catch-all for any other jars that
# a specific run might need. The use of this variable is for jars that
# are not in a Drill directory; that means the jars must exist on every
# node in the cluster.

if [ -n "$DRILL_CLASSPATH" ]; then
  CP="$CP:$DRILL_CLASSPATH"
fi

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
      # if $SOURCE was a relative symlink, we need to resolve it relative
      # to the path where the symlink file was located
      [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE"
    done
    JAVA_HOME="$( cd -P "$( dirname "$SOURCE" )" && cd .. && pwd )"
  fi
  # if we didn't set it
  if [ -z "$JAVA_HOME" ]; then
    fatal_error "JAVA_HOME is not set and Java could not be found"
  fi
fi

# Now, verify that 'java' binary exists and is suitable for Drill.
if $is_cygwin; then
  JAVA_BIN="java.exe"
else
  JAVA_BIN="java"
fi
JAVA=`find -L "$JAVA_HOME" -name $JAVA_BIN -type f | head -n 1`
if [ ! -e "$JAVA" ]; then
  fatal_error "Java not found at JAVA_HOME=$JAVA_HOME."
fi

# Ensure that Java version is at least 1.7
"$JAVA" -version 2>&1 | grep "version" | egrep -e "1.4|1.5|1.6" > /dev/null
if [ $? -eq 0 ]; then
  fatal_error "Java 1.7 or later is required to run Apache Drill."
fi

# Adjust paths for CYGWIN
if $is_cygwin; then
  DRILL_HOME=`cygpath -w "$DRILL_HOME"`
  DRILL_CONF_DIR=`cygpath -w "$DRILL_CONF_DIR"`
  DRILL_LOG_DIR=`cygpath -w "$DRILL_LOG_DIR"`
  CP=`cygpath -w -p "$CP"`
  if [ -z "$HADOOP_HOME" ]; then
    export HADOOP_HOME=${DRILL_HOME}/winutils
  fi
fi

# make sure allocator chunks are done as mmap'd memory (and reduce arena overhead)
# Newer versions of glibc use an arena memory allocator that causes virtual
# memory usage to explode. Tune the variable down to prevent vmem explosion.
export MALLOC_ARENA_MAX=${MALLOC_ARENA_MAX:-4}
export MALLOC_MMAP_THRESHOLD_=131072
export MALLOC_TRIM_THRESHOLD_=131072
export MALLOC_TOP_PAD_=131072
export MALLOC_MMAP_MAX_=65536

# Variables exported form this script
export is_cygwin
export DRILL_HOME
export DRILL_CONF_DIR
export DRILL_LOG_DIR
export CP
export JAVA_HOME
export JAVA
