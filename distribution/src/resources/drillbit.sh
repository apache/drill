#!/usr/bin/env bash
# Copyright 2013 The Apache Software Foundation
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Environment Variables
#
#   DRILL_CONF_DIR      Alternate drill conf dir. Default is ${DRILL_HOME}/conf.
#   DRILL_LOG_DIR       Where log files are stored. Default is /var/log/drill if
#                       that exists, else $DRILL_HOME/log
#   DRILL_PID_DIR       The pid files are stored. $DRILL_HOME by default.
#   DRILL_IDENT_STRING  A string representing this instance of drillbit.
#                       $USER by default
#   DRILL_NICENESS      The scheduling priority for daemons. Defaults to 0.
#   DRILL_STOP_TIMEOUT  Time, in seconds, after which we kill -9 the server if
#                       it has not stopped.
#                       Default 120 seconds.
#   SERVER_LOG_GC       Set to "1" to enable Java garbage collector logging.
#
# See also the environment variables defined in drill-config.sh
# and runbit. Most of the above can be set in drill-env.sh for
# each site.
#
# Modeled after $HADOOP_HOME/bin/hadoop-daemon.sh
#
# Usage:
#
# drillbit.sh [--config conf-dir] cmd [arg1 arg2 ...]
#
# The configuration directory, if provided, must exist and contain a Drill
# configuration file. The option takes precedence over the
# DRILL_CONF_DIR environment variable.
#
# The command is one of: start|stop|status|restart|run
#
# Additional arguments are passed as JVM options to the Drill-bit.
# They typically are of the form:
#
# -Dconfig-var=value
#
# Where config-var is a fully expanded form of a configuration variable.
# The value overrides any value in the user or Drill configuration files.

usage="Usage: drillbit.sh [--config|--site <site-dir>]\
 (start|stop|status|restart|run) [args]"

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin">/dev/null; pwd`

base=`basename "${BASH_SOURCE-$0}"`
command=${base/.*/}

# Setup environment. This parses, and removes, the
# options --config conf-dir parameters.

. "$bin/drill-config.sh"

# if no args specified, show usage
if [ ${#args[@]} = 0 ]; then
  echo $usage
  exit 1
fi

# Get command. all other args are JVM args, typically properties.
startStopStatus="${args[0]}"
args[0]=''
export args

# Set default scheduling priority
DRILL_NICENESS=${DRILL_NICENESS:-0}

waitForProcessEnd()
{
  pidKilled=$1
  commandName=$2
  processedAt=`date +%s`
  origcnt=${DRILL_STOP_TIMEOUT:-120}
  while kill -0 $pidKilled > /dev/null 2>&1;
   do
     echo -n "."
     sleep 1;
     # if process persists more than $DRILL_STOP_TIMEOUT (default 120 sec) no mercy
     if [ $(( `date +%s` - $processedAt )) -gt $origcnt ]; then
       break;
     fi
  done
  echo
  # process still there : kill -9
  if kill -0 $pidKilled > /dev/null 2>&1; then
    echo "$commandName did not complete after $origcnt seconds, killing with kill -9 $pidKilled"
    $JAVA_HOME/bin/jstack -l $pidKilled > "$logout" 2>&1
    kill -9 $pidKilled > /dev/null 2>&1
  fi
}

check_before_start()
{
  #check that the process is not running
  mkdir -p "$DRILL_PID_DIR"
  if [ -f $pid ]; then
    if kill -0 `cat $pid` > /dev/null 2>&1; then
      echo "$command is already running as process `cat $pid`.  Stop it first."
      exit 1
    fi
  fi
}

wait_until_done ()
{
  p=$1
  cnt=${DRILLBIT_TIMEOUT:-300}
  origcnt=$cnt
  while kill -0 $p > /dev/null 2>&1; do
    if [ $cnt -gt 1 ]; then
      cnt=`expr $cnt - 1`
      sleep 1
    else
      echo "Process did not complete after $origcnt seconds, killing."
      kill -9 $p
      exit 1
    fi
  done
  return 0
}

start_bit ( )
{
  check_before_start
  echo "Starting $command, logging to $logout"
  echo "`date` Starting $command on `hostname`" >> "$DRILLBIT_LOG_PATH"
  echo "`ulimit -a`" >> "$DRILLBIT_LOG_PATH" 2>&1
  nohup nice -n $DRILL_NICENESS "$DRILL_HOME/bin/runbit" exec ${args[@]} >> "$logout" 2>&1 &
  echo $! > $pid
  sleep 1
}

stop_bit ( )
{
  if [ -f $pid ]; then
    pidToKill=`cat $pid`
    # kill -0 == see if the PID exists
    if kill -0 $pidToKill > /dev/null 2>&1; then
      echo "Stopping $command"
      echo "`date` Terminating $command pid $pidToKill" >> "$DRILLBIT_LOG_PATH"
      kill $pidToKill > /dev/null 2>&1
      waitForProcessEnd $pidToKill $command
      retval=0
    else
      retval=$?
      echo "No $command to stop because kill -0 of pid $pidToKill failed with status $retval"
    fi
    rm $pid > /dev/null 2>&1
  else
    echo "No $command to stop because no pid file $pid"
    retval=1
  fi
  return $retval
}

pid=$DRILL_PID_DIR/drillbit.pid
logout="${DRILL_LOG_PREFIX}.out"

thiscmd=$0

case $startStopStatus in

(start)
  start_bit
  ;;

(run)
  # Launch Drill as a child process. Does not redirect stderr or stdout.
  # Does not capture the Drillbit pid.
  # Use this when launching Drill from your own script that manages the
  # process, such as (roll-your-own) YARN, Mesos, supervisord, etc.

  echo "`date` Starting $command on `hostname`"
  echo "`ulimit -a`"
  $DRILL_HOME/bin/runbit exec ${args[@]}
  ;;

(stop)
  stop_bit
  exit $?
  ;;

(restart)
  # stop the command
  stop_bit
  # wait a user-specified sleep period
  sp=${DRILL_RESTART_SLEEP:-3}
  if [ $sp -gt 0 ]; then
    sleep $sp
  fi
  # start the command
  start_bit
  ;;

(status)
  if [ -f $pid ]; then
    TARGET_PID=`cat $pid`
    if kill -0 $TARGET_PID > /dev/null 2>&1; then
      echo "$command is running."
    else
      echo "$pid file is present but $command is not running."
      exit 1
    fi
  else
    echo "$command is not running."
    exit 1
  fi
  ;;

(debug)
  # Undocumented command to print out environment and Drillbit
  # command line after all adjustments.

  echo "command: $command"
  echo "args: ${args[@]}"
  echo "cwd:" `pwd`
  # Print Drill command line
  "$DRILL_HOME/bin/runbit" debug ${args[@]}
  ;;

(*)
  echo $usage
  exit 1
  ;;
esac
