#!/usr/bin/env bash
#
#
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
#   DRILL_LOG_DIR       Where log files are stored. Default is
#                       /var/log/drill if that exists, else
#                       $DRILL_HOME/log  
#   DRILL_PID_DIR       The pid files are stored. $DRILL_HOME by default.
#   DRILL_IDENT_STRING  A string representing this instance of drillbit.
#                       $USER by default
#   DRILL_NICENESS      The scheduling priority for daemons. Defaults to 0.
#   DRILL_STOP_TIMEOUT  Time, in seconds, after which we kill -9 the server
#                       if it has not stopped. Default 120 seconds.
#
# See also the environment variables defined in drill-config.sh
# and runbit
#
# Modelled after $HADOOP_HOME/bin/hadoop-daemon.sh
#
# Usage:
#
# drillbit.sh [--config conf-dir] cmd [arg1 arg2 ...]
#
# The configuration directory, if provided, must exist and contain a Drill
# configuration file. The option takes precedence over the
# DRILL_CONF_DIR environment variable.
#
# The command is one of: start|stop|status|restart|autorestart
#
# Additional arguments are passed as JVM options to the Drill-bit.
# They typically are of the form:
#
# -Dconfig-var=value
#
# Where config-var is a fully expanded form of a configuration variable.
# The value overrides any value in the user or Drill configuration files.

usage="Usage: drillbit.sh [--config <conf-dir>]\
 (start|stop|status|restart|autorestart) [args]"

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin">/dev/null; pwd`

base=`basename "${BASH_SOURCE-$0}"`
command=${base/.*/}

# Setup environment. This parses, and removes, the
# options --config conf-dir parameters.

. "$bin"/drill-config.sh

# if no args specified, show usage
if [ $# -lt 1 ]; then
  echo $usage
  exit 1
fi

# get command
startStopStatus=$1
shift

waitForProcessEnd() {
  pidKilled=$1
  commandName=$2
  processedAt=`date +%s`
  while kill -0 $pidKilled > /dev/null 2>&1;
   do
     echo -n "."
     sleep 1;
     # if process persists more than $DRILL_STOP_TIMEOUT (default 120 sec) no mercy
     if [ $(( `date +%s` - $processedAt )) -gt ${DRILL_STOP_TIMEOUT:-120} ]; then
       break;
     fi
   done
  # process still there : kill -9
  if kill -0 $pidKilled > /dev/null 2>&1; then
    echo -n force stopping $commandName with kill -9 $pidKilled
    $JAVA_HOME/bin/jstack -l $pidKilled > "$logout" 2>&1
    kill -9 $pidKilled > /dev/null 2>&1
  fi
  # Add a CR after we're done w/ dots.
  echo
}

drill_rotate_log ()
{
    log=$1;
    num=5;
    if [ -n "$2" ]; then
    num=$2
    fi
    if [ -f "$log" ]; then # rotate logs
    while [ $num -gt 1 ]; do
        prev=`expr $num - 1`
        [ -f "$log.$prev" ] && mv -f "$log.$prev" "$log.$num"
        num=$prev
    done
    mv -f "$log" "$log.$num";
    fi
}

check_before_start(){
    #ckeck if the process is not running
    mkdir -p "$DRILL_PID_DIR"
    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo $command running as process `cat $pid`.  Stop it first.
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

# Return the number of seconds since the modification time
# for the given file.
# Sadly, getting the file mod time is platform dependent.
# The below are tested for MacOS and CentOS. Please contribute
# others.

secs_since_mod() {
  now=$(date +%s)
  case "`uname`" in
  Linux) modTime=$(date +%s -r "$1");; # CoreOS
  Darwin) modTime=$(stat -f%c "$1");; # MacOSX
  *) modTime="";;
  esac

  if [ -z "$modTime" ]; then
    echo ""
  else
    #echo $(( $(date +%s) - $(stat -f%c "$1") ))
    echo $(( $now - $modTime ))
  fi
}

# Convert a time in seconds to either seconds, minutes,
# hours or days depending on the duration.

elapsed_time() {
  secs=$1
  if [ $secs -lt 240 ]; then
    echo "$secs seconds"
  else
    let mins="( $secs + 30 ) / 60"
    if [ $mins -lt 240 ]; then
      echo "$mins minutes"
    else
      let hours="( $mins + 30 ) / 60"
      if [ $hours -lt 48 ]; then
        echo "$hours hours"
      else
        let days="( $hours + 12 ) / 24"
        echo "$days days"
      fi
    fi
  fi
}

start_bit() {
    check_before_start
    echo starting $command, logging to $logout
    nohup $thiscmd internal_start $args < /dev/null >> ${logout} 2>&1  &
    sleep 1;
}

launch_bit() {
    echo "`ulimit -a`" >> $loglog 2>&1
    nice -n $DRILL_NICENESS "$DRILL_HOME"/bin/runbit $args \
        >> "$logout" 2>&1 &
    echo $! > $pid
    bitpid=$!
    wait $bitpid
    retcode=$?
    rm $pid
}

stop_bit() {
    if [ -f $pid ]; then
      pidToKill=`cat $pid`
      # kill -0 == see if the PID exists
      if kill -0 $pidToKill > /dev/null 2>&1; then
        echo stopping $command
        echo "`date` Terminating $command" pid $pidToKill>> $loglog
        kill $pidToKill > /dev/null 2>&1
        waitForProcessEnd $pidToKill $command
      else
        retval=$?
        echo no $command to stop because kill -0 of pid $pidToKill failed with status $retval
      fi
      rm $pid > /dev/null 2>&1
    else
      echo no $command to stop because no pid file $pid
    fi
}

export DRILL_PID_DIR=${DRILL_PID_DIR:-$DRILL_HOME}

export DRILL_LOG_PREFIX=drillbit
export DRILL_LOGFILE=$DRILL_LOG_PREFIX.log
export DRILL_OUTFILE=$DRILL_LOG_PREFIX.out
export DRILL_QUERYFILE=${DRILL_LOG_PREFIX}_queries.json
loggc=$DRILL_LOG_DIR/$DRILL_LOG_PREFIX.gc
loglog="${DRILL_LOG_DIR}/${DRILL_LOGFILE}"
logout="${DRILL_LOG_DIR}/${DRILL_OUTFILE}"
logqueries="${DRILL_LOG_DIR}/${DRILL_QUERYFILE}"
pid=$DRILL_PID_DIR/drillbit.pid

export DRILLBIT_LOG_PATH=$loglog
export DRILLBIT_QUERY_LOG_PATH=$logqueries

if [ -n "$SERVER_GC_OPTS" ]; then
  export SERVER_GC_OPTS=${SERVER_GC_OPTS/"-Xloggc:<FILE-PATH>"/"-Xloggc:${loggc}"}
fi
if [ -n "$CLIENT_GC_OPTS" ]; then
  export CLIENT_GC_OPTS=${CLIENT_GC_OPTS/"-Xloggc:<FILE-PATH>"/"-Xloggc:${loggc}"}
fi

# Set default scheduling priority
export DRILL_NICENESS=${DRILL_NICENESS:-0}

thiscmd=$0
args=$@

case $startStopStatus in

(start)
  start_bit
  ;;

(internal_start)
  drill_rotate_log $loggc
  # Add to the command log file vital stats on our environment.
  echo "`date` Starting $command on `hostname`" >> $loglog
  launch_bit
  ;;

(yarn_start)

  # Start Drill within YARN.
  # This script does not exit until Drill does.

  check_before_start
  # The next message goes to YARN's container log file
  echo starting $command, logging to $logout
  drill_rotate_log $loggc
  # This message, and drill messages, go to Drill's log file
  echo "`date` Starting $command on `hostname` under YARN" >> $loglog
  launch_bit
  # Log to Drill and YARN logs
  echo "`date` $command on `hostname` pid $bitpid exited with status $retcode" >> $loglog
  echo "`date` $command on `hostname` pid $bitpid exited with status $retcode" 
  ;;
    
(stop)
  stop_bit
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
        upsecs=$(secs_since_mod $pid)
        if [ -z "$upsecs" ]; then
          uptime="Unknown (uname=`uname`)"
        else
          uptime=$(elapsed_time $upsecs)
        fi
        echo "$command is running, pid: $TARGET_PID, uptime: $uptime"
        exit 0
      else
        echo $pid file is present but $command not running.
        exit 1
      fi
    else
      echo $command not running.
      exit 2
    fi
    ;;

(debug)

    # Undocumented command to print out environment and Drillbit
    # command line after all adjustments.

    echo "----------------- Environment ------------------"
    env
    echo "------------------------------------------------"
    echo
    echo "command: $command"
    echo "args: $args"
    echo "cwd:" `pwd`
    # Print Drill command line
    export DRILL_DEBUG=1
    echo "Launch command:"
    "$DRILL_HOME"/bin/runbit $args
    if [[ -n "$DRILL_NODE_ENV" && ! -r "$DRILL_NODE_ENV" ]]; then
      echo "WARNING: DRILL_NODE_ENV is set, but $DRILL_NODE_ENV is not readable."
    fi  

    ;;

(*)
  echo $usage
  exit 1
  ;;
esac
