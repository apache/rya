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
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

NAME=rya-streams-query-manager
DESC="Rya Streams Query Manager Daemon service"

# The path to JSVC
EXEC="/usr/bin/jsvc"

# Get the parent directory of the dir this script resides within.
PROJECT_HOME=$(dirname $(cd $(dirname $0) && pwd))

# This classpath must contain the commons-daemon jar as well as the jar we're executing.
CLASS_PATH="$PROJECT_HOME/lib/commons-daemon-1.1.0.jar:$PROJECT_HOME/lib/${project.artifactId}-${project.version}-shaded.jar"

# The fully qualified name of the class to execute. This class must implement the Daemon interface.
DAEMON_CLASS=org.apache.rya.streams.querymanager.QueryManagerDaemon

# The PID file that indicates if rya-streams-query-manager is running. 
PID="/var/run/$NAME.pid"

# System.out writes to this file...
LOG_OUT="$PROJECT_HOME/logs/$NAME.out"

# System.err writes to this file...
LOG_ERR="$PROJECT_HOME/logs/$NAME.err"

start ()
{
    if [ -f "$PID" ]
    then
        echo "The $DESC is already running."
        exit 1
    else
        echo "Starting the $DESC."
        $EXEC -cp $CLASS_PATH -user root -outfile $LOG_OUT -errfile $LOG_ERR -pidfile $PID \
              -Dlog4j.configuration="file://$PROJECT_HOME/config/log4j.xml" \
              $DAEMON_CLASS -c "$PROJECT_HOME/config/configuration.xml"
    fi
}

stop ()
{
    if [ -f "$PID" ]
    then
        echo "Stopping the $DESC."
        $EXEC -cp $CLASS_PATH -user root -outfile $LOG_OUT -errfile $LOG_ERR -pidfile $PID -stop $DAEMON_CLASS
    else
        echo "The $DESC is already stopped."
    fi
}

status ()
{
    if [ -f "$PID" ]
    then
        echo "The $DESC is running." 
    else
        echo "The $DESC is stopped."
    fi
}

restart ()
{
    echo "Restarting the $DESC..."
    if [ -f "$PID" ]; then        
        # Stop the service
        stop
    fi
    
    # Start the service
    start
    echo "The $DESC has restarted."
}

usage ()
{
    echo "usage: rya-streams-query-manager.sh (start|stop|status|restart)"
    exit 1
}

case "$1" in
    start) start ;;
    stop) stop ;;
    status) status ;;
    restart) restart ;;
    *) usage ;;
esac