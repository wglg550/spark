#!/bin/bash

APP_MAIN_JAR="spark-demo-1.0-SNAPSHOT.jar"

FILE_PATH=$(cd `dirname $0`; pwd)

PID_LIST=`ps -ef|grep $APP_MAIN_JAR|grep java|awk '{print $2}'`

if [ ! -z "$PID_LIST" ]; then
    echo "Runnable jar is $APP_MAIN_JAR."
    for PID in $PID_LIST 
    do
        kill -9 $PID
    done
    echo "stopped !"
fi

exit 0
