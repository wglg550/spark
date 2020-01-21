#!/bin/bash

APP_MAIN_JAR="spark-demo-1.0-SNAPSHOT.jar"

FILE_PATH=$(cd `dirname $0`; pwd)

JAVA_OPTS=`cat $FILE_PATH/start.vmoptions`
APP_ARGS=`cat $FILE_PATH/start.args`

PID_LIST=`ps -ef|grep $APP_MAIN_JAR|grep java|awk '{print $2}'`

if [ ! -z "$PID_LIST" ]; then
    echo "[ERROR] : APP is already running!"
    exit -1
fi

echo "java options:"
echo "$JAVA_OPTS"
echo "args:"
echo "$APP_ARGS"
    
nohup java $JAVA_OPTS -jar $FILE_PATH/lib/$APP_MAIN_JAR $APP_ARGS >/dev/null 2>&1 &

echo "starting......"

exit 0

