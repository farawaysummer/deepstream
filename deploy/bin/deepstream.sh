#!/bin/bash

#JAVA_HOME="/home/reseig/DQC/jdk"
JAVA_OPTS="-server -Xms2048m -Xmx4096m"
APP_HOME="$(dirname "$PWD")"
APP_LOG=${APP_HOME}/logs
APP_TITLE="DeepStream"
APP_MAIN_CLASS=com.rui.ds.facade.kettle.KettleStreamRunner
CLASSPATH=$APP_HOME/libs
for i in "$APP_HOME"/libs/*.jar; do
   CLASSPATH="$CLASSPATH":"$i"
done

for i in "$APP_HOME"/supports/*.jar; do
   CLASSPATH="$CLASSPATH":"$i"
done

APP_MAIN_CLASS=

psid=0

showTitle() {
    echo "________                        _________ __                                 ";
    echo "\______ \   ____   ____ ______ /   _____//  |________   ____ _____    _____  ";
    echo " |    |  \_/ __ \_/ __ \\____ \\_____  \\   __\_  __ \_/ __ \\__  \  /     \ ";
    echo " |    \`   \  ___/\  ___/|  |_> >        \|  |  |  | \/\  ___/ / __ \|  Y Y  \";
    echo "/_______  /\___  >\___  >   __/_______  /|__|  |__|    \___  >____  /__|_|  /";
    echo "        \/     \/     \/|__|          \/                   \/     \/      \/ ";

}

sayGoodbye() {
    echo "";
    echo "___________                                  .__  .__   ";
    echo "\_   _____/____ _______   ______  _  __ ____ |  | |  |  ";
    echo " |    __) \__  \\_  __ \_/ __ \ \/ \/ // __ \|  | |  |  ";
    echo " |     \   / __ \|  | \/\  ___/\     /\  ___/|  |_|  |__";
    echo " \___  /  (____  /__|    \___  >\/\_/  \___  >____/____/";
    echo "     \/        \/            \/            \/           ";
}

trap "sayGoodbye; exit" 1 2

if [ $# -lt 1 ]; then
    echo "================================"
    echo "deepstream.sh [start| stop | restart]"
    echo "================================"

    exit 1
else
    showTitle
fi

checkpid() {
   javaps=`${JAVA_HOME}/bin/jps -l | grep $APP_MAIN_CLASS`

   if [ -n "$javaps" ]; then
      psid=`echo $javaps | awk '{print $1}'`
   else
      psid=0
   fi
}

start() {
   checkpid

   if [ $psid -ne 0 ]; then
      echo "================================"
      echo "warn: $APP_TITLE already started! (pid=$psid)"
      echo "================================"
   else
      echo "Starting $APP_TITLE ..."
        $JAVA_HOME/bin/java $JAVA_OPTS -classpath $CLASSPATH -Dlog4j.configurationFile=${APP_HOME}/conf/log4j2.xml ${APP_MAIN_CLASS} "${APP_HOME}/" "$1"
      checkpid
      if [ $psid -ne 0 ]; then
         echo "(pid=$psid) [OK]"
      else
         echo "[Failed]"
      fi
   fi
}

stop() {
   checkpid

   if [ $psid -ne 0 ]; then
      echo "Stopping $APP_TITLE ...(pid=$psid) "

      kill -9 $psid

      if [ $? -eq 0 ]; then
         echo "[OK]"
      else
         echo "[Failed]"
      fi

      checkpid
      if [ $psid -ne 0 ]; then
         stop
      fi
   else
      echo "================================"
      echo "warn: $APP_TITLE is not running"
      echo "================================"
   fi
}

case "$1" in
   'start')
      start
      ;;
   'stop')
     stop
     ;;
   'restart')
     stop
     start
     ;;
  *)
  esac
   exit 1
