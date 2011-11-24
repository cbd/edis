#!/bin/bash 
PREFIX= /usr/local

if [[ $EUID -ne 0 ]]; then
  echo "This script must be run as sudo or root" 1>&2
  exit 1
fi

function get_port { 
   PORT=`epmd -names | grep edis | cut -f 5 -d' '`
} 

function stop { 
   get_port
   if [ -z $PORT ]; then 
      echo "edis is not running." 
   else
      echo -n "Stopping edis..." 
      PID=`ps aux | grep beam | grep "/edis " | awk '{print $2}'`
      kill $PID
      sleep 1
      echo ".. Done (it was running on $PID)." 
   fi
}

function start { 
   get_port
   if [ -z $PORT ]; then
      echo "Starting edis..." 
      ulimit -n 99999
      export HOME=/tmp
      if [ -f /etc/edis/edis.conf ] then
        $PREFIX/bin/edis /etc/edis/edis.conf
      else
        $PREFIX/bin/edis
      fi
      sleep 1
      get_port
      echo "Done. PORT=$PORT" 
   else 
      echo "edis is already running, PORT=$PORT" 
   fi
}

function restart { 
   echo  "Restarting edis..." 
   stop 
   start 
}

function status { 
   get_port
   if [ -z $PORT ]; then 
      echo "edis is not running." 
      exit 1 
   else 
      echo "edis is running, PORT=$PORT" 
   fi
}

case "$1" in 
   start) 
      DETACHED='-detached'
      start 
   ;; 
   start_attached)
      start
   ;;
   stop) 
      stop 
   ;; 
   restart) 
      DETACHED='-detached'
      restart 
   ;; 
   status) 
      status 
   ;; 
   *) 
      echo "Usage: $0 {start|stop|restart|status}" 
esac