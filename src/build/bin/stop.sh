#!/bin/bash

# BASE_DIR
BASE_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )
echo "BASE_DIR: " ${BASE_DIR}

# DATA_DIR
DATA_DIR=${BASE_DIR}/data
if [ ! -d "$DATA_DIR" ]; then
  mkdir -p "$DATA_DIR"
fi
echo "DATA_DIR: " ${DATA_DIR}

# PID_FILE
PID_FILE=${BASE_DIR}/data/pid
echo "PID_FILE: " ${PID_FILE}

# STOP
echo "Stopping server ... "
if [ ! -f "$PID_FILE" ]
then
  echo "no server to stop (could not find file $PID_FILE)"
else
  kill `cat ${PID_FILE}`
  rm ${PID_FILE}
  echo STOPPED
fi