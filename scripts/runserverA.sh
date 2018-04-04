#!/usr/bin/env bash

if [ -z "$1" ]; then
  echo "Usage: runserver.sh idGlobal database"
  exit 1
fi
ID=$1
LEADER=$2
LOCAL=$3
DB=$4
mode=$5
mode2=$6
if [ "$DB" = "mixed" ]; then
  if [ "$ID" = "0" ]; then
    DB="neo4j"
  fi
  if [ "$ID" = "1" ]; then
    DB="sparksee"
  fi
  if [ "$ID" = "2" ]; then
    DB="titan"
  fi
  if [ "$ID" = "3" ]; then
    DB="orientdb"
  fi
fi
cd thesis
rm config/currentView
rm global/config/currentView
rm local0/config/currentView
rm local1/config/currentView
rm local2/config/currentView
rm local3/config/currentView

if [ "$LEADER" = "0" ]; then
echo "LEADER Starting with: $ID $DB $LOCAL $LEADER"
java -cp build/libs/1.0-0.1-Setup-fat.jar main.java.com.bag.server.ServerWrapper $ID $DB $LEADER $LOCAL true false $mode $mode2 > ~/output$ID.txt 2>&1
else
echo "Starting with: $ID $DB $LOCAL $LEADER"
java -cp build/libs/1.0-0.1-Setup-fat.jar main.java.com.bag.server.ServerWrapper $ID $DB $LEADER $LOCAL false false $mode $mode2 > ~/output$ID.txt 2>&1
fi