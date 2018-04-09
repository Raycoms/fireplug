#!/usr/bin/env bash
# Kills all java processes on all hosts.
HOSTS=$BAG_HOSTS

for HOST in ${HOSTS} ; do
  echo "Killing java in $HOST"
  ssh rneiheiser@$HOST "pkill -f java"
done
