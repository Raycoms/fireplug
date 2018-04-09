#!/usr/bin/env bash
for HOST in ${BAG_HOSTS} ; do
    echo "Compiling $HOST"
    ssh rneiheiser@$HOST "cd /home/rneiheiser/thesis && git pull && ./gradlew shadowjar" &
done
