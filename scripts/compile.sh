#!/usr/bin/env bash
# Pulls the newest version of the branch and generates the FAT-jar.
for HOST in ${BAG_HOSTS} ; do
    echo "Compiling $HOST"
    ssh rneiheiser@$HOST "cd thesis && git pull && ./gradlew shadowjar" &
done
