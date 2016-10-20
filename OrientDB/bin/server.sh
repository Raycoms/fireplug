#!/bin/sh
#
# Copyright (c) OrientDB LTD (http://orientdb.com/)
#

echo "           .                                          "
echo "          .\`        \`                                 "
echo "          ,      \`:.                                  "
echo "         \`,\`    ,:\`                                   "
echo "         .,.   :,,                                    "
echo "         .,,  ,,,                                     "
echo "    .    .,.:::::  \`\`\`\`                                 :::::::::     :::::::::   "
echo "    ,\`   .::,,,,::.,,,,,,\`;;                      .:    ::::::::::    :::    :::  "
echo "    \`,.  ::,,,,,,,:.,,.\`  \`                       .:    :::      :::  :::     ::: "
echo "     ,,:,:,,,,,,,,::.   \`        \`         \`\`     .:    :::      :::  :::     ::: "
echo "      ,,:.,,,,,,,,,: \`::, ,,   ::,::\`   : :,::\`  ::::   :::      :::  :::    :::  "
echo "       ,:,,,,,,,,,,::,:   ,,  :.    :   ::    :   .:    :::      :::  :::::::     "
echo "        :,,,,,,,,,,:,::   ,,  :      :  :     :   .:    :::      :::  :::::::::   "
echo "  \`     :,,,,,,,,,,:,::,  ,, .::::::::  :     :   .:    :::      :::  :::     ::: "
echo "  \`,...,,:,,,,,,,,,: .:,. ,, ,,         :     :   .:    :::      :::  :::     ::: "
echo "    .,,,,::,,,,,,,:  \`: , ,,  :     \`   :     :   .:    :::      :::  :::     ::: "
echo "      ...,::,,,,::.. \`:  .,,  :,    :   :     :   .:    :::::::::::   :::     ::: "
echo "           ,::::,,,. \`:   ,,   :::::    :     :   .:    :::::::::     ::::::::::  "
echo "           ,,:\` \`,,.                                  "
echo "          ,,,    .,\`                                  "
echo "         ,,.     \`,                                          GRAPH DATABASE  "
echo "       \`\`        \`.                                                          "
echo "                 \`\`                                          orientdb.com"
echo "                 \`                                    "

# resolve links - $0 may be a softlink
PRG="$0"

while [ -h "$PRG" ]; do
  ls=`ls -ld "$PRG"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '/.*' > /dev/null; then
    PRG="$link"
  else
    PRG=`dirname "$PRG"`/"$link"
  fi
done

# Get standard environment variables
PRGDIR=`dirname "$PRG"`

# Only set ORIENTDB_HOME if not already set
[ -f "$ORIENTDB_HOME"/bin/server.sh ] || ORIENTDB_HOME=`cd "$PRGDIR/.." ; pwd`
export ORIENTDB_HOME
cd "$ORIENTDB_HOME/bin"

if [ ! -f "${CONFIG_FILE}" ]
then
  CONFIG_FILE=$ORIENTDB_HOME/config/orientdb-server-config.xml
fi

# Raspberry Pi check (Java VM does not run with -server argument on ARMv6)
if [ `uname -m` != "armv6l" ]; then
  JAVA_OPTS="$JAVA_OPTS -server "
fi
export JAVA_OPTS

# Set JavaHome if it exists
if [ -f "${JAVA_HOME}/bin/java" ]; then 
   JAVA=${JAVA_HOME}/bin/java
else
   JAVA=java
fi
export JAVA



LOG_FILE=$ORIENTDB_HOME/config/orientdb-server-log.properties
WWW_PATH=$ORIENTDB_HOME/www
ORIENTDB_PID=$ORIENTDB_HOME/bin/orient.pid

if [ -f "$ORIENTDB_PID" ]; then
    echo "removing old pid file $ORIENTDB_PID"
    rm "$ORIENTDB_PID"
fi

# DEBUG OPTS, SIMPLY USE 'server.sh debug'
DEBUG_OPTS=""
ARGS='';
for var in "$@"; do
    if [ "$var" = "debug" ]; then
        DEBUG_OPTS="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=1044"
    else
        ARGS="$ARGS $var"
    fi
done

# ORIENTDB memory options, default to 512 of heap.

if [ -z "$ORIENTDB_OPTS_MEMORY" ] ; then
    ORIENTDB_OPTS_MEMORY="-Xms512m -Xmx512m"
fi

if [ -z "$JAVA_OPTS_SCRIPT" ] ; then
    JAVA_OPTS_SCRIPT="-Djna.nosys=true -XX:+HeapDumpOnOutOfMemoryError -XX:MaxDirectMemorySize=512g -Djava.awt.headless=true -Dfile.encoding=UTF8 -Drhino.opt.level=9"
fi

# ORIENTDB SETTINGS LIKE DISKCACHE, ETC
if [ -z "$ORIENTDB_SETTINGS" ]; then
    ORIENTDB_SETTINGS="" # HERE YOU CAN PUT YOUR DEFAULT SETTINGS
fi

echo $$ > $ORIENTDB_PID

exec "$JAVA" $JAVA_OPTS $ORIENTDB_OPTS_MEMORY $JAVA_OPTS_SCRIPT $ORIENTDB_SETTINGS $DEBUG_OPTS \
    -Djava.util.logging.config.file="$LOG_FILE" \
    -Dorientdb.config.file="$CONFIG_FILE" \
    -Dorientdb.www.path="$WWW_PATH" \
    -Dorientdb.build.number="2.2.x@r8b3a478e3ca7321a48e7cf0f5991569bbe06ed89; 2016-10-03 09:39:41+0000" \
    -cp "$ORIENTDB_HOME/lib/orientdb-server-2.2.11.jar:$ORIENTDB_HOME/lib/*:$ORIENTDB_HOME/plugins/*" \
    $ARGS com.orientechnologies.orient.server.OServerMain
