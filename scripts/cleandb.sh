#!/usr/bin/env bash
# Script to reset all databases.
# Requires 2 parameters:
# ID: the id of the server
# DB: the wanted db type for the database.
if [ -z "$1" ]; then
  echo "Usage: cleandb.sh id database"
  exit 1
fi
ID=$1
DB=$2
if [ "$2" = "mixed" ]; then
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
cd ~
if [ "$DB" = "neo4j" ]; then
  echo "Cleaning neo4j on $ID"
  rm -rf Neo4jDB$ID
  tar -zxf neo4j.tar.gz
  mv Neo4jDB Neo4jDB$ID
fi
if [ "$DB" = "sparksee" ]; then
  echo "Cleaning sparksee on $ID"
  rm -rf Sparksee$ID
  unzip sparksee.zip -d Sparksee$ID
fi
if [ "$DB" = "titan" ]; then
  echo "Cleaning titan on $ID"
  rm -rf TitanDB$ID
  tar -zxf titan.tar.gz
  mv TitanDB TitanDB$ID
fi
if [ "$DB" = "orientdb" ]; then
  echo "Cleaning orientdb on $ID"
  rm -rf OrientDB$ID
  tar -zxf odb.tar.gz
  mv OrientDB OrientDB$ID
fi
