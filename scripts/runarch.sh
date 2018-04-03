#!/usr/bin/env bash
#BAG_HOSTS should contain the host ips in the right order.

#DBS=('neo4j' 'neo4j' 'orientdb' 'orientdb' 'neo4j' 'neo4j' 'orientdb' 'orientdb')
DBS=('neo4j' 'neo4j' 'neo4j' 'neo4j' 'neo4j' 'neo4j' 'neo4j' 'neo4j')

multi=$1
mode=$2
echo "Removing existing results files"

i=0
for HOST in ${BAG_HOSTS} ; do
    echo "Removing $ID results file";
    ssh $HOST "rm ~/results$ID.txt"
    ((i++))
done

i=0
for HOST in ${BAG_HOSTS} ; do
    DB=${DBS[i]}
    echo "Removing $ID $DB file";
    ssh $HOST "~/cleandb.sh $i $DB"
    ((i++))
done

localClusterId=(0 1 0 1 0 1 0 1 0 1 0 1 0 1 0 1)

#This one depends on the topology
idInLocalCluster=(0 0 1 1 2 2 3 3)
#idInLocalCluster=(0 0 0 1 1 1 2 2 2 3 3 3)
#idInLocalCluster=(0 0 0 0 1 1 1 1 2 2 2 2 3 3 3 3)

i=0
flag=1
for HOST in ${BAG_HOSTS} ; do
  DB=${DBS[i]}
  echo "Running global server $i on $HOST : ~/runserverA.sh $i ${localClusterId[$i]} ${idInLocalCluster[$i]} $DB $flag ";
  ssh $HOST "~/runserverA.sh $i ${localClusterId[$i]} ${idInLocalCluster[$i]} $DB $flag $multi $mode" &
  if [ "$i" = "3" ]; then
    ((flag--))
  fi

((i++))
done
