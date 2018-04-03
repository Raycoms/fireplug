#!/usr/bin/env bash
if [ -z "$1" ]; then
  echo "Usage: runserver.sh name numClients percWrites"
  exit 1
fi

NAME=$1
NUMCLI=$2
PERCWRITES=$3
i=-1
x=0

# Those are dependend on the server infra, this is for 8 replicas.
id=(0 0 1 1 0 0 1 1)
server=(0 1 0 1 2 3 2 3)
#id=(0 1 2 0 1 2 0 1 2 0 1 2)
#server=(0 0 0 1 1 1 2 2 2 3 3 3)


j=0
for HOST in ${BAG_HOSTS} ; do
   echo "Running $NUMCLI clients on $ID (pid: $j, cluster: $i) with $PERCWRITES writes: ~/runclientA.sh $j $i $NUMCLI $PERCWRITES -$x >> clientOutput$j.txt";
   ssh $HOST "~/runclientA.sh $j ${id[$j]} $NUMCLI $PERCWRITES -${server[$j]} 2 >> clientOutput$j.txt" &
   ((j++))
done

#for HOST in ${BAG_HOSTS} ; do
#   echo "Running $NUMCLI clients on $ID (pid: $j, cluster: $i) with $PERCWRITES writes: ~/runclientA.sh" $
#   ssh $HOST "~/runclientA.sh $j ${id[$j]} $NUMCLI $PERCWRITES -${server[$j]} >> clientOutput$j.txt" &
#   ((j++))
#done



for t in {1..6}
do
  echo "Waiting $t minutes"
  sleep 60
done
echo "Finishing"
./killjava.sh
echo "Collecting results"
mkdir -p ~/results_arch/$NAME
i=0
  for HOST in ${BAG_HOSTS} ; do
    FN="$NUMCLI-$PERCWRITES-$i"
    scp $HOST:results$i.txt ~/results_arch/$NAME/results_$FN.txt
    ((i++))
  done
ls ~/results_arch/$NAME
echo "Finished"