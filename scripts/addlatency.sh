#!/usr/bin/env bash
ITF=eth0
HOSTS="172.16.52.3 172.16.52.4 172.16.52.6 172.16.52.7 172.16.52.9 172.16.52.20 172.16.52.22 172.16.52.23"
FILTER="sudo-g5k tc filter add dev $ITF protocol ip parent 1: prio 1 u32"
DELAY=20ms
JITTER=5ms

stop(){

    #delete possible existing rules
    sudo-g5k tc qdisc del dev $ITF root

    echo "Deleted rule"

}

start(){

    #set base handle for the rules
    sudo-g5k tc qdisc add dev $ITF root handle 1: prio
    sudo-g5k tc qdisc add dev $ITF parent 1:1 handle 2: netem delay $DELAY $JITTER distribution normal

    #set filter
    for HOST in ${HOSTS} ; do
        $FILTER match ip dst $HOST flowid 1:1
    done

    echo "NETEM rule added"

}

show(){

    sudo-g5k tc -s qdisc ls dev $ITF

}

usage(){

    echo "$0 [-i <interface>] [-d <delay>] [-j <jitter>] [start|stop|show]"
    exit 1
}

while getopts ":hi:d:j:" opt; do
    case "${opt}" in
        i)
            ITF=${OPTARG}
            ;;

        d)
            DELAY=${OPTARG}
            ;;

        j)
            JITTER=${OPTARG}
            ;;

        h | *)
            usage
            ;;

    esac
done
shift $((OPTIND-1))

case "$1" in

    start)
        start
        ;;
    stop)
        stop
        ;;
    show)
        show
        ;;
    *)
        usage
        ;;
esac

exit 0
