#!/bin/bash

module purge
module load PrgEnv-gnu/4.8
mkdir -p /scratch/procmon
cd /scratch/procmon
remoteloc=/global/projectb/shared/data/genepool/procmon_ldisk
mkdir -p $remoteloc
killtime=0
pid=""
umask 022
ulimit -v 20971520

cmd="/global/homes/d/dmj/genepool/procmon/ProcReducer.um --statblock=8359 --datablock=957"
wait=3600
while [ 1 ]; do
    currtime=`date +"%s"`
    ps=""
    if [ $currtime -ge $killtime ]; then
        if [ "x$pid" != "x" ]; then
            echo "about to kill $pid"
            file=`ls`
            oldpid=$pid
            kill $pid
            $cmd &
            pid=$!
            killtime=$(( $currtime + $wait ))
            wait $oldpid
            mv $file $remoteloc/
        fi
    fi        
    ps=`ps aux | grep ProcReducer | grep -v grep`
    if [ "x$ps" == "x" ]; then
        echo "starting failsafe"
	    $cmd &
        pid=$!
        killtime=$(( $currtime + $wait ))
    fi
    sleep 5
done
