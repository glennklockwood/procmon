#!/bin/bash

module purge
module load PrgEnv-gnu/4.8
ulimit -v 20971520
umask 022
cd /global/projectb/shared/data/genepool/procmon

cmd="/global/homes/d/dmj/genepool/procmon/ProcReducer.um --statblock=8359 --datablock=957"
psCmd="ps aux | grep blaghr | grep -v grep"
wait=3600
while [ 1 ]; do
ps=`ps aux | grep ProcReducer | grep -v grep`
if [ "x$ps" == "x" ]; then
	echo "$cmd &"
	$cmd &
	pid=$$
	sleep $wait
	killall ProcReducer.um
else
	echo "running"
fi
sleep 5
done
