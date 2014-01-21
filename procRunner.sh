#!/bin/bash

module purge
module load PrgEnv-gnu/4.8
module load boost/1.54.0
module load hdf5/1.8.11
ulimit -v 20971520
umask 022
cd /global/projectb/shared/data/genepool/procmon

cmd="/global/homes/d/dmj/genepool/procmon/ProcReducer -c 20 -m 3600"
psCmd="ps aux | grep blaghr | grep -v grep"
wait=3600
while [ 1 ]; do
ps=`ps aux | grep ProcReducer | grep -v grep`
if [ "x$ps" == "x" ]; then
	echo "$cmd &"
	$cmd &
	pid=$$
else
	echo "running"
fi
sleep $wait
done
