#!/bin/bash -l
#$ -l exclusive.c

## load the needed modules
module purge
module load PrgEnv-gnu/4.6
module load python/2.7.4
module load hdf5/1.8.11
module load openmpi/1.6.3
module load uge

## start the houseHunter virtualenv
. $HOME/houseHunter/bin/activate

PROCMON_SOURCE=/global/projectb/shared/data/genepool/procmon
HOUSEHUNTER=$HOME/genepool/procmon/houseHunter_orig.py
CURRDATE=`date -d yesterday +"%Y%m%d"`
ENDDATE=`date +"%Y%m%d"`

START=${1:-"${CURRDATE}000000"}
END=${2:-"${ENDDATE}000000"}
SAVEPREFIX=${3:-"${SCRATCH}/houseHunted.${START}"}

echo "START: $START"
echo "END:   $END"
echo "SAVEPREFIX: $SAVEPREFIX"

time $HOUSEHUNTER --start $START --end $END --save $SAVEPREFIX
