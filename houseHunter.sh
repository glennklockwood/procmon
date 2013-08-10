#!/bin/bash -l
#$ -l exclusive.c
#$ -pe pe_16 16
#$ -l h_rt=1:00:00

## HouseHunter batch script
## Author: Doug Jacobsen <dmj@nersc.gov>
## Date  : 2013-06-10
##
## Takes three arguments (all optional)
##   1. Start datetime (YYYY-mm-dd HH:MM:SS)
##   2. End datetime   (YYYY-mm-dd HH:MM:SS)
##   3. save file prefix

set -e

## load the needed modules
module purge
module load PrgEnv-gnu/4.7
module load python/2.7.4
module load hdf5/1.8.11
module load uge
module load openmpi/1.6.4

## start the houseHunter virtualenv
. $HOME/houseHunter/bin/activate

export _LD_LIBRARY_PATH=$LD_LIBRARY_PATH

PROCMON_SCRIPTS=$HOME/genepool/procmon
PROCMON_SOURCE=/global/projectb/shared/data/genepool/procmon
PROCFINDER=$PROCMON_SCRIPTS/procFinder.py
MONGOSUMMARY=$PROCMON_SCRIPTS/mongoSummary.py
STARTDATE=`date -d yesterday +"%Y-%m-%d"`
ENDDATE=`date +"%Y-%m-%d"`

START=${1:-"${STARTDATE} 00:00:00"}
END=${2:-"${ENDDATE} 00:00:00"}

## re-calculate the dates/times with possible user input
echo ${START}
STARTFMT=`date -d "${START}" +"%Y%m%d%H%M%S"`
ENDFMT=`date -d "${END}" +"%Y%m%d%H%M%S"`
STARTDATE=`date -d "${START}" +"%Y-%m-%d"`
STARTDATE_NODASH=`date -d "${START}" +"%Y%m%d"`
STARTDATETIME=`date -d "${START}" +"%Y-%m-%d %H:%M:%S"`
ENDDATE=`date -d "${END}" +"%Y-%m-%d"`
ENDDATETIME=`date -d "${END}" +"%Y-%m-%d %H:%M:%S"`

## calculate (or accept) the save-file prefix
SAVEPREFIX=${3:-"${SCRATCH}/houseHunted.${STARTFMT}"}

## run qqacct to get the job accounting data
QQACCT_FILE=`mktemp $TMPDIR/qqacct.XXXXXXXXXX`
echo qqacct -S "$STARTDATE" -E "$ENDDATE" -q "last_good_end_time >= $STARTDATETIME && last_good_end_time < $ENDDATETIME" -c "user,project,job_number,task_number,hostname,h_rt,end-start,memory(ppn*h_vmem),memory(maxvmem),ppn,failed,exit_status" -t : -o $QQACCT_FILE
qqacct -S "$STARTDATE" -E "$ENDDATE" -q "last_good_end_time >= $STARTDATETIME && last_good_end_time < $ENDDATETIME" -c "user,project,job_number,task_number,hostname,h_rt,end-start,memory(ppn*h_vmem),memory(maxvmem),ppn,failed,exit_status" -t : -o $QQACCT_FILE

## run the procFinder with houseHunter options
time mpirun --bind-to-socket python $PROCFINDER --start $STARTFMT --end $ENDFMT --save-prefix $SAVEPREFIX --qqacct-data $QQACCT_FILE exePath '^(/chos)?/house'

time python $MONGOSUMMARY houseHunter $SAVEPREFIX.summary.h5 $STARTDATE_NODASH
