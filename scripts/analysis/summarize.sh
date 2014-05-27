#!/bin/bash -l

module purge
module load PrgEnv-gnu/4.6
module load openmpi
module load mkl
module load python

. $HOME/procmonAnalysis/bin/activate
mpirun -n 8 python $HOME/git/nersc-procmon/scripts/analysis/workloadAnalysis_pd.py -f $HOME/git/nersc-procmon/scripts/analysis/workloadAnalysis.conf $@
