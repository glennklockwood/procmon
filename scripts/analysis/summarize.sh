#!/bin/bash -l

module purge
module load uge
module load PrgEnv-gnu/4.6
module load openmpi
module load mkl
module load python

. $HOME/procmonAnalysis/bin/activate
mpirun python $HOME/git/nersc-procmon/scripts/analysis/workloadAnalysis_pd.py -f $HOME/git/nersc-procmon/scripts/analysis/workloadAnalysis.conf $@
