#!/bin/bash -l

module purge
module load PrgEnv-gnu/4.6
module load mkl
module load python
module load slurm
module load slurm/openmpi/1.6.5

. $HOME/procmonAnalysis/bin/activate
srun python $HOME/git/nersc-procmon/scripts/analysis/workloadAnalysis_pd.py -f $HOME/git/nersc-procmon/scripts/analysis/workloadAnalysis.conf $@
