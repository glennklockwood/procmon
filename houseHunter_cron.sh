#!/bin/bash -l
module load uge
qsub -wd $SCRATCH $HOME/genepool/procmon/houseHunter.sh
