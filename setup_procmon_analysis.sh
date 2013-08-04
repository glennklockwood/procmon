#!/bin/bash -l

set -e

module purge
module load PrgEnv-gnu/4.7
module load python/2.7.4
module load hdf5/1.8.11
module load uge
module load openmpi/1.6.4

INSTALLDIR=${1:-"undef"}
if [ $INSTALLDIR == "undef" ]; then
    echo "Must specify installation directory!"
    exit 1
fi
mkdir -p $INSTALLDIR/python
virtualenv $INSTALLDIR/python
. $INSTALLDIR/python/bin/activate
pip install numpy
pip install matplotlib
pip install numexpr
pip install Cython
pip install h5py
pip install tables
pip install pandas
pip install mpi4py
pip install ipython
pip install pymongo

mkdir -p $INSTALLDIR/scripts
mkdir -p $INSTALLDIR/bin
cp procFinder.py $INSTALLDIR/scripts
cat procHunter.sh.init | sed "s|___INSTALLDIR___|$INSTALLDIR|g" > $INSTALLDIR/bin/procHunter.sh
