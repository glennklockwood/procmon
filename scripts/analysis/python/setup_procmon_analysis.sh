#!/bin/sh -ef

INSTALLDIR=${1:-"undef"}
if [ $INSTALLDIR == "undef" ]; then
    echo "Must specify installation directory!"
    exit 1
fi
pwd
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
python setup.py install
