#!/bin/bash -l

module purge
module load PrgEnv-gnu
module load python
module load openmpi
module load mkl
module load hdf5-parallel/1.8.12

INSTALLDIR=${1:-"undef"}
if [ $INSTALLDIR == "undef" ]; then
    echo "Must specify installation directory!"
    exit 1
fi
pwd
mkdir -p $INSTALLDIR/python
virtualenv $INSTALLDIR/python
. $INSTALLDIR/python/bin/activate

wget http://downloads.sourceforge.net/project/numpy/NumPy/1.8.1/numpy-1.8.1.tar.gz
tar xf numpy-1.8.1.tar.gz
cd numpy-1.8.1
cat > site.cfg << EOF
[mkl]
library_dirs = $MKL_DIR/lib/intel64
include_dirs = $MKL_DIR/include
mkl_libs = mkl_rt
lapack_libs =
EOF
python setup.py build
python setup.py install
cd ..
# need bleeding edge h5py
git clone https://github.com/h5py/h5py.git
pip install Cython
pip install mpi4py
cd h5py
OLDCC=$CC
export CC=mpicc
cd h5py
cython *.pyx
cd ..
python setup.py build --mpi=yes
python setup.py install
export CC=$OLDCC

chmod -R u+w numpy-1.8.1 h5py
rm -r numpy-1.8.1 h5py

pip install matplotlib
pip install numexpr
pip install tables
pip install pandas
pip install pymongo
python setup.py install
