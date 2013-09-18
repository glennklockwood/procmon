#!/bin/bash

# make a debian package, run from genepool/procmon

BUILDDIR=$BSCRATCH/procmon-build
rm -rf $BUILDDIR
[ -d "$BUILDDIR" ] || mkdir -p "$BUILDDIR"

module purge
module load PrgEnv-gnu/4.8
module load rabbitmq-c/0.4.0_nossl
module load boost/1.54.0
module load hdf5/1.8.11
make clean
make GENEPOOL=1 SECURED=1

DIRS="usr etc"
mkdir -p $BUILDDIR/usr/sbin
cp runprocmon procmon $BUILDDIR/usr/sbin
cp -r debian $BUILDDIR/

VER=$(awk '/^#define PROCMON_VERSION/{print $3}' procmon.hh | sed 's/"//g')

cd $BUILDDIR
TFILE=procmon.tar.gz
rm -f $TFILE
tar zcvf $TFILE $DIRS
DESC="procmon - NERSC process monitor"
fakeroot alien -g --version=$VER --description="$DESC" $TFILE

DEST=procmon-$VER
echo 8 >$DEST/debian/compat
cp debian/* $DEST/debian/

cat >$DEST/debian/control <<EOF
Source: procmon
Section: alien
Priority: extra
Maintainer: Doug Jacobsen <DMJacobsen@lbl.gov>, Matt Dunford <mdunford@lbl.gov>

Package: procmon
Architecture: amd64
Depends: bash
Description:
 $DESC
 .
EOF

cd $DEST
dpkg-buildpackage -rfakeroot -b -uc -us
