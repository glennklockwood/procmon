#!/bin/bash

# make a debian package, run from genepool/procmon

BUILDDIR=$BSCRATCH/procmon-build
[ -d "$BUILDDIR" ] || mkdir -p "$BUILDDIR"

module purge
module load PrgEnv-gnu/4.8
module load rabbitmq-c/0.4.0_nossl
module load boost/1.54.0
module load hdf5/1.8.11
make -f Makefile clean
make -f Makefile GENEPOOL=1 SECURED=1

DIRS="usr etc"
mkdir -p $BUILDDIR/usr/sbin
mkdir -p $BUILDDIR/etc/cron.d
cp runprocmon procmon ProcReducer $BUILDDIR/usr/sbin

cat >$BUILDDIR/etc/cron.d/procmon <<EOF
@reboot root test if /usr/sbin/runprocmon && /usr/sbin/runprocmon
0 * * * * root test -f /usr/sbin/runprocmon && /usr/sbin/runprocmon
EOF

VER=$(awk '/^#define PROCMON_VERSION/{print $3}' procmon.hh | sed 's/"//g')

cd $BUILDDIR
TFILE=procmon.tar.gz
rm -f $TFILE
tar zcvf $TFILE $DIRS
DESC="procmon - NERSC process monitor"
fakeroot alien -g --version=$VER --description="$DESC" $TFILE

DEST=procmon-$VER
cat >$DEST/debian/control <<EOF
Source: procmon
Section: alien
Priority: extra
Maintainer: Doug Jacobsen <DMJacobsen@lbl.gov>, Matt Dunford <mdunford@lbl.gov>

Package: procmon
Architecture: amd64
Depends: \${shlibs:Depends}
Description:
 $DESC
 .
EOF

cd $DEST
dpkg-buildpackage -rfakeroot -b -uc -us
