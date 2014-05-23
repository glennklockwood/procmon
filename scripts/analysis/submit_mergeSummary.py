#!/usr/bin/env python

import os
import sys
import subprocess
from datetime import datetime
from datetime import timedelta
import time
import time

year=2013

for month in xrange(11,13):
    args = ["qsub", "-l", "h_rt=4:00:00,ram.c=120G", "-wd", os.environ['BSCRATCH'], "mergeSummary.sh", "-o", "monthly_summary_%04d%02d.h5" % (year,month), "%s/summary%04d%02d*.h5" % (os.environ['GSCRATCH'], year, month)]
    print args
    subprocess.call(args)
