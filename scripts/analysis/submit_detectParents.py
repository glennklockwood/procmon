#!/usr/bin/env python

import os
import sys
import subprocess
from datetime import datetime
from datetime import timedelta
import time
import time

year=2014
month=5

for day in xrange(1,17):
    start=datetime(year, month, day)
    #end = datetime(2013, 12, 1)
    end = start + timedelta(days=1)
    idx = 0

    while start < end:
        tomorrow = start + timedelta(days=1)
        start_time = '%s000000' % (start.strftime("%Y%m%d"))
        outputFile = '%s/parents_%s.h5' % (os.environ['GSCRATCH'], start.strftime("%Y%m%d"))

        args = ["qsub", "-l", "h_rt=4:00:00", "-wd", os.environ['BSCRATCH'], "detectParents.sh", start_time, outputFile]
        print args
        subprocess.call(args)
        start += timedelta(days=1)
        idx += 1
