#!/usr/bin/env python

import os
import sys
import subprocess
from datetime import datetime
from datetime import timedelta
import time
import time

start = datetime(2014,5,1)
end = datetime(2014,5,28)
if True:
    idx = 0

    while start < end:
        tomorrow = start + timedelta(days=1)
        inputFile = '%s/rev2/processes%s_processes.h5' % (os.environ['GSCRATCH'], start.strftime("%Y%m%d"))
        outputFile = '%s/rev2/gpsummary2_%s.h5' % (os.environ['GSCRATCH'], start.strftime("%Y%m%d"))

        if os.path.exists(outputFile):
            start += timedelta(days=1)
            idx += 1
            continue

        args = ["qsub", "-l", "h_rt=12:00:00", "-l", "exclusive.c", "-wd", os.environ['BSCRATCH'], "summarize.sh", inputFile, "-o", outputFile]
        print args
        start += timedelta(days=1)
        idx += 1
        subprocess.call(args)
