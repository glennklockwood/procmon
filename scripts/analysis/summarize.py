#!/usr/bin/env python

import os
import sys
import subprocess
from datetime import datetime
from datetime import timedelta
import time
import time

start = datetime(2013,11,1)
end = datetime(2014,5,1)
if True:
    idx = 0

    while start < end:
        tomorrow = start + timedelta(days=1)
        inputFile = '%s/processes%s_processes.h5' % (os.environ['GSCRATCH'], start.strftime("%Y%m%d"))
        outputFile = '%s/summary%s.h5' % (os.environ['GSCRATCH'], start.strftime("%Y%m%d"))

        args = ["qsub", "-l", "h_rt=4:00:00", "-wd", os.environ['BSCRATCH'], "summarize.sh", inputFile, "-o", outputFile]
        print args
        subprocess.call(args)
        start += timedelta(days=1)
        idx += 1
