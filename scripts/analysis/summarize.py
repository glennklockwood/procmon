#!/usr/bin/env python

import os
import sys
import subprocess
from datetime import datetime
from datetime import timedelta
import time
import time

start = datetime(2013,12,3)
end = datetime(2014,12,5)
if True:
    idx = 0

    while start < end:
        tomorrow = start + timedelta(days=1)
        inputFile = '%s/genepool_workload_v2.%s000000.summary.h5' % (os.environ['GSCRATCH'], start.strftime("%Y%m%d"))
        outputFile = '%s/genepool_workload_v2.%s.agg2.h5' % (os.environ['GSCRATCH'], start.strftime("%Y%m%d"))

        if os.path.exists(outputFile):
            start += timedelta(days=1)
            idx += 1
            continue

        args = ["qsub", "-l", "h_rt=12:00:00", "-l", "exclusive.c", "-q", "mendel_test.q", "-cwd", "summarize.sh", inputFile, "-o", outputFile]
        print args
        start += timedelta(days=1)
        idx += 1
        subprocess.call(args)
