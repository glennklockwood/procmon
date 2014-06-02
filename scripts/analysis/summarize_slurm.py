#!/usr/bin/env python

import os
import sys
import subprocess
from datetime import datetime
from datetime import timedelta
import time
import time

start = datetime(2014,1,1)
end = datetime(2014,5,1)
if True:
    idx = 0

    while start < end:
        tomorrow = start + timedelta(days=1)
        inputFile = '%s/rev2/processes%s_processes.h5' % (os.environ['GSCRATCH'], start.strftime("%Y%m%d"))
        outputFile = '%s/rev2/summary%s.h5' % (os.environ['GSCRATCH'], start.strftime("%Y%m%d"))

        args = ["sbatch", "-t", "720", "-N", "1", "-n", "8", "-D", os.environ['BSCRATCH'], "-o", "%s/summarize_slurm.%%A.out" % os.environ['BSCRATCH'], "summarize_slurm.sh", inputFile, "-o", outputFile]
        print args
        subprocess.call(args)
        start += timedelta(days=1)
        idx += 1
