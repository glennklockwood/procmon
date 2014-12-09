#!/usr/bin/env python

import os
import sys
import subprocess
from datetime import datetime
from datetime import timedelta
import time
import time

start = datetime(2013,12,11)
end = datetime(2014,12,5)
if True:
    idx = 0

    while start < end:
        tomorrow = start + timedelta(days=1)
        inputFile = '%s/genepool_workload_v2.%s.agg.h5' % (os.environ['GSCRATCH'], start.strftime("%Y%m%d"))
        outputFile1 = '%s/genepool_workload_v2.%s.generalized.h5' % (os.environ['GSCRATCH'], start.strftime("%Y%m%d"))

        if not os.path.exists(outputFile1):
            args = ['qsub', '-q', 'mendel_test.q', '-l', 'exclusive.c', '-cwd', 'generalize.sh', '-s', 'useful', inputFile, '-o', outputFile1]
            print args
            subprocess.call(args)
#args = ["sbatch", "-t", "720", "-N", "1", "-n", "8", "-D", os.environ['BSCRATCH'], "generalize.sh", "-s", "useful", inputFile, "-o", outputFile1]  
#            print args
#            subprocess.call(args)

#if not os.path.exists(outputFile2):
#            args = ["sbatch", "-t", "720", "-N", "1", "-n", "8", "--cpus-per-task=1", "-D", os.environ['BSCRATCH'], "generalize.sh", "-s", "usefulpadded", inputFile, "-o", outputFile2]
#            print args
#            subprocess.call(args)

        start += timedelta(days=1)
        idx += 1
