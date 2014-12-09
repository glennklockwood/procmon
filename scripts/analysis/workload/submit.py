#!/usr/bin/env python

import os
import sys
import subprocess
from datetime import datetime
from datetime import timedelta
import time
import time

year=2014
month=9
count=0

def write_script(start_time, prefix, jobcwd):
    prefix = "%s.%s" % (prefix, start_time.strftime("%Y%m%d%H%M%S"))
    fname = "%s.sh" % prefix
    fd = open(fname, 'w')
    fd.write("""#!/bin/bash -l
set -e
cd %s
if [ -e %s.summary.h5 ]; then
    rm %s.summary.h5
fi
/global/homes/d/dmj/git/nersc-procmon/build/src/ProcessSummarizer --config.file %s.summarizer.conf
status=$?
if [ $status -ne 0 ]; then
    mv %s.summary.h5 %s.FAILED.summary.h5
fi
exit $status
""" % (jobcwd, prefix, prefix, prefix, prefix, prefix)
    )
    fd.close()
    return fname

for month in range(6,7):
    for day in xrange(7):
        day += 1
        start=None
        try:
            start=datetime(year, month, day)
        except:
            pass
        if start is None:
            continue
        #end = datetime(2013, 12, 1)
        end = start + timedelta(days=1) - timedelta(seconds=1)
        idx = 0

        while start < end:
            tomorrow = start + timedelta(days=1)
            l_end = tomorrow - timedelta(seconds=1)
            args = ["python", "processSummarizer.py", "-f", "/global/homes/d/dmj/git/nersc-procmon/scripts/analysis/workload/test.conf", "--start", start.strftime("%Y%m%d%H%M%S"), "--end",  l_end.strftime("%Y%m%d%H%M%S")]
            print args
            subprocess.call(args)
            script_path = write_script(start, os.path.join(os.environ['GSCRATCH'], 'genepool_workload_v2'), os.environ['GSCRATCH'])
            jobargs = ["qsub", "-l", "exclusive.c", "-q", "mendel_test.q", script_path]
            subprocess.call(jobargs)
            start += timedelta(days=1)
            idx += 1
            count += 1
