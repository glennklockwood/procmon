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

for day in [18,19]:
    start=datetime(year, month, day)
    #end = datetime(2013, 12, 1)
    end = start + timedelta(days=1)
    idx = 0

    while start < end:
        tomorrow = start + timedelta(days=1)
        args = ["qsub", "-l", "h_rt=4:00:00", "-pe", "pe_16", "16", "-l", "ram.c=15G", "-wd", os.environ['BSCRATCH'], "procHunter.sh", start.strftime("%Y-%m-%d"), tomorrow.strftime("%Y-%m-%d"), "%s/processes%s" % (os.environ["GSCRATCH"], start.strftime("%Y%m%d")), "asdf", "asdf"]
        print args
        subprocess.call(args)
        start += timedelta(days=1)
        idx += 1
