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

for month in range(7,11):
    for day in xrange(31):
        day += 1
        start=datetime(year, month, day)
        #end = datetime(2013, 12, 1)
        end = start + timedelta(days=1)
        idx = 0

        while start < end:
            tomorrow = start + timedelta(days=1)
            args = ["qsub", "-l", "h_rt=4:00:00", "-pe", "pe_8", "8", "-l", "ram.c=5G", "-wd", os.environ['BSCRATCH'], "procHunter.sh", start.strftime("%Y-%m-%d"), tomorrow.strftime("%Y-%m-%d"), "%s/garbage/processes%s" % (os.environ["GSCRATCH"], start.strftime("%Y%m%d")), "asdf", "asdf"]
            print args
            subprocess.call(args)
            start += timedelta(days=1)
            idx += 1
            count += 1
            if count == 30:
                time.sleep(15)
                count = 0
