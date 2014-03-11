#!/usr/bin/env python

import numpy
import h5py
import sys
import os
import pandas
from datetime import date,datetime,timedelta,time
import re
import cPickle
import pwd
import subprocess
import tempfile

def get_job_data(start, end):
	ret = -1
	qqacct_data = None
	(output_fd, outputFile) = tempfile.mkstemp(prefix="qqacct_data")
	os.close(output_fd)
	qqacct_exec = ['qqacct','-S',start.strftime('%Y-%m-%d'),'-E',end.strftime('%Y-%m-%d'),'-q','last_good_end_time >= %s && last_good_end_time < %s' % (start.strftime('%Y-%m-%d %H:%M:%S'), end.strftime('%Y-%m-%d %H:%M:%S')), '-c', 'user,project,job_number,task_number,hostname,h_rt,end-start,memory(ppn*h_vmem),memory(maxvmem),ppn,failed,exit_status', '-t', ':', '-o', outputFile]
	print " ".join(qqacct_exec)
	try:
		ret = subprocess.call(qqacct_exec)
		if ret == 0:
			return (ret, outputFile)
	except:
		pass
	return (ret, None)

def usage(ret):
	print "houseHunter.py [--h5-path <path=/global/projectb/shared/data/genepool/procmon>] [--start YYYYMMDDHHMMSS] [--end YYYYMMDDHHMMSS] [--h5-prefix <h5prefix=procmon_genepool>] [--localize <local-path>] --save-prefix <...>"
	print "  Start time defaults to yesterday at Midnight (inclusive)"
	print "  End time defaults to today at Midnight (non-inclusive)"
	print "  Therefore, the default is to process all the files from yesterday!"
	sys.exit(ret)

def main(args):
	## initialize configurable variables
	yesterday = date.today() - timedelta(days=1)
	start_time = datetime.combine(yesterday, time(0,0,0))
	end_time = datetime.combine(date.today(), time(0,0,0))
	h5_path = "/global/projectb/shared/data/genepool/procmon"
	h5_prefix = "procmon_genepool"
	localize = None
	save_prefix = None

	## parse command line arguments
	i = 0
	while i < len(args):
		if args[i] == "--start":
			i += 1
			if i < len(args):
				try:
					start_time = datetime.strptime(args[i], "%Y%m%d%H%M%S")
				except:
					usage(1)
			else:
				usage(1)
		if args[i] == "--end":
			i += 1
			if i < len(args):
				try:
					end_time = datetime.strptime(args[i], "%Y%m%d%H%M%S")
				except:
					usage(1)
			else:
				usage(1)
		if args[i] == "--h5-path":
			i += 1
			if i < len(args):
				h5_path = args[i]
				if not os.path.exists(h5_path):
					print "%s doesn't exist!" % (h5_path)
					usage(1)
			else:
				usage(1)
		if args[i] == "--localize":
			i += 1
			if i < len(args):
				localize = args[i]
				if not os.path.exists(localize):
					print "%s doesn't exist!" % (localize)
					usage(1)
			else:
				usage(1)
		if args[i] == "--h5-prefix":
			i += 1
			if i < len(args):
				h5_prefix = args[i]
			else:
				usage(1)
		if args[i] == "--save-prefix":
			i += 1
			if i < len(args):
				save_prefix = args[i]
			else:
				usage(1)
		if args[i] == "--help":
			usage(0)
		i += 1

	## get list of files
	filename_hash = {}
	all_files = os.listdir(h5_path)
	pattern = "%s\.([0-9]+)\.h5" % (h5_prefix)
	regex = re.compile(pattern)
	for filename in all_files:
		f_match = regex.match(filename)
		if f_match is not None:
			currts = datetime.strptime(f_match.group(1), "%Y%m%d%H%M%S")
			if currts >= start_time and currts < end_time:
				filename_hash[currts] = os.path.join(h5_path, filename)
	
	keys = sorted(filename_hash.keys())
	filenames = [filename_hash[x] for x in keys]

	if len(filenames) == 0:
		print "No procmon data file found in the specified path."
		sys.exit(1)

	if save_prefix is None:
		print "must specify --save-prefix <...>"
		sys.exit(1)
	
	status = 0
	if localize is not None:
		print "localizing data to %s" % localize
		cpArgs = ['cp']
		cpArgs.extend(filenames)
		cpArgs.append(localize)
		status = subprocess.call(cpArgs)
		h5_path = localize

	print "getting qqacct data between %s and %s" % (str(start_time), str(end_time))
	(q_status, qqacct_file) =  get_job_data(start_time,end_time)

	if q_status == 0 and status == 0:
		scriptpath = os.path.split(os.path.realpath(__file__))[0]
		args = ['mpirun','--bind-to-socket','python',os.path.join(scriptpath,'houseHunter2.py'),'--start',start_time.strftime("%Y%m%d%H%M%S"),'--end',end_time.strftime("%Y%m%d%H%M%S"),'--h5-path',h5_path,'--h5-prefix',h5_prefix,'--qqacct-data',qqacct_file,'--save-prefix',save_prefix,'exePath','^(/chos)?/house']
		status = subprocess.call(args)

if __name__ == "__main__":
	main(sys.argv[1:])
