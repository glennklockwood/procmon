#!/usr/bin/env python

import numpy
import h5py
import sys
import os
import pandas
from datetime import date,datetime,timedelta,time
import re
import cPickle

id_processes = {}

def get_host_processes_on_house(hostname, fd):
	hostgroup = fd[hostname]
	procdata = pandas.DataFrame(hostgroup['procdata'][...]).sort('recTime', ascending=0).set_index(['pid','startTime'])
	procstat = pandas.DataFrame(hostgroup['procstat'][...]).sort('recTime', ascending=0).set_index(['pid','startTime'])
	t_data = None
	if not procdata.empty:
		pd_houseExe = procdata.exePath.str.contains('^(/chos)?/house')
		pd_houseCwd = procdata.cwdPath.str.contains('^(/chos)?/house')
		house_process_idx = pd_houseExe | pd_houseCwd
		house_processes = procdata[house_process_idx]
		if not house_processes.empty:
			pd_house = house_processes.groupby(level=[0,1]).first()
			ps_house = procstat.ix[pd_house.index].groupby(level=[0,1]).first()
			t_data = pd_house.join(ps_house, rsuffix='_ps')
			t_data['host'] = hostname
			t_data = t_data.set_index('host', append=True)
	return t_data
	

def parseH5(filename):
	global id_processes
	print "parsing: %s" % filename
	fd = h5py.File(filename, 'r')

	totalRecords = 0
	finalRecords = 0
	for hostname in fd:
		hostdata = get_host_processes_on_house(hostname, fd)
		records = 0
		if hostdata is not None and not hostdata.empty:
			records = hostdata.shape[0]
			if hostname not in id_processes:
				id_processes[hostname] = hostdata
			else:
				replaceList = id_processes[hostname].index.isin(hostdata.index)
				if len(replaceList) > 0 and len(id_processes[hostname].index[replaceList]) > 0:
					id_processes[hostname].ix[replaceList] = hostdata.ix[id_processes[hostname].index[replaceList]]
				addList = numpy.invert(hostdata.index.isin(id_processes[hostname].index))
				if len(addList) > 0:
					id_processes[hostname] = id_processes[hostname].append(hostdata.ix[addList])
				finalRecords += id_processes[hostname].shape[0]
		print hostname, records, finalRecords
		totalRecords += records
	finalRecords = 0
	for hostname in id_processes:
		finalRecords += id_processes[hostname].shape[0]

	print "===== %d; %d =====" % (totalRecords, finalRecords)



def usage(ret):
	print "houseHunter.py [--h5-path <path=/global/projectb/shared/data/genepool/procmon>] [--start YYYYMMDDHHMMSS] [--end YYYYMMDDHHMMSS] [--prefix <h5prefix=procmon_genepool>] [--load <intr_file>] [--save <intr_file>]"
	print "  Start time defaults to yesterday at Midnight (inclusive)"
	print "  End time defaults to today at Midnight (non-inclusive)"
	print "  Therefore, the default is to process all the files from yesterday!"
	sys.exit(ret)

if __name__ == "__main__":
	## initialize configurable variables
	yesterday = date.today() - timedelta(days=1)
	start_time = datetime.combine(yesterday, time(0,0,0))
	end_time = datetime.combine(date.today(), time(0,0,0))
	h5_path = "/global/projectb/shared/data/genepool/procmon"
	h5_prefix = "procmon_genepool"
	load_intermediate = None
	save_intermediate = None

	## parse command line arguments
	i = 1
	while i < len(sys.argv):
		if sys.argv[i] == "--start":
			i += 1
			if i < len(sys.argv):
				try:
					start_time = datetime.strptime(sys.argv[i], "%Y%m%d%H%M%S")
				except:
					usage(1)
			else:
				usage(1)
		if sys.argv[i] == "--end":
			i += 1
			if i < len(sys.argv):
				try:
					end_time = datetime.strptime(sys.argv[i], "%Y%m%d%H%M%S")
				except:
					usage(1)
			else:
				usage(1)
		if sys.argv[i] == "--h5-path":
			i += 1
			if i < len(sys.argv):
				h5_path = sys.argv[i]
				if not os.path.exists(h5_path):
					print "%s doesn't exist!" % (h5_path)
					usage(1)
			else:
				usage(1)
		if sys.argv[i] == "--save":
			i += 1
			if i < len(sys.argv):
				save_intermediate = sys.argv[i]
			else:
				usage(1)
		if sys.argv[i] == "--load":
			i += 1
			if i < len(sys.argv):
				load_intermediate = sys.argv[i]
				if not os.path.exists(load_intermediate):
					print "%s doesn't exist!" % (load_intermediate)
					usage(1)
			else:
				usage(1)
		if sys.argv[i] == "--help":
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
	
	filenames = []
	keys = sorted(filename_hash.keys())
	for key in keys:
		filenames.append(filename_hash[key])
	id_processes = {}
	if load_intermediate is not None:
		id_processes = cPickle.load(open(load_intermediate, 'rb'))

	if len(id_processes.keys()) == 0:
		for filename in filenames:
			parseH5(filename)

	if save_intermediate is not None:
		cPickle.dump(id_processes, open(save_intermediate, 'wb'))

	

