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

def get_host_processes(hostname, fd):
	hostgroup = fd[hostname]
	procdata = pandas.DataFrame(hostgroup['procdata'][...])
	procdata['key_startTime'] = procdata['startTime']
	procdata = procdata.sort('recTime', ascending=0).set_index(['pid','key_startTime'])
	procstat = pandas.DataFrame(hostgroup['procstat'][...])
	procstat['key_startTime'] = procstat['startTime']
	procstat = procstat.sort('recTime', ascending=0).set_index(['pid','key_startTime'])
	
	t_data = None

	## for just /house processes
	if not procdata.empty:
		pd_houseExe = procdata.exePath.str.contains('^(/chos)?/house')
		#pd_houseCwd = procdata.cwdPath.str.contains('^(/chos)?/house')
		house_process_idx = pd_houseExe # | pd_houseCwd
		house_processes = procdata[house_process_idx]
		if not house_processes.empty:
			pd_house = house_processes.groupby(level=[0,1]).first()
			ps_house = procstat.ix[pd_house.index].groupby(level=[0,1]).first()
			t_data = pd_house.join(ps_house, rsuffix='_ps')
			t_data['host'] = hostname
			t_data = t_data.set_index('host', append=True)

	## for all processes
	#if not procdata.empty:
	#	pd = procdata.groupby(level=[0,1]).first()
	#	ps = procstat.ix[pd.index].groupby(level=[0,1]).first()
	#	t_data = pd.join(ps, rsuffix='_ps')
	#	t_data['host'] = hostname
	#	t_data = t_data.set_index('host', append=True)
	return t_data
	

def parse_h5(filename, id_processes):
	print "parsing: %s" % filename
	fd = h5py.File(filename, 'r')

	totalRecords = 0
	finalRecords = 0
	for hostname in fd:
		hostdata = get_host_processes(hostname, fd)
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
					newdata = hostdata.ix[addList]
					id_processes[hostname] = pandas.concat([id_processes[hostname], newdata], axis=0)
					#id_processes[hostname] = id_processes[hostname].append(hostdata.ix[addList])
				finalRecords += id_processes[hostname].shape[0]
		print hostname, records, finalRecords
		totalRecords += records
	finalRecords = 0
	for hostname in id_processes:
		finalRecords += id_processes[hostname].shape[0]

	print "===== %d; %d =====" % (totalRecords, finalRecords)

def get_processes(filenames):
	status = 0
	processes = None

	host_processes = {}
	for filename in filenames:
		parse_h5(filename, host_processes)

	## merge all processes into a single data table
	host_process_list = []
	for host in host_processes:
		host_process_list.append(host_processes[host])
	processes = pandas.concat(host_process_list, axis=0)

	## clean up paths to make equivalent and understandable
	processes.exePath = processes.exePath.str.replace("^/chos","",case=False)
	processes.exePath = processes.exePath.str.replace("^/house/tooldirs/jgitools/Linux_x86_64","/jgi/tools",case=False)
	processes.exePath = processes.exePath.str.replace("^/syscom/os/deb6gp","",case=False)
	processes.exePath = processes.exePath.str.replace("^/tlphoebe/genepool","/usr/syscom",case=False)
	processes.cwdPath = processes.cwdPath.str.replace("^/chos","",case=False)
	processes.cwdPath = processes.cwdPath.str.replace("^/house/tooldirs/jgitools/Linux_x86_64","/jgi/tools",case=False)
	processes.cwdPath = processes.cwdPath.str.replace("^/syscom/os/deb6gp","",case=False)
	processes.cwdPath = processes.cwdPath.str.replace("^/tlphoebe/genepool","/usr/syscom",case=False)

	return (status,processes)

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
			qqacct_data = pandas.read_table(outputFile, sep=':', header=None, names=['user','project','job','task','hostname','h_rt','wall','hvmem','maxvmem','ppn','sge_status','exit_status'])
	except:
		pass
	return (ret, qqacct_data)

def find_script(args):
	for arg in args:
		if arg[0] != "-":
			return arg
	return None

def identify_scripts(processes):
	executables = processes.exePath.unique()
	prefixes = ["perl","python","ruby","bash","sh","tcsh","csh"]
	processes['scripts'] = None
	for exe in executables:
		executable = os.path.split(exe)[1]
		scriptable = False
		for prefix in prefixes:
			pattern = "^%s.*" % prefix
			if re.match(pattern, executable) is not None:
				scriptable = True
		if scriptable:
			selection = processes.exePath == exe
			subset = processes.ix[selection]
			args = subset.cmdArgs.str.split("|")
			scripts = args.apply(lambda x: find_script(x[1::]))
			processes.scripts[selection] = scripts

def identify_users(processes):
	uids = processes.realUid.unique()
	processes['username'] = None
	user_hash = {}
	for uid in uids:
		uid_processes_idx = processes.realUid == uid
		try:
			uid = int(uid)
			if uid not in user_hash:
				user_hash[uid] = pwd.getpwuid(uid)
			processes.username[uid_processes_idx] = user_hash[uid].pw_name
		except:
			pass
	return user_hash

def integrate_job_data(processes, qqacct_data):
	qqacct_data.job = qqacct_data.job.astype('str')
	qqacct_data.task[qqacct_data.task == 0] = 1
	qqacct_data.task = qqacct_data.task.astype('str')
	# get the last state for each job/task/hostname combination that was observed
	qqacct_data = qqacct_data.groupby(['job']).last().reset_index()
	processes = processes.merge(right=qqacct_data[['job','project']], how='left', left_on='identifier', right_on='job', suffixes=['','qq'])
	return processes
	
def summarize_processes(group, key):
	ret = pandas.DataFrame({
			'key'  :     group[key].reset_index()[key],
			'cpu_time' : (group.utime + group.stime).sum() / 100.,
			'walltime' : (group.recTime - group.startTime).sum(),
			'nobs'     : group.utime.count()
	 }, index=[key])
	del ret['key']
	return ret

def summarize_data(processes, summaries=['executables','execUser','execProject','scripts','scriptUser','scriptProject','projects','users']):
	ret = {}
	if 'executables' in summaries:
		groups = processes.groupby('exePath')
		summary = groups.apply(lambda x: summarize_processes(x,'exePath'))
		ret['executables'] = summary
	if 'execUser':
		groups = processes.groupby(['username','exePath'])
		summary = groups.apply(lambda x: summarize_processes(x, ['username','exePath']))
		ret['execUser'] = summary
	if 'execProject':
		groups = processes.groupby(['project','exePath'])
		summary = groups.apply(lambda x: summarize_processes(x, ['project','exePath']))
		ret['execProject'] = summary
	if 'scripts' in summaries:
		groups = processes.groupby('scripts')
		summary = groups.apply(lambda x: summarize_processes(x, 'scripts'))
		ret['scripts'] = summary
	if 'scriptUser':
		groups = processes.groupby(['username','scripts'])
		summary = groups.apply(lambda x: summarize_processes(x, ['username','scripts']))
		ret['scriptUser'] = summary
	if 'scriptProject':
		groups = processes.groupby(['project','scripts'])
		summary = groups.apply(lambda x: summarize_processes(x, ['project','scripts']))
		ret['scriptProject'] = summary
	if 'projects' in summaries:
		groups = processes.groupby("project")
		summary = groups.apply(lambda x: summarize_processes(x, 'project'))
		ret['projects'] = summary
	if 'users' in summaries:
		groups = processes.groupby("username")
		summary = groups.apply(lambda x: summarize_processes(x, 'username'))
		ret['users'] = summary
	return ret

def usage(ret):
	print "houseHunter.py [--h5-path <path=/global/projectb/shared/data/genepool/procmon>] [--start YYYYMMDDHHMMSS] [--end YYYYMMDDHHMMSS] [--prefix <h5prefix=procmon_genepool>] [--load <intr_file>] [--save <intr_file>]"
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
	load_intermediate = None
	save_intermediate = None

	## parse command line arguments
	i = 0
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

	q_status = 0
	h_status = 0
	user_hash = None
	summaries = None
	if load_intermediate is not None:
		print "Loading from intermediate data file: %s" % load_intermediate
		(processes,qqacct_data,user_hash) = cPickle.load(open(load_intermediate, 'rb'))

	else:
		print "getting qqacct data between %s and %s" % (str(start_time), str(end_time))
		(q_status, qqacct_data) =  get_job_data(start_time,end_time)
		print "getting process data (may take a long time):"
		(h_status, processes) = get_processes(filenames)
		identify_scripts(processes)
		identify_users(processes)
		processes = integrate_job_data(processes, qqacct_data)
		summaries = summarize_data(processes)

	if save_intermediate is not None:
		print "Saving to intermediate data file: %s" % save_intermediate
		h5file = "%s.summary.h5" % save_intermediate
		for key in summaries.keys():
			summaries[key].to_hdf(h5file, key)
		cPickle.dump((processes,qqacct_data,user_hash), open(save_intermediate, 'wb'))
		processes.to_hdf(save_intermediate, "processes")
		#qqacct_data.to_hdf(save_intermediate, "qqacct")

	if q_status == 0 and h_status == 0:
		pass

if __name__ == "__main__":
	main(sys.argv[1:])
	pass
