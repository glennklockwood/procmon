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
from mpi4py import MPI

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

def get_host_processes(hostname, fd, query):
	hostgroup = fd[hostname]
	procdata = pandas.DataFrame(hostgroup['procdata'][...])
	procdata['key_startTime'] = procdata['startTime']
	procdata = procdata.sort('recTime', ascending=0).set_index(['pid','key_startTime'])
	procstat = pandas.DataFrame(hostgroup['procstat'][...])
	procstat['key_startTime'] = procstat['startTime']
	procstat = procstat.sort('recTime', ascending=0).set_index(['pid','key_startTime'])
	
	t_data = None

	## for just query processes
	if not procdata.empty:
		idx = None
		for key in query:
			for q in query[key]:
				sq = procdata[key].str.contains(q)
				if idx is None:
					idx = sq
				else:
					idx = idx | sq
		processes = procdata[idx]
		if not processes.empty:
			pd = processes.groupby(level=[0,1]).first()
			ps = procstat.ix[pd.index].groupby(level=[0,1]).first()
			t_data = pd.join(ps, rsuffix='_ps')
			t_data['host'] = hostname
			t_data = t_data.set_index('host', append=True)
	return t_data
	
def merge_host_data(existing_hostdata, new_hostdata):
	records = 0
	if new_hostdata is not None and not new_hostdata.empty:
		records = new_hostdata.shape[0]
		if existing_hostdata is None:
			existing_hostdata = new_hostdata
		else:
			replaceList = existing_hostdata.index.isin(new_hostdata.index)
			if len(replaceList) > 0 and len(existing_hostdata.index[replaceList]) > 0:
				existing_hostdata.ix[replaceList] = new_hostdata.ix[existing_hostdata.index[replaceList]]
			addList = numpy.invert(new_hostdata.index.isin(existing_hostdata.index))
			if len(addList) > 0:
				newdata = new_hostdata.ix[addList]
				existing_hostdata = pandas.concat([existing_hostdata, newdata], axis=0)
				#existing_hostdata = existing_hostdata.append(new_hostdata.ix[addList])
	return (records,existing_hostdata)
	

def parse_h5(filename, id_processes, query):
	print "parsing: %s" % filename
	fd = h5py.File(filename, 'r')

	totalRecords = 0
	finalRecords = 0
	for hostname in fd:
		hostdata = get_host_processes(hostname, fd, query)
		existing_hostdata = None
		if hostname in id_processes:
			existing_hostdata = id_processes[hostname]
		(records, output) = merge_host_data(existing_hostdata, hostdata)
		if output is not None:
			id_processes[hostname] = output
		finalRecords += records
		#print hostname, records, finalRecords
		totalRecords += records
	finalRecords = 0
	for hostname in id_processes:
		finalRecords += id_processes[hostname].shape[0]

	print "[%d] ===== %d; %d =====" % (rank, totalRecords, finalRecords)

def get_job_data(start, end, outputFile):
	ret = -1
	qqacct_data = None
	try:
		qqacct_data = pandas.read_table(outputFile, sep=':', header=None, names=['user','project','job','task','hostname','h_rt','wall','hvmem','maxvmem','ppn','sge_status','exit_status'])
		ret = 0
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


def identify_files(filenames):

	global comm
	global rank
	global size

	used_ranks = size
	if len(filenames) < size:
		used_ranks = len(filenames)

	filesizes = [os.path.getsize(x) for x in filenames]
	tgt_size_per_rank = sum(filesizes) / used_ranks

	## spread out files
	files_per_rank = len(filenames) / size
	n_ranks_with_one_extra = len(filenames) % size
	rank_files = []
	f = 0
	for i in xrange(size):
		t = {'file_idx': [], 'size': 0}
		files_to_add = files_per_rank
		if i < n_ranks_with_one_extra:
			files_to_add += 1
		for j in xrange(files_to_add):
			t['file_idx'].append(f)
			t['size'] += filesizes[f]
			f += 1
		rank_files.append(t)

	for r in rank_files:
		r['file'] = []
		for idx in r['file_idx']:
			r['file'].append(filenames[idx])

	print "rank %d files, resid %02.f, %0.2f bytes: ; %d files" % (rank,float(rank_files[rank]['size'] - tgt_size_per_rank)/1024**2, float(rank_files[rank]['size'])/1024**2, len(rank_files[rank]['file']))
	return rank_files

def identify_hosts(hosts):
	global comm
	global rank
	global size

	used_ranks = size
	if len(hosts) < size:
		used_ranks = len(hosts)
	
	h_list = sorted(hosts.keys())
	h_values = [(x,hosts[x],) for x in h_list]

	# spread out hosts
	hosts_per_rank = len(h_values) / size
	n_ranks_with_one_extra = len(h_values) % size

	print "[%d] hosts: %d, hosts_per_rank: %d, n_ranks_with_one_extra: %d" % (rank, len(h_values), hosts_per_rank, n_ranks_with_one_extra)
	rank_hosts = []
	h = 0
	for i in xrange(size):
		t = {'host_idx': [], 'size': 0}
		hosts_to_add = hosts_per_rank
		if i < n_ranks_with_one_extra:
			hosts_to_add += 1
		for j in xrange(hosts_to_add):
			t['host_idx'].append(h)
			t['size'] += h_values[h][1]
			h += 1
		rank_hosts.append(t)
	
	for r in rank_hosts:
		r['host'] = []
		for idx in r['host_idx']:
			if idx > len(h_list):
				print "[%d]: trying to append %d to hostlist" % (rank, idx)
			else:
				r['host'].append(h_values[idx][0])
	
	return rank_hosts

def divide_data(rank_hosts, host_processes):
	global comm
	global size
	global rank
	
	div_data = []
	div_count = []

	for r in xrange(size):
		processes = None
		cnt = 0
		if r < len(rank_hosts) and len(rank_hosts[r]['host']) > 0:
			host_process_list = []
			for host in rank_hosts[r]['host']:
				if host in host_processes:
					host_process_list.append(host_processes[host])
			if len(host_process_list) > 0:
				processes = pandas.concat(host_process_list, axis=0)
				if processes is not None and not processes.empty:
					cnt = processes.shape[0]
		div_count.append(cnt)
		div_data.append(processes)
	print "[%d] divided processes: " % rank, div_count
	return div_data

def get_processes(filenames, query):
	status = 0
	processes = None

	global comm
	global size
	global rank

	host_processes = {}
	rank_files = identify_files(filenames)
	filenames = rank_files[rank]['file']

	for filename in filenames:
		parse_h5(filename, host_processes, query)

	# build dictionary of hosts and known processes per host
	hosts = {x:host_processes[x].shape[0] for x in host_processes}
	print "[%d] type(hosts): %s, %d" % (rank, type(hosts), len(hosts.keys()))
	print "[%d] starting allgather communication" % rank
	allhosts = comm.allgather(hosts)
		
	for i in range(size):
		if i == rank:
			continue
		for host in allhosts[i]:
			if host not in hosts:
				hosts[host] = allhosts[i][host]
			else:
				hosts[host] += allhosts[i][host]
	procsum = 0
	for host in hosts:
		procsum += hosts[host]
	print "[%d] got %d total hosts for %d processes" % (rank, len(hosts.keys()), procsum)

	rank_hosts = identify_hosts(hosts)
	split_data_by_rank = divide_data(rank_hosts, host_processes)
	
	print "[%d] starting all to all" % rank
	my_data = comm.alltoall(split_data_by_rank)

	print "[%d] merging data: %s" % (rank, type(my_data))
	print "[%d] merging data len: %d, %s" % (rank, len(my_data), type(my_data[0]))
	if rank == 0:
		import cPickle
		cPickle.dump(my_data, open('output.debug.pk', 'wb'))
	processes = None
	for r in xrange(len(my_data)):
		if my_data[r] is not None:
			try:
				(records,processes) = merge_host_data(processes, my_data[r])
			except:
				import traceback
				(a,b,c) = sys.exc_info()
				exception_strs = traceback.format_exception(a,b,c)
				for string in exception_strs:
					print "[%d] from %d, got %s" % (rank, r, type(my_data[r]))
					print "[%d] from %d, shape %d" % (rank, r, my_data[r].shape[0])
					print "[%d] from %d, shape %s" % (rank, r, my_data[r].index)
					print "[%d] %s\n" % (rank, string)
		
	print "[%d] finished merging data" % rank

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
		groups = processes.groupby('exePath', as_index=False)
		summary = groups.apply(lambda x: summarize_processes(x,'exePath'))
		ret['executables'] = summary
	if 'execUser':
		groups = processes.groupby(['username','exePath'], as_index=False)
		summary = groups.apply(lambda x: summarize_processes(x, ['username','exePath']))
		ret['execUser'] = summary
	if 'execProject':
		groups = processes.groupby(['project','exePath'], as_index=False)
		summary = groups.apply(lambda x: summarize_processes(x, ['project','exePath']))
		ret['execProject'] = summary
	if 'scripts' in summaries:
		groups = processes.groupby('scripts', as_index=False)
		summary = groups.apply(lambda x: summarize_processes(x, 'scripts'))
		ret['scripts'] = summary
	if 'scriptUser':
		groups = processes.groupby(['username','scripts'], as_index=False)
		summary = groups.apply(lambda x: summarize_processes(x, ['username','scripts']))
		ret['scriptUser'] = summary
	if 'scriptProject':
		groups = processes.groupby(['project','scripts'], as_index=False)
		summary = groups.apply(lambda x: summarize_processes(x, ['project','scripts']))
		ret['scriptProject'] = summary
	if 'projects' in summaries:
		groups = processes.groupby("project", as_index=False)
		summary = groups.apply(lambda x: summarize_processes(x, 'project'))
		ret['projects'] = summary
	if 'users' in summaries:
		groups = processes.groupby("username", as_index=False)
		summary = groups.apply(lambda x: summarize_processes(x, 'username'))
		ret['users'] = summary
	return ret

def usage(ret):
	print "procFinder.py [--h5-path <path=/global/projectb/shared/data/genepool/procmon>] [--start YYYYMMDDHHMMSS] [--end YYYYMMDDHHMMSS] [--prefix <h5prefix=procmon_genepool>] --qqacct-data <filename> --save-prefix <save_filename_prefix> <col> <regex> [<col> <regex> ...]"
	print "  Start time defaults to yesterday at Midnight (inclusive)"
	print "  End time defaults to today at Midnight (non-inclusive)"
	print "  Therefore, the default is to process all the files from yesterday!"
	sys.exit(ret)

def print_process(proc):
	print "%s,%s,%lu" % (row['username'],"%s,%s,%s" % (row['exePath'],row['execName'],row['cwdPath']),row['startTime'])

def main(args):
	## initialize configurable variables
	yesterday = date.today() - timedelta(days=1)
	start_time = datetime.combine(yesterday, time(0,0,0))
	end_time = datetime.combine(date.today(), time(0,0,0))
	h5_path = "/global/projectb/shared/data/genepool/procmon"
	h5_prefix = "procmon_genepool"
	qqacct_file = None
	save_prefix = None

	query = {}

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
		elif args[i] == "--end":
			i += 1
			if i < len(args):
				try:
					end_time = datetime.strptime(args[i], "%Y%m%d%H%M%S")
				except:
					usage(1)
			else:
				usage(1)
		elif args[i] == "--qqacct-data":
			i += 1
			if i < len(args):
				qqacct_file = args[i]
				if not os.path.exists(qqacct_file):
					print "%s doesn't exist!" % (qqacct_file)
					usage(1)
			else:
				usage(1)
		elif args[i] == "--h5-path":
			i += 1
			if i < len(args):
				h5_path = args[i]
				if not os.path.exists(h5_path):
					print "%s doesn't exist!" % (h5_path)
					usage(1)
			else:
				usage(1)
		elif args[i] == "--h5-prefix":
			i += 1
			if i < len(args):
				h5_prefix = args[i]
			else:
				usage(1)
		elif args[i] == "--save-prefix":
			i += 1
			if i < len(args):
				save_prefix = args[i]
			else:
				usage(1)
		elif args[i] == "--help":
			usage(0)
		else:
			key = args[i]
			value = None
			i += 1
			if i < len(args):
				value = args[i]
				if key not in query:
					query[key] = []
				query[key].append(value)
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
		print "procmon data files could not be found"
		sys.exit(1)

	if qqacct_file is None:
		print "qqacct data file must be specified"
		sys.exit(1)

	if save_prefix is None:
		print "must specify a save_prefix!"
		sys.exit(1)

	q_status = 0
	h_status = 0
	user_hash = None
	summaries = None

	global comm
	global rank
	qqacct_data = None
	if rank == 0:
		(q_status, qqacct_data) =  get_job_data(start_time,end_time, qqacct_file)
	print "distributing qqacct data"
	qqacct_data = comm.bcast(qqacct_data, root=0)
	print "getting process data (may take a long time):"
	(h_status, processes) = get_processes(filenames, query)
	identify_scripts(processes)
	identify_users(processes)
	processes = integrate_job_data(processes, qqacct_data)
	summaries = summarize_data(processes)
	all_summaries = comm.gather(summaries, root=0)
	all_processes = comm.gather(processes, root=0)
	if rank == 0:
		summ_list = {}
		final_summaries = {}
		for r in xrange(size):
			if all_summaries[r] is None:
				continue
			for key in all_summaries[r]:
				if key not in summ_list:
					summ_list[key] = []
				summ_list[key].append(all_summaries[r][key])
		summary_save_file = '%s.summary.h5' % save_prefix
		for key in summ_list:
			summary = pandas.concat(summ_list[key], axis=0)
			nLevel = len(summary.index.levels)
			print 'got nLevel: %d' % nLevel
			summary = summary.groupby(level=range(nLevel)).sum()
			summary.to_hdf(summary_save_file,key)
		processes = pandas.concat(all_processes, axis=0)
		processes.to_hdf('%s.processes.h5' % save_prefix, 'processes')


if __name__ == "__main__":
	main(sys.argv[1:])
	pass
