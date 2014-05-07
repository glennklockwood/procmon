#!/usr/bin/env python
## procFinder.py -- query the procmon data
##
## Author: Doug Jacobsen <dmj@nersc.gov>
## Copyright (C) 2013 - The Regents of the University of California

## discovered that numexpr was starting as many threads as there are cores;
## this, in combination with MPI was causing far too many running processes at
## the same time.  Also, the code ran ~2x faster without these excess threads
import numexpr
numexpr.set_num_threads(1)

import numpy as np
import numpy.lib.recfunctions as nprec
import h5py
import sys
import os
import traceback
import pandas
from datetime import date,datetime,timedelta,time
import re
import cPickle
import pwd
import subprocess
import tempfile
from mpi4py import MPI
import procmon
import procmon.Scriptable
import stat

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

monitored_filesystems = {
    'projectb':'^(/chos)?/global/projectb/.*',
    'project':'^(/chos)?/global/project/.*',
    'scratch2':'^(/chos)?/global/scratch2/.*',
    'u1':'^(/chos)?/global/u1/.*',
    'u2':'^(/chos)?/global/u2/.*',
    'seqfs':'^(/chos)?/global/seqfs/.*',
    'dna':'^(/chos)?/global/dna/.*',
    'common':'^(/chos)?/global/common/.*',
    'local_scratch':'^/scratch/.*',
    'local_tmp':'^/tmp/.*'
}

commands_modifications = {
    r'iprscan-\d+-\d+-(.*?)-cnk\d+\.(.*)': ('command',r'iprscan-<date>-<id>-\1-<chunk>.\2'),
    r'(.*) \(deleted\)': ('command',r'\1'),
    r'SMcli\..*': ('command','SMcli.<pid>'),
    r'\/opt\/uge\/genepool\/uge\/genepool\/spool\/qmaster\/execd\/.*?\/job_scripts\/\d+': ('script','BATCH_SCRIPT'),
    r'hudson\d+\.sh': ('command','hudson<unique>.sh'),
}

def generate_mpi_type_simple(np_dtype):
    offsets = [0]
    counts  = [np_dtype.itemsize]
    mpi_types = [MPI.BYTE]
    mpidt = MPI.Datatype.Create_struct(counts, offsets, mpi_types)
    mpidt_final = mpidt.Create_resized(0, np_dtype.itemsize).Commit()
    mpidt.Free()

    return mpidt_final


class HostProcesses:
    def __init__(self, hostname):
        self.hostname = hostname
        self.read_datasets = ['procdata','procstat','procfd','procobs']
        self.gentype = None
        self.procmon = {
            'procdata': None,
            'procstat': None,
            'procfd' : None,
            'procobs'  : None,
        }
        self.procmon_keys = {
            'procdata': ['pid','startTime'],
            'procstat': ['pid','startTime','recTime'],
        }
        self.mpi_types = {}
    
    def reduce_data(self, datasets):
        if ('procdata' not in datasets or datasets['procdata'] is None) or ('procstat' not in datasets or datasets['procstat'] is None) or ('procfd' not in datasets or datasets['procfd'] is None) or ('procobs' not in datasets or datasets['procobs'] is None):

            print "[%d] %s missing a dataset! I have: "%(rank,self.hostname),datasets.keys(), 
            return

        print "[%d] starting to reduce %s" % (rank, self.hostname)
        pd = datasets['procdata']
        ps = datasets['procstat']
        po = datasets['procobs']
        pf = datasets['procfd']

        self.orig_types = {
            'procdata': pd.dtype,
            'procstat': ps.dtype,
            'procobs' : po.dtype,
            'procfd'  : pf.dtype,
        }

        pd.sort(order=['recTime'])
        ps.sort(order=['recTime'])
        po.sort(order=['recTime'])
        pf.sort(order=['recTime'])

        ## identify pids which have ancestors, done by transposing pidv
        ## vector and subtracting ppid vector, and looking for indices
        ## that are zero-value
        ## the underlying assumption is that we won't see any duplication
        ## of pids (or ppids) at this point
        def detectChildProcesses(pids, ppids):
            out = []
            it = np.nditer([pids, ppids], ['external_loop'], [['readonly'], ['readonly']],
                    op_axes=[range(pids.ndim)+[-1]*ppids.ndim, [-1]*pids.ndim+range(ppids.ndim)])
            for (lpids, lppids) in it:
                out.append(np.unique( lpids[np.nonzero(np.subtract(lpids,lppids) == 0)[0]]))
            return np.unique(np.concatenate(out))
        parentPids = detectChildProcesses(np.unique(pd['pid']), np.unique(pd['ppid']))

        ## detect which filesystems were written to and which were read from
        fs_masks = {}
        fs_types = [('pid',ps['pid'].dtype), ('startTime',ps['startTime'].dtype), ('recTime',ps['recTime'].dtype),('fread',bool),('fwrite',bool)]
        for fs in monitored_filesystems:
            fsq = re.compile(monitored_filesystems[fs])
            fsmatch = np.vectorize(lambda x: bool(fsq.match(x)))
            fs_mask = fsmatch(pf['path'])
            fs_masks[fs] = fs_mask
            fs_types.append(('%s' % fs, np.bool,))
        fsData = np.zeros(shape=pf.size, dtype=np.dtype(fs_types))
        fsData['pid'] = pf['pid']
        fsData['startTime'] = pf['startTime']
        fsData['recTime'] = pf['recTime']
        fsData['fread'] = (stat.S_IRUSR & pf['mode']) > 0
        fsData['fwrite'] = (stat.S_IWUSR & pf['mode']) > 0
        for fs in fs_masks:
            fsData[fs] = fs_masks[fs]

        # convert to pandas DataFrames for more efficient
        # grouping and summarization    
        pd = pandas.DataFrame(pd)#.sort(['recTime','recTimeUSec'])
        ps = pandas.DataFrame(ps)#.sort(['recTime','recTimeUSec'])
        po = pandas.DataFrame(po)#.sort(['recTime','recTimeUSec'])
        fs = pandas.DataFrame(fsData)#.sort(['recTime','recTimeUSec'])

        pd['hasChildren'] = False
        parentIdx = pd.query('pid in parentPids').index
        pd['hasChildren'][parentIdx] = True

        pd_group = pd.groupby(['pid','startTime'])
        ps_group = ps.groupby(['pid','startTime'])
        po_group = po.groupby(['pid','startTime'])
        fs_group = fs.groupby(['pid','startTime'])

        def summarize_timeseries(ps_rec):
            ps_rec = ps_rec.sort(['recTime'])
            cpu = (ps_rec.utime + ps_rec.stime) / 100.
            iosum = ps_rec.io_wchar + ps_rec.io_rchar
            duration = ps_rec.recTime - ps_rec.startTime
            durDiff = duration.diff()
            cpuRate = cpu.diff() / durDiff
            iowRate = ps_rec.io_wchar.diff() / durDiff
            iorRate = ps_rec.io_rchar.diff() / durDiff
            ioRate = iosum.diff() / durDiff
            msizeRate = ps_rec.m_size.diff() / durDiff
            mresidentRate = ps_rec.m_resident.diff() / durDiff

            rates = pandas.DataFrame({'cpuRate':cpuRate, 'iowRate':iowRate, 'iorRate':iorRate, 'ioRate':ioRate, 'msizeRate': msizeRate, 'mresidentRate': mresidentRate})
            correlation = rates.corr()
            retData = {
                'cpuRateMax': cpuRate.max(),
                'iowRateMax': iowRate.max(),
                'iorRateMax': iorRate.max(),
                'msizeRateMax': msizeRate.max(),
                'mresidentRateMax': mresidentRate.max(),
                'nRecords': ps_rec.shape[0],
            }
            res = ['cpu','iow','ior','io','msize','mresident']
            resIdx1 = 0
            resIdx2 = 0
            while resIdx1 < len(res):
                resIdx2 = resIdx1 + 1
                while resIdx2 < len(res):
                    retData['cor_%sX%s'%(res[resIdx1],res[resIdx2])] = correlation['%sRate'%res[resIdx1]]['%sRate'%res[resIdx2]]
                    resIdx2 += 1
                resIdx1 += 1
                    
            return pandas.Series(retData)

        def summarize_procobs(po_rec):
            return pandas.Series({'nRecords': po_rec.shape[0]})

        def summarize_fs(fs_rec):
            ret = {}
            for fs in monitored_filesystems:
                ret['fs_%s_write' % fs] = sum(fs_rec.fwrite & fs_rec[fs]) > 0
                ret['fs_%s_read' % fs] = sum(fs_rec.fread & fs_rec[fs]) > 0
            return pandas.Series(ret)
                
        def pd_last_record(x):
            mask = np.invert(x['exePath'].str.match('Unknown'))
            retRec = x.ix[x.index[-1]]
            if np.sum(mask) > 0:
                okData = x[mask]
                okRec = okData.ix[okData.index[-1]]
                return okRec
            return retRec

        ps_summaries = ps_group.apply(summarize_timeseries)
        po_summaries = po_group.apply(summarize_procobs)
        fs_summaries = fs_group.apply(summarize_fs)

        pd_final = pd_group.apply(pd_last_record)
        ps_final = ps_group.last()
        combined = pd_final.join(other=ps_final, how='left', rsuffix='_ps').join(other=ps_summaries,how='left').join(other=fs_summaries, how='left')
        combined['volatilityScore'] = 0.
        combined['volatilityScore'] = (ps_summaries['nRecords'] - 1) / po_summaries['nRecords']
        combined['nObservations'] = po_summaries['nRecords']
        combined['recTime'] = ps_final['recTime']
        combined['recTimeUSec'] = ps_final['recTimeUSec']
        cols = combined.columns
        null_idx = pandas.isnull(combined['hasChildren'])
        if np.sum(null_idx) > 0:
            combined['hasChildren'][null_idx] = False
        for col in cols:
            if col.endswith('_ps'):
                del combined[col]
            if col.startswith('fs_'):
                null_idx = pandas.isnull(combined[col])
                if np.sum(null_idx) > 0:
                    combined[col][null_idx] = False

        combined['host'] = self.hostname
        self.procmon['processes'] = combined

    def set_baseline(self, baseline, start_time):
        baseline_ps = pandas.DataFrame(baseline['procstat']).sort(['recTime'])
        baseline_ps = baseline_ps.groupby(['pid','startTime']).last()
        processes = self.procmon['processes']
        processes['startTime_baseline'] = processes.startTime.copy(True)
        processes['utime_baseline'] = np.zeros(processes.shape[0],dtype=processes.utime.dtype)
        processes['stime_baseline'] = np.zeros(processes.shape[0],dtype=processes.stime.dtype)
        processes['io_rchar_baseline'] = np.zeros(processes.shape[0],dtype=processes.io_rchar.dtype)
        processes['io_wchar_baseline'] = np.zeros(processes.shape[0],dtype=processes.io_wchar.dtype)
        early_starters = processes.ix[processes.startTime < start_time].index
        if baseline_ps is not None and not baseline_ps.empty and len(early_starters) > 0:
            # identify processes which started before start_time
            processes.utime_baseline[early_starters] = baseline_ps.ix[early_starters].utime
            processes.stime_baseline[early_starters] = baseline_ps.ix[early_starters].stime
            processes.io_rchar_baseline[early_starters] = baseline_ps.ix[early_starters].io_rchar
            processes.io_wchar_baseline[early_starters] = baseline_ps.ix[early_starters].io_wchar
            processes.startTime_baseline[early_starters] = start_time
        
            ## in case the baseline data is missing some things we think should be there
            processes.utime_baseline[np.invert(processes.utime_baseline.notnull())] = 0
            processes.stime_baseline[np.invert(processes.stime_baseline.notnull())] = 0

    def generate_nptype(self, processes):
        if self.gentype is not None:
            return self.gentype
        types = []
        fixedTypes = {
            'nRecords': np.uint64,
            'nObservations': np.uint64,
            'host': '|S48',
            'script': '|S1024',
            'execCommand': '|S1024',
            'command': '|S64',
            'username': '|S12',
            'project': '|S20',
            'job': '|S64',
        }
        for col in processes.columns:
            if col in self.orig_types['procdata'].names:
                types.append( (col, self.orig_types['procdata'].fields[col][0]) )
            elif col in self.orig_types['procstat'].names:
                types.append( (col, self.orig_types['procstat'].fields[col][0]) )
            elif col == "hasChildren" or col.startswith("fs_"):
                types.append( (col, bool) )
            elif col.startswith("cor_") or col.endswith("Max") or col == "volatilityScore":
                types.append( (col, np.float64) )
            elif col.endswith('_baseline'):
                realcol = re.match('(.*)_baseline', col).groups()[0]
                found_type = np.int64
                if realcol in self.orig_types['procdata'].names:
                    found_type = self.orig_types['procdata'].fields[realcol][0]
                elif realcol in self.orig_types['procstat'].names:
                    found_type = self.orig_types['procstat'].fields[realcol][0]
                types.append( (col, found_type) )
            elif col in fixedTypes:
                types.append( (col, fixedTypes[col]) )
            else:
                print 'missing type for col: ', col
        self.gentype = np.dtype(types)
        return self.gentype


    def count_processes(self):
        if 'procdata' in self.procmon and self.procmon['procdata'] is not None:
            unique_processes = np.unique(self.procmon['procdata'][['pid','startTime']])
            return unique_processes.size
        return 0

def get_job_data(start, end, qqacct_file):
    """Read job (qqacct) data from a file and return pandas DataFrame of it."""
    ret = -1
    qqacct_data = None
    try:
        qqacct_data = pandas.read_table(qqacct_file, sep=':', header=None, names=['user','project','job','task','hostname','h_rt','wall','hvmem','maxvmem','ppn','sge_status','exit_status'])
        ret = 0
    except:
        pass
    return (ret, qqacct_data)

def identify_scripts(processes):
    """For known scripting interpreter processes, work out what the executed script was."""
    executables = processes.exePath.unique()
    prefixes = ["perl","python","ruby","bash","sh","tcsh","csh","java"]
    processes['script'] = None
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
            scripts = args.apply(lambda x: procmon.Scriptable.Scriptable(executable, x[1::]))
            processes.script[selection] = scripts

def identify_userCommand(processes):
    """ Use a simple heuristic to identify the intended command for each process
        depends on indentify_scripts already having been run """

    processes['execCommand'] = processes.exePath.str.split('/').str.get(-1)
    command = processes.script.str.split('/').str.get(-1)
    mask = (command == "COMMAND" ) | (command == "") | (np.invert(command.notnull()))
    command[mask] = processes.ix[mask].execCommand
    processes['command'] = command 

    for search in commands_modifications:
        (column,replace) = commands_modifications[search]
        searchre = re.compile(search)
        mask = processes[column].apply(lambda x: False if x is None else bool(searchre.match(x)))
        if np.sum(mask) > 0:
            processes['command'][mask] = processes[column][mask].apply(lambda x: searchre.sub(replace, x))


def identify_files(filenames, baseline_filenames):
    """Determine which mpi rank should parse which hdf5 files.

    Arguments:
    filenames -- list of absolute paths
    baseline_filenames -- list of absolute paths of files for baseline subtraction

    Returns:
    list [mpi.size length] of hashs which include in the 'file' key a
      list of files for the rank in to parse
    
    Note:  this function must be deterministic for a given set of inputs
    and mpi.size.  This is because all ranks will calculate this list, and
    all must arrive at the same answer.

    Note 2: <removed - no longer relevant>

    Note 3: the load balancing performed by this function could be improved.
    The size of the file should be proportional to the time taken to parse it,
    so, trying to optimize the size distribution amung the ranks is the planned
    improvement for load-balancing

    """
    global comm
    global rank
    global size

    ## sort the filenames to ensure all ranks have the same idea of the file
    ## ordering
    filenames = sorted(filenames)
    baseline_filenames = sorted(baseline_filenames)

    filesizes = [os.path.getsize(x) for x in filenames]
    baseline_filesizes = [os.path.getsize(x) for x in baseline_filenames]
    total_bytes = sum(filesizes) + sum(baseline_filesizes)
    total_files = len(filenames) + len(baseline_filenames)

    tgt_size_per_rank = total_bytes / size
    rr_skip = size / total_files
    expected_files_per_rank = min(1, total_files / size)

    count = []
    rank_files = []
    rank_baseline = []
    for i in xrange(size):
        count.append(0)
        rank_files.append([])
        rank_baseline.append([])
    rank_idx = 0
    filename_idx = 0
    baseline_idx = 0
    while True:

        if count[rank_idx] >= expected_files_per_rank and count[(rank_idx+1)%size] < expected_files_per_rank:
            rank_idx = (rank_idx+1)%size
        if filename_idx < len(filenames):
            rank_files[rank_idx].append(filenames[filename_idx])
            filename_idx += 1
        elif baseline_idx < len(baseline_filenames):
            rank_baseline[rank_idx].append(baseline_filenames[baseline_idx])
            baseline_idx += 1
        else:
            break           
        count[rank_idx] += 1

        rank_idx = (rank_idx + 1 + rr_skip) % size
    print "[%d] input h5 files: %d, baseline h5 files: %d" % (rank, len(rank_files[rank]), len(rank_baseline[rank]))

    return (rank_files, rank_baseline)

def identify_hosts(host_list, host_proc_cnt):
    """ Identify which hosts will be processed by each rank.

        @param host_list - master list of all hostnames in analysis
        @param host_proc_cnt - quantity of procdata records by host (this rank)

        This function is sent the master host_list (list of hostnames (str))
        which is the complete list of all hosts (was constructed via allgather
        in previous step).  This function also receives host_proc_cnt, a 1d 
        numpy array of the quantity of procdata records contained in this
        rank foreach host (ordered same as host_list).

        @returns 1d numpy array of number of hosts for each rank
    """

    global comm
    global rank
    global size

    ## if there are fewer hosts than ranks, we'll assign one host per rank and
    ## leave the higher numbered ranks idle, otherwise use them all
    used_ranks = size
    if len(host_list) < size:
        used_ranks = len(host_list)
    if used_ranks == 0:
        return np.empty(shape=0, dtype=np.int64)

    rank_host_cnt = np.zeros(dtype=np.int64, shape=used_ranks)
    rank_proc_cnt = np.zeros(dtype=np.int64, shape=used_ranks)
    totalprocs = sum(host_proc_cnt)
    goal_procs_per_rank = totalprocs / used_ranks

    ## load balancing by assigning a set of hosts per rank trying to optimize
    ## the same number of total processes per rank (the processes within each
    ## host)
    print "[%d] ranks: %d, hosts: %d, goal_processes_per_rank: %d" % (rank, used_ranks, len(host_list), goal_procs_per_rank)
    h_idx = 0
    for lrank in xrange(used_ranks):
        t = {'host': [], 'size': 0}
        done = False
        cnt = 0
        while not done and h_idx < len(host_list):
            proc_count = host_proc_cnt[h_idx]
            operand = proc_count if lrank < used_ranks/2 else 0
            done = cnt > 0 and rank_proc_cnt[lrank] + operand > goal_procs_per_rank
            if not done:
                rank_host_cnt[lrank] += 1
                rank_proc_cnt[lrank] += proc_count
                h_idx += 1
                cnt += 1
    lrank = used_ranks-1
    while h_idx < len(host_list):
        proc_count = host_proc_cnt[h_idx]
        rank_host_cnt[lrank] += 1
        rank_proc_cnt[lrank] += proc_count       
        h_idx += 1


    print "[%d] rank_hosts: " % rank, rank_host_cnt, rank_proc_cnt
    return rank_host_cnt

def divide_data(host_list, rank_hosts, h5parser, dataset):
    """Combine datasets into massive single arrays for alltoallv transmission.
       @param host_list - ordered list of hosts
       @param rank_hosts - list of how many hosts per rank (as ordered in host_list)
       @param host_processes - all the host records this process has collected

       This function needs to iterate through each dataset and construct
       a very large single array of each dataset, as well as an index indicating which
       records are for which host.
    """
    global comm
    global size
    global rank
    
    h_idx = 0
    retdata = [None,np.zeros(dtype=np.int64, shape=len(host_list))]
    if dataset in h5parser.datasets:
        retdata[0] = h5parser.datasets[dataset]

    while h_idx < len(host_list):
        host_data = None
        hostname = host_list[h_idx]
        if hostname in h5parser.hosts:
            file_idx = h5parser.hosts.index(hostname)
            retdata[1][h_idx] = h5parser.host_counts[dataset][file_idx]
        else:
            retdata[1][h_idx] = 0
        h_idx += 1
   
    rank_row_offsets = np.zeros(dtype=np.int64, shape=len(rank_hosts))
    cumsum = 0
    idx = 0
    while idx < len(rank_hosts):
        rank_row_offsets[idx] = np.sum(retdata[1][cumsum:(cumsum+rank_hosts[idx])])
        cumsum += rank_hosts[idx]
        idx += 1
    retdata.append(rank_row_offsets)

    return retdata

def mpi_transfer_datasets(root, host_list, rank_hosts, h5parser):
    global comm
    global rank
    global size

    complete_host_processes = {}
    for dataset in ['procdata','procstat','procobs','procfd']:
        print '[%d] getting ready to transmit %s' % (rank, dataset)
        data = divide_data(host_list, rank_hosts, h5parser, dataset)
        outbound_counts = data[2]

        np_type = None
        if rank == root:
            np_type = data[0].dtype

        np_type = comm.bcast(np_type, root=root)
        mpi_type = generate_mpi_type_simple(np_type)

        # if this process didn't read an hdf5 file, it won't have any data
        # to send, so put in some place-holder empties
        if data[0] is None or len(data[0]) == 0:
            data[0] = np.zeros(shape=size, dtype=np_type)
            outbound_counts = np.array([1] * size, dtype=np.int64)

        data_recv_counts = np.zeros(shape=size, dtype=np.int64)
        ## transmit the counts via alltoall
        comm.Alltoallv([outbound_counts, [1]*size, range(size), MPI.LONG], [data_recv_counts, [1]*size, range(size), MPI.LONG])

        recv_data = np.zeros(shape=np.sum(data_recv_counts), dtype=np_type)
        outbound_offsets = np.hstack((0, np.cumsum(outbound_counts)))[0:-1]
        data_recv_offsets = np.hstack((0, np.cumsum(data_recv_counts)))[0:-1]
        print "[%d] data_recv_counts: "% rank, list(outbound_counts), list(data_recv_counts)
        print "[%d] data_recv_offsets: "% rank, list(outbound_offsets), list(data_recv_offsets)
        print "[%d] data: "%rank, data[0].size
        print "[%d] recv_data: "%rank, recv_data.size
        ## transmit the actual dataset
        comm.Alltoallv([data[0], outbound_counts, outbound_offsets, mpi_type], [recv_data, data_recv_counts, data_recv_offsets, mpi_type])

        ## prepare to send host-index of the data
        dset_idx = data[1]
        outbound_counts = rank_hosts
        outbound_offsets = np.hstack((0, np.cumsum(outbound_counts)))[0:-1]
        recv_counts = [outbound_counts[rank]]*size
        recv_offsets = np.hstack((0, np.cumsum(recv_counts)))[0:-1]
        recv_idx = np.zeros(shape=sum(recv_counts), dtype=np.int64)
        comm.Alltoallv([dset_idx, rank_hosts, outbound_offsets, MPI.LONG],[recv_idx, recv_counts, recv_offsets, MPI.LONG])

        ## reconstruct the per-host data structures
        dataset_base_idx = 0
        host_base_indices = np.hstack((0, np.cumsum(rank_hosts)))[0:-1]
        components = {}
        for rank_idx in xrange(len(rank_hosts)):
            if dataset_base_idx > data_recv_offsets[rank_idx]:
                print "WARNING! DANGER! It looks we over-ran into another rank's data! DANGER! WARNING!"
            if dataset_base_idx < data_recv_offsets[rank_idx]:
                print "INFO! USEFUL! Moving up %d records, hopefully accounting for data from an empty rank. USEFUL! INFO!" % (data_recv_offsets[rank_idx] - dataset_base_idx)
            dataset_base_idx = data_recv_offsets[rank_idx]
            for host_cnt in xrange(rank_hosts[rank]):
                host_idx = host_base_indices[rank] + host_cnt
                host = host_list[host_idx]
                if host not in complete_host_processes:
                    complete_host_processes[host] = {}
                if host not in components:
                    components[host] = []
                limit = recv_idx[ (rank_idx * rank_hosts[rank]) + host_cnt ]

                components[host].append( recv_data[dataset_base_idx:(dataset_base_idx + limit)] )
                dataset_base_idx += limit
        for host in complete_host_processes:
            complete_host_processes[host][dataset] = np.concatenate( components[host] )
        del recv_data
    return complete_host_processes

def get_processes(filenames, baseline_filenames, query, start_time, qqacct_data):
    status = 0
    processes = None
    start_time = int(start_time.strftime("%s"))

    global comm
    global size
    global rank

    host_processes = {}
    baseline_processes = {}
    (rank_files,rank_baseline_files) = identify_files(filenames, baseline_filenames)
    filenames = rank_files[rank]
    baseline_filenames = rank_baseline_files[rank]

    files_root = -1
    baseline_root = -1

    for idx in xrange(size):
        if files_root == -1 and len(rank_files[idx]) > 0:
            files_root = idx
        if baseline_root == -1 and len(rank_baseline_files[idx]) > 0:
            baseline_root = idx
        
    print "[%d] starting input file parsing" % rank
    input_parser = procmon.H5Parser.RawH5Parser()
    input_parser.parse(filenames)
    print "[%d] finished input file parsing: " % rank, input_parser

    # build dictionary of hosts and known processes per host
    hosts = {}
    for x in input_parser.hosts:
        hosts[x] = input_parser.count_processes(x)


    print "[%d] beginning collective hostname comparison" % rank
    allhosts = comm.allgather(hosts)
    for i in xrange(size):
        if i == rank:
            continue
        for host in allhosts[i]:
            if host not in hosts:
                hosts[host] = 0
            hosts[host] += allhosts[i][host]


    print "[%d] parsing %d baseline files" % (rank, len(baseline_filenames))
    baseline_parser = procmon.H5Parser.RawH5Parser()
    baseline_parser.parse(baseline_filenames, ref_hosts=hosts)
    print "[%d] done parsing baseline files"

    procsum = 0
    for host in hosts:
        procsum += hosts[host]

    print "[%d] got %d total hosts for %d processes" % (rank, len(hosts.keys()), procsum)

    # master list of hosts sorted alphabetically
    master_hosts = sorted(hosts.keys())

    # numpy array of process counts per host in this rank
    host_proc_cnt = np.array( [hosts[x] for x in master_hosts], dtype=np.int64 )
    rank_hosts = identify_hosts(master_hosts, host_proc_cnt)

    complete_host_processes = mpi_transfer_datasets(files_root, master_hosts, rank_hosts, input_parser)
    baseline_host_processes = mpi_transfer_datasets(baseline_root, master_hosts, rank_hosts, baseline_parser)
            
    del input_parser
    del baseline_parser

    host_processes = {}
    print "[%d] reducing data" % (rank)
    for host in complete_host_processes:
        hostprocs = HostProcesses(host)
        hostprocs.reduce_data(complete_host_processes[host])
        host_processes[host] = hostprocs
    print "[%d] data reduction complete, starting baseline" % rank
    for host in host_processes:
        host_processes[host].set_baseline(baseline_host_processes[host], start_time)
    print "[%d] baselining complete" % rank

    for host in host_processes:
        processes = host_processes[host].procmon['processes']
        identify_scripts(processes)
        identify_userCommand(processes)
        identify_users(processes)
        host_processes[host].procmon['processes'] = integrate_job_data(processes, qqacct_data)

    ret_processes = {}
    hosts = host_processes.keys()
    for host in hosts:
        processes = host_processes[host].procmon['processes']
        np_type = host_processes[host].generate_nptype(processes)
        npprocs = np.zeros(shape=processes.shape[0], dtype=np_type)
        for col in processes.columns:
            coltype = npprocs[col].dtype
            try:
                if coltype in (np.uint8, np.uint16, np.uint32, np.uint64, np.int8, np.int16, np.int32, np.int64):
                    nandex = pandas.isnull(processes[col])
                    if sum(nandex) > 0:
                        processes[col][nandex] = 0
                npprocs[col] = processes[col].astype(npprocs[col].dtype, copy=True)
            except:
                print '[%d] %s issue setting column %s\n' % (rank, host, col), npprocs[col].dtype, processes[col].dtype
                print list(processes[col])
                print processes[col]
                traceback.print_exc(file=sys.stdout)
                exc_type, exc_value, exc_traceback = sys.exc_info()
                traceback.print_tb(exc_traceback, limit=1, file=sys.stdout)
                sys.exit(1)
                
        ret_processes[host] = npprocs
        del host_processes[host]

    return ret_processes

def identify_users(processes):
    uids = processes.realUid.unique()
    processes['username'] = 'unknown uid'
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
    if qqacct_data is None:
        processes['project'] = 'unknown project'
        return processes
    qqacct_data.job = qqacct_data.job.astype('str')
    qqacct_data.task[qqacct_data.task == 0] = 1
    qqacct_data.task = qqacct_data.task.astype('str')
    # get the last state for each job/task/hostname combination that was observed
    qqacct_data = qqacct_data.groupby(['job']).last().reset_index()
    processes = processes.merge(right=qqacct_data[['job','project']], how='left', left_on='identifier', right_on='job', suffixes=['','qq'])
    return processes
    
def summarize_processes(group):
    rowhash = {
        'cpu_time' : (group.utime + group.stime).sum() / 100.,
        'net_cpu_time': ((group.utime + group.stime) - (group.utime_baseline + group.stime_baseline)).sum() / 100.,
        'walltime' : (group.recTime - group.startTime).sum(),
        'net_walltime' : (group.recTime - group.startTime_baseline).sum(),
        'nobs'     : group.utime.count()
    }
    return pandas.Series(rowhash)

def summarize_data(processes, summaries = {
        'executables' : ['exePath'],
        'execUser' : ['username', 'exePath'],
        'execProject' : ['project', 'exePath'],
        'script' : ['script', 'exePath', 'execName'],
        'scriptUser' : ['username', 'script', 'exePath', 'execName'],
        'scriptProject' : ['project', 'script', 'exePath', 'execName'],
        'projects' : ['project'],
        'users' : ['username']
    }):

    ret = {}
    for summ_type in summaries:
        groups = processes.groupby(summaries[summ_type], as_index=False)
        summary = groups.apply(lambda x: summarize_processes(x))
        if type(summary) is pandas.core.series.Series:
            t_data = summary.to_dict()
            data = []
            data.extend(t_data.keys()[0][0:len(summaries[summ_type])])
            data.extend(t_data.values())
            keys = []
            keys.extend(summaries[summ_type])
            keys.extend(['cpu_time','walltime','nobs'])
            t_data = {}
            for (idx,key) in enumerate(keys):
                t_data[key] = data[idx]
            summary = pandas.DataFrame([t_data]).set_index(summaries[summ_type])
             
        ret[summ_type] = summary

    return ret

def usage(ret):
    print "procFinder.py [--h5-path <path=/global/projectb/statistics/procmon/genepool>] [--start YYYYMMDDHHMMSS] [--end YYYYMMDDHHMMSS] [--prefix <h5prefix=procmon_genepool>] [--save-all-processes] --qqacct-data <filename> --save-prefix <save_filename_prefix> <col> <regex> [<col> <regex> ...]"
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
    h5_path = "/global/projectb/statistics/procmon/genepool"
    #h5_path = "/scratch/proc"
    h5_prefix = "procmon_genepool"
    save_all_processes = False
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
                    qqacct_file = None
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
        elif args[i] == "--save-all-processes":
            save_all_processes = True
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

    end_time -= timedelta(seconds=1)
    procmon_h5cache = procmon.H5Cache.H5Cache(h5_path, h5_prefix)
    filenames = [ x['path'] for x in procmon_h5cache.query(start_time, end_time) ]
    baseline_filenames = [ x['path'] for x in procmon_h5cache.query(start_time - timedelta(minutes=20), start_time - timedelta(minutes=1)) ]

    filenames = sorted(filenames)
    baseline_filenames = sorted(baseline_filenames)

    if len(filenames) == 0:
        print "procmon data files could not be found"
        sys.exit(1)

    #if qqacct_file is None:
    #    print "qqacct data file must be specified"
    #    sys.exit(1)

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
    if rank == 0 and qqacct_file is not None:
        (q_status, qqacct_data) =  get_job_data(start_time,end_time, qqacct_file)
    print "distributing qqacct data"
    qqacct_data = comm.bcast(qqacct_data, root=0)
    print "getting process data (may take a long time):"
    host_processes = get_processes(filenames, baseline_filenames, query, start_time, qqacct_data)

    local_processes = 0
    for host in host_processes:
        local_processes += host_processes[host].size
    process_cnts = comm.allgather(local_processes)

    print "[%d] about to begin writing processes in parallel (%d global, %d local)" % (rank, sum(process_cnts), local_processes)
    output_dtype = host_processes.values()[0].dtype if rank == 0 else None
    output_dtype = comm.bcast(output_dtype, root=0)
        
    output = h5py.File('%s_processes.h5' % save_prefix, 'w', driver='mpio', comm=comm)
    dset = output.create_dataset('processes', (sum(process_cnts),), dtype=output_dtype)
    offset = sum(process_cnts[0:rank])
    local_idx = 0
    for host in host_processes:
        base = offset + local_idx
        limit = base + host_processes[host].size
        local_idx += host_processes[host].size

        dset[base:limit] = host_processes[host][:]
    output.close()
        

    sys.exit(0)
    processes = None
    summaries = None
    if processes is not None and not processes.empty:
        identify_scripts(processes)
        identify_userCommand(processes)
        identify_users(processes)
        summ_index = {
            'command' : ['command', 'execCommand'],
            'commandUser' : ['username','command','execCommand'],
            'commandProject' : ['project','command','execCommand'],
            'commandHost' : ['host','command','execCommand'],
            'executables' : ['exePath'],
            'execUser' : ['username', 'exePath'],
            'execProject' : ['project', 'exePath'],
            'execHost' : ['host','exePath'],
            'scripts' : ['script', 'exePath', 'execName'],
            'scriptUser' : ['username', 'script', 'exePath', 'execName'],
            'scriptProject' : ['project', 'script', 'exePath', 'execName'],
            'scriptHost' : ['host','script','exePath','execName'],
            'projects' : ['project'],
            'users' : ['username'],
            'hosts' : ['host'],
        }
        summaries = summarize_data(processes, summ_index)
    all_summaries = comm.gather(summaries, root=0)
    if rank == 0:
        summ_list = {}
        final_summaries = {}
        write_summary_debug = False

        for l_rank in xrange(size):
            if all_summaries[l_rank] is None:
                continue
            for key in all_summaries[l_rank]:
                if key not in summ_list:
                    summ_list[key] = []
                l_summary = all_summaries[l_rank][key]
                summ_list[key].append(l_summary)

        if write_summary_debug:
            cPickle.dump(all_summaries, open('%s.summ_list.debug.pk' % save_prefix,'wb'))
            write_summary_debug = False
            pass

        summary_save_file = '%s.summary.h5' % save_prefix
        for key in summ_list:
            summary = None
            if len(summ_list[key]) > 0:
                summary = pandas.concat(summ_list[key], axis=0)

            if summary is not None:
                nLevel=0
                if type(summary.index) is pandas.core.index.MultiIndex:
                    nLevel = range(len(summary.index.levels))
                else:
                    print "%s: got different type: %s" % (key, str(type(summary.index)))
                summary = summary.groupby(level=nLevel).sum()
                summary.to_hdf(summary_save_file,key)

        if write_summary_debug:
            cPickle.dump(summ_list, open('%s.summ_list.debug2.pk' % save_prefix, 'wb'))
    
    if save_all_processes:
        all_processes = None
        print "[%d] about to send %s processes" % (rank, processes.shape if processes is not None else "None")
        try:
            all_processes = comm.gather(processes, root=0)
        except:
            pass
        if rank == 0 and all_processes is not None:
            processes = pandas.concat(all_processes, axis=0)
            processes.to_hdf('%s.processes.h5' % save_prefix, 'processes')


if __name__ == "__main__":
    main(sys.argv[1:])
    pass
