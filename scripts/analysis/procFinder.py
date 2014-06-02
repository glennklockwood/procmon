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

process_dt_list = [
        ('identifier', 'S24'),
        ('subidentifier','S24'),
        ('username','S12'),
        ('project','S24'),
        ('host','S24'),
        ('isParent', np.uint),
        ('pid', np.uint),
        ('ppid', np.uint),
        ('recTime', np.float64),
        ('startTime', np.float64),
        ('origStartTime',np.uint64),
        ('startTime_baseline', np.float64),
        ('execName', 'S256'),
        ('command', 'S256'),
        ('execCommand','S256'),
        ('script','S1024'),
        ('cmdArgBytes', np.uint64),
        ('cmdArgs', 'S1024'),
        ('exePath', 'S1024'),
        ('cwdPath', 'S1024'),
        ('state', 'S1'),
        ('pgrp', np.int),
        ('session', np.int),
        ('tty', np.int),
        ('tpgid', np.int),
        ('flags', np.uint),
        ('utime', np.uint64),
        ('stime', np.uint64),
        ('priority', np.int64),
        ('nice', np.int64),
        ('numThreads', np.int64),
        ('vsize', np.uint64),
        ('rss', np.uint64),
        ('rsslim', np.uint64),
        ('vmpeak', np.uint64),
        ('rsspeak', np.uint64),
        ('signal', np.uint64),
        ('blocked', np.uint64),
        ('sigignore', np.uint64),
        ('sigcatch', np.uint64),
        ('cpusAllowed', np.int),
        ('rtPriority', np.uint),
        ('policy', np.uint),
        ('guestTime', np.uint),
        ('delayacctBlkIOTicks', np.uint64),
        ('io_rchar', np.uint64),
        ('io_wchar', np.uint64),
        ('io_syscr', np.uint64),
        ('io_syscw', np.uint64),
        ('io_readBytes', np.uint64),
        ('io_writeBytes', np.uint64),
        ('io_cancelledWriteBytes', np.uint64),
        ('m_size', np.uint64),
        ('m_resident', np.uint64),
        ('m_share', np.uint64),
        ('m_text', np.uint64),
        ('m_data', np.uint64),
        ('realUid', np.uint64),
        ('effUid', np.uint64),
        ('realGid', np.uint64),
        ('effGid', np.uint64),
        ('nObservations', np.uint64),
        ('nRecordings',np.uint64),
        ('volatilityScore',np.float64),
        ('cputime', np.float64),
        ('duration',np.float64),
        ('cputime_net',np.float64),
        ('utime_net', np.uint64),
        ('stime_net', np.uint64),
        ('io_rchar_net',np.uint64),
        ('io_wchar_net',np.uint64),
        ('cpuRateMax', np.float64),
        ('iowRateMax', np.float64),
        ('iorRateMax', np.float64),
        ('msizeRateMax', np.float64),
        ('mresidentRateMax', np.float64),
        ('cor_cpuXiow', np.float64),
        ('cor_cpuXior', np.float64),
        ('cor_cpuXmsize', np.float64),
        ('cor_cpuXmresident',np.float64),
        ('cor_iowXior', np.float64),
        ('cor_iowXmsize', np.float64),
        ('cor_iowXmresident',np.float64),
        ('cor_iorXmsize',np.float64),
        ('cor_iorXmresident',np.float64),
        ('cor_msizeXmresident',np.float64),
]

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

for fs in monitored_filesystems:
    process_dt_list.append(('fs_%s_read' % fs, np.int32,))
    process_dt_list.append(('fs_%s_write' % fs, np.int32,))

process_dt = np.dtype(process_dt_list)

commands_modifications = {
    r'iprscan-\d+-\d+-(.*?)-cnk\d+\.(.*)': ('command',r'iprscan-<date>-<id>-\1-<chunk>.\2'),
    r'(.*) \(deleted\)': ('command',r'\1'),
    r'SMcli\..*': ('command','SMcli.<pid>'),
    r'\/opt\/uge\/genepool\/uge\/genepool\/spool\/qmaster\/execd\/.*?\/job_scripts\/\d+': ('script','BATCH_SCRIPT'),
    r'hudson\d+\.sh': ('command','hudson<unique>.sh'),
    r'task.\d+.csh': ('command','task.<number>.csh'),
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

    def bounds(self, mask):
        indices = np.array(np.nonzero(mask)[0])
        if indices.size == 0:
            raise ValueError("No entries in mask!")
        if indices.size > 1:
            if np.max(np.diff(indices)) > 1:
                raise ValueError("No gaps allowed in mask, check sorting!")
        min_val = np.min(indices)
        max_val = np.max(indices) + 1
        return (min_val,max_val)

    def reduce_data(self, datasets, start_time, hostnum, hosttotal):
        if 'procdata' not in datasets or datasets['procdata'] is None or \
           'procstat' not in datasets or datasets['procstat'] is None or \
           'procfd' not in datasets or datasets['procfd'] is None or \
           'procobs' not in datasets or datasets['procobs'] is None:

            print "[%d] %s missing a dataset! Have: " % (rank,self.hostname), \
                datasets.keys() 
            return

        pd = datasets['procdata']
        ps = datasets['procstat']
        po = datasets['procobs']
        pf = datasets['procfd']

        pd_procs = np.unique(pd[['pid','startTime']])
        print "[%d] starting to reduce %s, %d procs; %d/%d (%f%%))" % (rank, self.hostname, pd_procs.size, hostnum, hosttotal, 100.*hostnum/hosttotal)

        self.orig_types = {
            'procdata': pd.dtype,
            'procstat': ps.dtype,
            'procobs' : po.dtype,
            'procfd'  : pf.dtype,
        }

        hostmismatch = pd['host'] != self.hostname
        if np.sum(hostmismatch) > 0:
            print "[%d] have the following mismatches: " % rank, np.unique(pd['host'][hostmismatch]), np.sum(hostmismatch), " / ", pd.size
        hostmismatch = ps['host'] != self.hostname
        if np.sum(hostmismatch) > 0:
            print "[%d] have the following mismatches: " % rank, np.unique(ps['host'][hostmismatch]), np.sum(hostmismatch), " / ", ps.size


        pd.sort(order=['pid','startTime','recTime'])
        ps.sort(order=['pid','startTime','recTime'])
        po.sort(order=['pid','startTime','recTime'])
        pf.sort(order=['pid','startTime','recTime'])


        ## detect which filesystems were written to and which were read from
        fs_masks = {}
        fs_types = [
            ('pid',ps['pid'].dtype),
            ('startTime',ps['startTime'].dtype),
            ('recTime',ps['recTime'].dtype),
            ('fread',bool),('fwrite',bool)
        ]
        for fs in monitored_filesystems:
            fsq = re.compile(monitored_filesystems[fs])
            fsmatch = np.vectorize(lambda x: bool(fsq.match(x)))
            if pf.size > 0:
                fs_mask = fsmatch(pf['path'])
            else:
                fs_mask = np.empty(0, dtype=bool)
            fs_masks[fs] = fs_mask
            fs_types.append(('%s' % fs, np.bool,))
        fsData = None
        if pf.size > 0:
            fsData = np.zeros(shape=pf.size, dtype=np.dtype(fs_types))
            fsData['pid'] = pf['pid']
            fsData['startTime'] = pf['startTime']
            fsData['recTime'] = pf['recTime']
            fsData['fread'] = (stat.S_IRUSR & pf['mode']) > 0
            fsData['fwrite'] = (stat.S_IWUSR & pf['mode']) > 0
            for fs in fs_masks:
                fsData[fs] = fs_masks[fs]
        else:
            fsData = np.empty(0, dtype=np.dtype(fs_types))

        processes = np.zeros(pd_procs.size, dtype=process_dt)
        proc_status = np.ones(pd_procs.size, dtype=bool)

        def summarize_timeseries(ps_rec):
            if ps_rec.size == 0:
                raise ValueError("ALARM: not enough ps records!")

            keys = ['recTime','nRecordings','cputime','cpuRateMax','iowRateMax','iorRateMax','msizeRateMax','mresidentRateMax','cor_cpuXiow','cor_cpuXior','cor_cpuXmsize','cor_cpuXmresident','cor_iowXior','cor_iowXmsize','cor_iowXmresident','cor_iorXmsize','cor_iorXmresident','cor_msizeXmresident']
            ret = { key:np.nan for key in keys }
            cpu = (ps_rec['utime'] + ps_rec['stime']) / 100.
            ret['nRecordings'] = ps_rec.size
            ret['cputime'] = cpu[-1]
            ret['recTime'] = ps_rec['recTime'][-1] + ps_rec['recTimeUSec'][-1]*10**-6

            if ps_rec.size == 1:
                return ret

            duration = (ps_rec['recTime'] + ps_rec['recTimeUSec']*10**-6) - (ps_rec['startTime'] + ps_rec['startTimeUSec']*10**-6)
            durDiff = np.diff(duration)
            cpuRate = np.diff(cpu) / durDiff
            iowRate = np.diff(ps_rec['io_wchar']) / durDiff
            iorRate = np.diff(ps_rec['io_rchar']) / durDiff
            msizeRate = np.diff(ps_rec['m_size']) / durDiff
            mresidentRate = np.diff(ps_rec['m_resident']) / durDiff

            ret['cpuRateMax'] = np.max(cpuRate)
            ret['iorRateMax'] = np.max(iorRate)
            ret['iowRateMax'] = np.max(iowRate)
            ret['msizeRateMax'] = np.max(msizeRate)
            ret['mresidentRateMax'] = np.max(mresidentRate)

            rates = np.array(
                [cpuRate,iowRate,iorRate,msizeRate,mresidentRate],
                dtype=np.dtype([
                    ('cpuRate',cpuRate.dtype),
                    ('iowRate',iowRate.dtype),
                    ('iorRate',iorRate.dtype),
                    ('msizeRate',msizeRate.dtype),
                    ('mresidentRate',mresidentRate.dtype)
                ])
            )

            correlation = np.corrcoef(rates)
            res = ['cpu','iow','ior','msize','mresident']
            resIdx1 = 0
            resIdx2 = 0
            while resIdx1 < len(res):
                resIdx2 = resIdx1 + 1
                while resIdx2 < len(res):
                    cor_colname = 'cor_%sX%s' % (res[resIdx1],res[resIdx2])
                    ret[cor_colname] = correlation[resIdx1][resIdx2]
                    resIdx2 += 1
                resIdx1 += 1
            return ret

        def pd_last_record(x):
            unk = re.compile('^Unknown$')
            unkmatch = np.vectorize(lambda x: bool(unk.match(x)))
            mask = unkmatch(x['exePath'])
            retidx = x.size - 1
            isParent = np.nanmax(x['isParent'])
            if np.sum(mask) > 0:
                indices = np.array(np.nonzero(~mask)[0])
                if indices.size > 0:
                    retidx = np.max(indices)

            return (retidx,isParent)

        def summarize_fs(fs_rec):
            ret = {}
            for fs in monitored_filesystems:
                ret['fs_%s_write' % fs] = 1 if np.sum(fs_rec['fwrite'] & fs_rec[fs]) > 0 else 0
                ret['fs_%s_read' % fs] = 1 if np.sum(fs_rec['fread'] & fs_rec[fs]) > 0 else 0
            return ret

        for (idx,proc) in enumerate(pd_procs):
            pd_mask = pd[ ['pid','startTime'] ] == proc
            po_mask = po[ ['pid','startTime'] ] == proc
            ps_mask = ps[ ['pid','startTime'] ] == proc
            fs_mask = fsData[ ['pid','startTime'] ] == proc

            (pd_start,pd_end) = (0,0)
            (ps_start,ps_end) = (0,0)
            (fs_start,fs_end) = (0,0)
            fsrecords = {}
            try:
                (pd_start,pd_end) = self.bounds(pd_mask)
                (ps_start,ps_end) = self.bounds(ps_mask)
            except ValueError, e:
                proc_status[idx] = False
                print "[%d] %s Throwing out %d" % (rank, self.hostname, proc[0])
                continue

            try:
                (fs_start,fs_end) = self.bounds(fs_mask)
                fsrecords = summarize_fs(fsData[fs_start:fs_end])
            except ValueError, e:
                pass

            (pd_idx,isParent) = pd_last_record(pd[pd_start:pd_end])
            pd_idx = pd_start + pd_idx
            cols = list(pd.dtype.names)
            cols.remove("sortidx")
            cols.remove("recTimeUSec")
            cols.remove("startTimeUSec")
            for col in cols:
                processes[idx][col] = pd[pd_idx][col]
            if processes[idx]['host'] != self.hostname:
                print "[%d] %s: pd WARNING GOT HOSTNAME of %s" % (rank, self.hostname, processes[idx]['host'])
            processes[idx]['origStartTime'] = pd[pd_idx]['startTime']

            cols = list(ps.dtype.names)
            cols.remove("sortidx")
            cols.remove("recTimeUSec")
            cols.remove("startTimeUSec")
            ps_idx = ps_end - 1
            if ps[ps_idx]['pid'] != processes[idx]['pid']:
                print "[%d] %s: ps WARNING WRONG PID! %d != %d != %d" % (rank, self.hostname, proc[0], ps[ps_idx]['pid'], processes[idx]['pid'])
            for col in cols:
                processes[idx][col] = ps[ps_idx][col]
            if processes[idx]['host'] != self.hostname:
                print "[%d] %s: ps WARNING GOT HOSTNAME of %s" % (rank, self.hostname, processes[idx]['host'])

            ## set some initial values for the _net columns
            if processes[idx]['startTime'] < start_time:
                processes[idx]['startTime_baseline'] = start_time
                processes[idx]['cputime_net'] = processes[idx]['cputime'] - (ps[ps_start]['utime'] + ps[ps_start]['stime'])/100.
                processes[idx]['utime_net'] = processes[idx]['utime'] - ps[ps_start]['utime']
                processes[idx]['stime_net'] = processes[idx]['stime'] - ps[ps_start]['stime']
                processes[idx]['io_rchar_net'] = processes[idx]['io_rchar'] - ps[ps_start]['io_rchar']
                processes[idx]['io_wchar_net'] = processes[idx]['io_wchar'] - ps[ps_start]['io_wchar']
            else:
                processes[idx]['startTime_baseline'] = processes[idx]['startTime']
                processes[idx]['cputime_net'] = processes[idx]['cputime']
                processes[idx]['utime_net'] = processes[idx]['utime']
                processes[idx]['stime_net'] = processes[idx]['stime']
                processes[idx]['io_rchar_net'] = processes[idx]['io_rchar']
                processes[idx]['io_wchar_net'] = processes[idx]['io_wchar']
            processes[idx]['duration'] = processes[idx]['recTime'] - processes[idx]['startTime_baseline']
            processes[idx]['isParent'] = isParent
            processes[idx]['nObservations'] = np.sum(po_mask)

            try:
                timeseries = summarize_timeseries(ps[ps_start:ps_end])
                for key in timeseries:
                    try:
                        processes[idx][key] = timeseries[key]
                    except IndexError, e:
                        print e, key
                        print processes[idx]
                        raise e
            except ValueError, e:
                proc_status[idx] = False
                print "[%d] %s Throwing out %d" % (rank, self.hostname, proc[0])
                continue

            processes[idx]['volatilityScore'] = (processes[idx]['nRecordings'] - 1)/processes[idx]['nObservations']
            for key in fsrecords:
                processes[idx][key] = fsrecords[key]

        self.procmon['processes'] = processes[proc_status]

    def set_baseline(self, baseline, start_time):
        if 'processes' not in self.procmon or self.procmon['processes'].size == 0:
            return

        baseline_ps = baseline['procstat']
        baseline_ps.sort(order=['pid','startTime','recTime'])

        processes = self.procmon['processes']
        early_starters = processes['startTime'] < start_time

        early_procs = np.unique(processes[early_starters][['pid','origStartTime']])
        for proc in early_procs:
            baseline_mask = baseline_ps[ ['pid','startTime']] == proc
            proc_idx = np.nonzero(processes[['pid','origStartTime']] == proc)[0][0]
            (start,end) = (0,0)
            try:
                (start,end) = self.bounds(baseline_mask)
            except:
                pass
            end -= 1
            if end > 0:
                processes[proc_idx]['utime_net'] = processes[proc_idx]['utime'] - baseline_ps[end]['utime']
                processes[proc_idx]['stime_net'] = processes[proc_idx]['stime'] - baseline_ps[end]['stime']
                processes[proc_idx]['io_rchar_net'] = processes[proc_idx]['io_rchar'] - baseline_ps[end]['io_rchar']
                processes[proc_idx]['io_wchar_net'] = processes[proc_idx]['io_wchar'] - baseline_ps[end]['io_wchar']
                processes[proc_idx]['cputime_net'] = processes[proc_idx]['cputime'] - (baseline_ps[end]['utime'] + baseline_ps[end]['stime'])/100.
                processes[proc_idx]['duration'] = processes[proc_idx]['recTime'] - start_time

    def generate_nptype(self, processes):
        if self.gentype is not None:
            return self.gentype
        types = []
        fixedTypes = {
            'nRecords': np.uint64,
            'nObservations': np.uint64,
            'host': '|S48',
            'script': '|S1024',
            'execCommand': '|S128',
            'command': '|S128',
            'username': '|S12',
            'project': '|S20',
            'job': '|S64',
            'cputime': np.float64,
            'cputime_net': np.float64,
            'duration': np.float64,
            'volatilityScore': np.float64,
            'startTime_baseline': np.float64,
        }
        for col in processes.columns:
            if col in self.orig_types['procdata'].names:
                types.append( (col, self.orig_types['procdata'].fields[col][0]) )
            elif col in self.orig_types['procstat'].names:
                types.append( (col, self.orig_types['procstat'].fields[col][0]) )
            elif col in fixedTypes:
                types.append( (col, fixedTypes[col]) )
            elif col.startswith("fs_"):
                types.append( (col, np.int8) )
            elif col.startswith("cor_") or col.endswith("Max"):
                types.append( (col, np.float64) )
            elif col.endswith('_net'):
                realcol = re.match('(.*)_net', col).groups()[0]
                found_type = np.int64
                if realcol in self.orig_types['procdata'].names:
                    found_type = self.orig_types['procdata'].fields[realcol][0]
                elif realcol in self.orig_types['procstat'].names:
                    found_type = self.orig_types['procstat'].fields[realcol][0]
                types.append( (col, found_type) )
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
    """Read job (qqacct) data from a file and return numpy array of it."""
    ret = -1
    qqacct_data = None
    if True:
        columns = [
            ('user','S48'),
            ('project','S48'),
            ('job','S24'),
            ('task','S24'),
            ('hostname','S48'),
            ('h_rt','S48'),
            ('wall','S48'),
            ('hvmem','S24'),
            ('maxvmem','S24'),
            ('ppn','S24'),
            ('sge_status','S24'),
            ('exit_status','S24'),
        ]
        qqacct_data = np.loadtxt(qqacct_file, delimiter=':', dtype=np.dtype(columns))
        ret = 0
    
    return (ret, qqacct_data)

def identify_scripts(processes):
    """For known scripting interpreter processes, work out what the executed script was."""
    executables = np.unique(processes['exePath'])

    prefixes = ["perl","python","ruby","bash","sh","tcsh","csh","java"]
    for exe in executables:
        executable = os.path.split(exe)[1]
        scriptable = False
        for prefix in prefixes:
            pattern = "^%s.*" % prefix
            if re.match(pattern, executable) is not None:
                scriptable = True
        if scriptable:
            selection = processes['exePath'] == exe
            if np.sum(selection) == 0:
                continue
            indices = np.nonzero(selection)[0]
            for idx in indices:
                args = (processes['cmdArgs'][idx]).split("|")
                script = procmon.Scriptable.Scriptable(executable, args[1::])
                processes[idx]['script'] = script

def identify_userCommand(processes):
    """ Use a simple heuristic to identify the intended command for each process
        depends on indentify_scripts already having been run """

    getExecCommand = np.vectorize(lambda x: x.split('/')[-1])
    processes['execCommand'] = getExecCommand(processes['exePath'])
    command = getExecCommand(processes['script'])
    mask = (command == "COMMAND") | (command == "")
    command[mask] = processes['execCommand'][mask]
    processes['command'] = command

    for search in commands_modifications:
        (column,replace) = commands_modifications[search]
        searchre = re.compile(search)
        matchCmdMod = np.vectorize(lambda x: False if x is None else bool(searchre.match(x)))
        replaceCmdMod = np.vectorize(lambda x: searchre.sub(replace, x))

        mask = matchCmdMod(processes[column])
        if np.sum(mask) > 0:
            processes['command'][mask] = replaceCmdMod(processes[column][mask])

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
    if rank == 0:
        print "host proc cnts: ", list(host_proc_cnt)
        print "n hosts: ", len(host_list)
    used_ranks = size
    if len(host_list) < size:
        used_ranks = len(host_list)
    if used_ranks == 0:
        return np.empty(shape=0, dtype=np.int64)

    rank_host_cnt = np.zeros(dtype=np.int64, shape=used_ranks)
    rank_proc_cnt = np.zeros(dtype=np.int64, shape=used_ranks)
    totalprocs = sum(host_proc_cnt)
    goal_procs_per_rank = totalprocs / used_ranks

    rank_host_cnt[:] = len(host_list) / used_ranks
    rank_host_cnt[0:len(host_list)%used_ranks] += 1

    ## implement really crappy optimization
    iter = 0
    while True:
        h_idx = 0
        for r_idx in xrange(used_ranks):
            rank_proc_cnt[r_idx] = np.sum(host_proc_cnt[h_idx:h_idx+rank_host_cnt[r_idx]])
            h_idx += rank_host_cnt[r_idx]
        maxidx = np.argmax(rank_proc_cnt)
        minidx = np.argmin(rank_proc_cnt)
        iter += 1
        if rank_host_cnt[minidx] <= 0 or (iter < 100000 and rank_proc_cnt[maxidx] > goal_procs_per_rank*1.2):
            rank_host_cnt[minidx] += 1
            rank_host_cnt[maxidx] -= 1
        else:
            break

    print "[%d] rank_hosts: " % rank, rank_host_cnt, rank_proc_cnt
    return rank_host_cnt

def divide_data(host_list, rank_hosts, h5parser, dset_name):
    """Calculate which rows of the current dataset go to which MPI processes.
    @param host_list: ordered list of hosts
    @param rank_hosts: list of number of hosts per rank, sorted like host_list
    @param h5parser: RawH5Parser object containing parsed procmon data
    @param dset_name: string of dataset name (procdata, procstat, ...)

       This function needs to iterate through each dataset and construct
       a very large single array of each dataset, as well as an index indicating which
       records are for which host.
    """
    global comm
    global size
    global rank
    
    h_idx = 0
    retdata = None
    perhost_count = np.zeros(shape=len(host_list), dtype=np.int64)
    perhost_index = np.zeros(shape=len(host_list), dtype=np.int64)
    perrank_counts = np.zeros(shape=len(rank_hosts), dtype=np.int64)
    perrank_offsets = np.zeros(shape=len(rank_hosts), dtype=np.int64)

    if dset_name in h5parser.datasets:
        retdata = h5parser.datasets[dset_name]

    last_host_index = 0
    while h_idx < len(host_list):
        host_data = None
        hostname = host_list[h_idx]
        if hostname in h5parser.hosts:
            file_idx = h5parser.hosts.index(hostname)
            perhost_index[h_idx] = h5parser.host_offsets[dset_name][file_idx]
            perhost_count[h_idx] = h5parser.host_counts[dset_name][file_idx]
            last_host_index = perhost_index[h_idx] + perhost_count[h_idx]
        else:
            perhost_count[h_idx] = 0
            perhost_index[h_idx] = last_host_index
        h_idx += 1
   
    host_base_indices = np.hstack((0,np.cumsum(rank_hosts)))[0:-1]
    for r_idx in xrange(len(rank_hosts)):
        ## the following can cause a crash if the final rank has no
        ## work to do (since the host_base_index will be equal
        ## to the array length)
        try:
            h_idx = host_base_indices[r_idx]
            perrank_offsets[r_idx] = perhost_index[h_idx]
        except IndexError, e:
            print "[%d] IdxError 1 "%rank, e
            print "[%d] IdxError 1 "%rank, h_idx,ii,(h_idx+ii),r_idx
            print "[%d] IdxError 1 "%rank, rank_hosts
            raise
        for ii in xrange(rank_hosts[r_idx]):
            try:
                perhost_index[h_idx+ii] -= perrank_offsets[r_idx]
            except IndexError, e:
                print "[%d] "%rank, e
                print "[%d] "%rank, h_idx,ii,(h_idx+ii),r_idx
                print "[%d] "%rank, rank_hosts
                raise

    datasize = retdata.size if retdata is not None else 0
    last_rank_cnt = datasize - perrank_offsets[-1]
    perrank_counts = np.hstack( (np.diff(perrank_offsets),last_rank_cnt,) )

    perrank = np.column_stack( (perrank_offsets, perrank_counts,) )
    perhost = np.column_stack( (perhost_index, perhost_count) )

    return [retdata, perrank, perhost]

def mpi_transfer_datasets(root, host_list, rank_hosts, h5parser):
    global comm
    global rank
    global size

    host_data = {}
    for dataset in ['procdata','procstat','procobs','procfd']:
        print '[%d] getting ready to transmit %s' % (rank, dataset)
        (send_data, perrank, perhost) = divide_data(host_list, rank_hosts, h5parser, dataset)

        np_type = None
        if rank == root:
            np_type = send_data.dtype

        np_type = comm.bcast(np_type, root=root)
        mpi_type = generate_mpi_type_simple(np_type)

        # if this process didn't read an hdf5 file, it won't have any data
        # to send, so put in some place-holder empties
        if send_data is None or len(send_data) == 0:
            send_data = np.zeros(shape=size, dtype=np_type)
            perrank = np.column_stack((
                np.ones(shape=size, dtype=np.int64),
                np.arange(start=size, dtype=np.int64),
            ))

        #print "[%d]: perrank: " % rank, perrank
        perrank_recv = np.zeros(shape=(size,2), dtype=np.int64)
        ## transmit the rank indices/counts via alltoall
        comm.Alltoallv([perrank, [2]*size, range(0,2*size,2), MPI.LONG], [perrank_recv, [2]*size, range(0,2*size,2), MPI.LONG])

        #print "[%d]: perrank_recv: " % rank, perrank_recv
        
        print "[%d]: perhost: " % rank, perhost.size
        perhost_rankcnt = rank_hosts.copy() * 2
        perhost_rankoffset = np.hstack( (0, np.cumsum(perhost_rankcnt)))[0:-1]

        perhost_recv = np.zeros(shape=(size*rank_hosts[rank], 2), dtype=np.int64)
        perhost_recv_count = np.array([2*rank_hosts[rank]] * size, dtype=np.int64)
        perhost_recv_offset = np.hstack( (0, np.cumsum(perhost_recv_count)))[0:-1]

        ## transmit the host relative indices/counts via alltoall
        comm.Alltoallv(
            [perhost, perhost_rankcnt, perhost_rankoffset, MPI.LONG], 
            [perhost_recv, perhost_recv_count, perhost_recv_offset, MPI.LONG]
        )
        print "[%d]: recv perhost: " % rank, perhost_recv.size

        try:
            recv_data = np.zeros(shape=np.sum(perrank_recv[:,1]), dtype=np_type)
        except MemoryError, e:
            print "[%d] MemoryError on allocate: %d" % (rank, np.sum(perrank_recv[:,1])), perrank_recv[:,1], np_type
            raise e
        outbound_counts = perrank[:,1]
        outbound_offsets = perrank[:,0]
        data_recv_counts = perrank_recv[:,1]
        data_recv_offsets = np.hstack((0, np.cumsum(data_recv_counts)))[0:-1]
        #data_recv_offsets = np.hstack((0, np.cumsum(data_recv_counts)))[0:-1]
        print "[%d] data_recv_counts: "% rank, list(outbound_counts), list(data_recv_counts)
        print "[%d] data_recv_offsets: "% rank, list(outbound_offsets), list(data_recv_offsets)
        print "[%d] data: "%rank, send_data.size
        print "[%d] recv_data: "%rank, recv_data.size
        ## transmit the actual dataset
        #if rank == 2:
        #    objs = [send_data, perrank, perhost]
        #    cPickle.dump(objs, open("rank2save.p", "wp"))

        comm.Alltoallv(
            [send_data, outbound_counts, outbound_offsets, mpi_type],
            [recv_data, data_recv_counts, data_recv_offsets, mpi_type]
        )

        # clean up useless memory a bit
        del send_data
        h5parser.free(dataset)

        host_base_indices = np.hstack((0, np.cumsum(rank_hosts)))[0:-1]
        host_counts = perhost_recv[:,1].copy()
        host_rel_idx = perhost_recv[:,0].copy()

        (remap_idx,byhost_offsets,byhost_count) = reorganize_data(recv_data, perhost_recv, data_recv_offsets, rank, rank_hosts, host_list, dataset)
        try:
            #recv_data['sortidx'] = remap_idx
            #recv_data.sort(order=['sortidx'])
            recv_data = recv_data[remap_idx]
        except IndexError, e:
            objs = [recv_data.size, perhost_recv, data_recv_counts, data_recv_offsets, rank, rank_hosts, host_list, dataset, remap_idx, byhost_offsets, byhost_count]
            cPickle.dump(objs, open("indexError.%d.p"%rank, "wb"))
            raise e
        host_base_indices = np.hstack((0, np.cumsum(rank_hosts)))[0:-1]
        for h_idx in xrange(rank_hosts[rank]):
            host = host_list[host_base_indices[rank] + h_idx]
            base = byhost_offsets[h_idx]
            limit = base + byhost_count[h_idx]
            if host not in host_data:
                host_data[host] = {}
            host_data[host][dataset] = recv_data[base:limit]
        continue
    return host_data
    if False:
        components = {}
        for rank_idx in xrange(len(rank_hosts)):
            rank_offset = data_recv_offsets[rank_idx]
            for ii in xrange(rank_hosts[rank]):
                h_idx = host_base_indices[rank] + ii
                host = host_list[h_idx]
                if host_counts[ii] == 0:
                    continue
                base = rank_offset + host_rel_idx[ii]
                limit = base + host_counts[ii]
                if host not in components:
                    components[host] = []
                components[host].append(recv_data[base:limit])
        for ii in xrange(rank_hosts[rank]):
            h_idx = host_base_indices[rank] + ii
            if host not in components:
                continue
            if host not in complete_host_processes:
                complete_host_processes[host] = {}
            complete_host_processes[host][dataset] = np.concatenate(components[host])
    return complete_host_processes


def reorganize_data(recv_data, perhost_recv, perrank_offsets, rank, rank_hosts, host_list, dataset):
    ## calculate an index array to reorder the received data into host
    ## ordered data
    data_host_idx = np.zeros(shape=recv_data.size, dtype=np.int64)
    host_counts = perhost_recv[:,1].copy()
    host_rel_idx = perhost_recv[:,0].copy()
    nranks = len(rank_hosts)

    # reshaping the host_counts array to put all values by host in axis 1
    # thus allowing an easy (and efficient) summation
    host_counts = host_counts.reshape((rank_hosts[rank], nranks), order='F')
    host_rel_idx = host_rel_idx.reshape((rank_hosts[rank], nranks), order='F')
    byhost_count = np.sum(host_counts, axis=1)
    byhost_offsets = np.hstack((0,np.cumsum(byhost_count)))[0:-1]

    # now we know (1) how many records are per-host, (2) where they should end up,
    # (3) by extension how much extra space there is need to populate the
    # data_host_idx but putting current indices into the positions where they will
    # eventually need to end up. to do this, iterate through the recv_data array
    # (kind of) in the order it was sent, populating blocks of the data_host_idx
    # array as we go.
    recv_abs_idx = host_rel_idx + perrank_offsets
    recv_abs_lim = recv_abs_idx + host_counts
    host_indices = np.mgrid[0:rank_hosts[rank],0:size][0]


    #recv_idx = np.dstack( (recv_abs_idx, recv_abs_lim) )
    local_abs_idx = np.hstack( (0, np.cumsum(host_counts.ravel())))[0:-1]
    local_abs_idx = local_abs_idx.reshape((rank_hosts[rank], nranks), order='C')  # c ordering because host_counts is re-ordered by earlier transformation
    local_abs_lim = local_abs_idx + host_counts
    #local_idx = np.dstack((local_abs_idx, local_abs_lim))
    all_idx = np.dstack((recv_abs_idx, recv_abs_lim,local_abs_idx, local_abs_lim))
    def assign(x):
        data_host_idx[x[2]:x[3]] = np.arange(x[0],x[1])
        return x
    try:
        np.apply_along_axis(assign, 2, all_idx)
    except IndexError, e:
        print "[%d] failed to Index: all_idx: %d" % (rank, all_idx.size), all_idx
        raise e
    return (data_host_idx, byhost_offsets, byhost_count,)

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
    #ref_hosts = ["sgi05a01","sgi05a02","sgi05a03","sgi05a04","sgi05a05","sgi05a06","sgi05a07","sgi05a08","sgi05a09","sgi05a10","sgi05a11","sgi05a12","sgi05a13","sgi05a14","sgi05a15","sgi05a16","sgi05a17","sgi05a18","sgi05a19","sgi05a20"]
    input_parser = procmon.H5Parser.RawH5Parser()
    input_parser.parse(filenames)#, ref_hosts)
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
    print "[%d] done parsing baseline files" % rank

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
    for (idx,host) in enumerate(complete_host_processes):
        hostprocs = HostProcesses(host)
        hostprocs.reduce_data(complete_host_processes[host], start_time, float(idx), len(complete_host_processes))
        host_processes[host] = hostprocs

    print "[%d] data reduction complete, starting baseline" % rank
    for host in host_processes:
        host_processes[host].set_baseline(baseline_host_processes[host], start_time)
    print "[%d] baselining complete" % rank

    for host in host_processes:
        processes = host_processes[host].procmon['processes']
        if processes.size == 0:
            continue
        identify_scripts(processes)
        identify_userCommand(processes)
        identify_users(processes)
        integrate_job_data(processes, qqacct_data)

    ret_processes = {}
    hosts = host_processes.keys()
    for host in hosts:
        processes = host_processes[host].procmon['processes']
        if processes.size == 0:
            continue
        ret_processes[host] = processes
        del host_processes[host]

    return ret_processes

def identify_users(processes):
    uids = np.unique(processes['realUid'])
    processes['username'] = 'unknown uid'
    user_hash = {}
    for uid in uids:
        uid_processes_mask = processes['realUid'] == uid
        try:
            uid = int(uid)
            if uid not in user_hash:
                user_hash[uid] = pwd.getpwuid(uid)
            processes['username'][uid_processes_mask] = user_hash[uid].pw_name
        except:
            pass
    return user_hash

def integrate_job_data(processes, qqacct_data):
    if qqacct_data is None:
        processes['project'][:] = 'unknown project'

    identifiers = np.unique(processes['identifier'])
    for ident in identifiers:
        job_mask = qqacct_data['job'] == ident
        mask = processes['identifier'] == ident
        project = "unknown project"
        if np.sum(job_mask) > 0:
            index = np.min(np.nonzero(job_mask)[0])
            project = qqacct_data[index]['project']
        if np.sum(mask) > 0:
            processes['project'][mask] = project

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
    host_offset = sum(process_cnts[0:rank])
    local_offset = 0
    for host in host_processes:
        h5_base = host_offset + local_offset
        local_offset += host_processes[host].size

        hproc_base = 0
        while hproc_base < host_processes[host].size:
            # write 10000 records at a time
            nrec = min(10000, host_processes[host].size - hproc_base)
            dset[h5_base:(h5_base+nrec)] = host_processes[host][hproc_base:(hproc_base+nrec)]
            hproc_base += nrec
            h5_base += nrec

    output.close()


if __name__ == "__main__":
    main(sys.argv[1:])
    pass
