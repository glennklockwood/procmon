#!/usr/bin/env python

import sys
import os
import re
import subprocess
from datetime import datetime,timedelta
ticksPerSec       = 100.


import numexpr
numexpr.set_num_threads(1)
import pandas
import numpy

H5FILEPATH="/global/projectb/statistics/procmon/genepool"

def formatBytes(byteVal):
    exp = 0
    fmt = float(byteVal)
    unitString = " kMGTPEZY"
    while fmt > 1000:
        fmt /= 1024
        exp += 1

    return "%1.2f%cB" % (fmt, unitString[exp])

class JobRecord:
    """Representation of a single job execution."""

    def __init__(self, inputLine, h5cache):
        """Build a JobRecord instance.

           Arguments:
             self -- reference to this object
             inputLine -- comma delimited string containing required job data
               Must contain:
                  start time - unix epoch timestamp (must not be 0)
                  end time   - unix epoch timestamp (must not be 0)
                  job        - integral job number
                  task       - integral task number (job array id)
                  user       - string username
                  project    - string project/repo
                  host       - fully qualified hostname
                  cpu        - total cpu seconds
                  maxvmem    - bytes of vmem used
            Returns: initialized JobRecord
        """

        self.h5cache = h5cache
        (start,end,job,task,user,project,host,cpu,maxvmem) = (None,None,None, None, None, None, None,None,None)
        if 1==1:
            (start,end,job,task,user,project,host,cpu,maxvmem) = inputLine.split(',')
            self.start = datetime.fromtimestamp(int(start))
            self.end = datetime.fromtimestamp(int(end))
            self.job = int(job)
            self.task = int(task)
            self.display_task = task
            if self.task == 0:
                self.task = 1
                self.display_task = ""
            mendelibMatch = re.match(r'([A-Za-z0-9\-\_]+?)-ib\.nersc\.gov', host)
            otherMatch = re.match(r'([A-Za-z0-9\-\_]+?)\.nersc\.gov', host)
            if mendelibMatch is not None:
                self.host = mendelibMatch.groups(1)[0]
            elif otherMatch is not None:
                self.host = otherMatch.groups(1)[0]
            else:
                raise ValueError("invalid host!")

            self.cpu = float(cpu)
            self.maxvmem = float(maxvmem)
            self.project = project
            self.user = user

            self.h5files = None
            self.ps = None
            self.pd = None
            self.fd = None
            self.__job_data_finalized = False
            self.earliestStart = None
            self.duration = None

    def __repr__(self):
        """Format JobRecord as a string."""
        return "%s%s (%s - %s) By %s/%s@%s" % (self.job, ".%s" % self.display_task if self.display_task != "" else "", self.start.strftime("%Y-%m-%d %H:%M:%S"), self.end.strftime("%Y-%m-%d %H:%M:%S"), self.user, self.project, self.host)
    
    def get_h5files(self):
        """Calculate h5 filenames."""
        if self.h5files is None:
            self.h5files = h5cache.query(self.start, self.end)
            if self.h5files is None:
                self.h5files = [] 
        return self.h5files

    def __merge_records(self, existing_records, new_records):
        existing_records_count = 0
        records = 0
        if new_records is not None and not new_records.empty:
            records = new_records.shape[0]
            if existing_records is None:
                existing_records = new_records
            else:
                existing_records.update(new_records)
                addList = numpy.invert(new_records.index.isin(existing_records.index))
                if len(addList) > 0:
                    newdata = new_records.ix[addList]
                    existing_records = pandas.concat([existing_records, newdata], axis = 0)
        return existing_records

    def __calculate_rate(self, ps, name, dividend):
        ps['running%s' % name] = dividend.diff() / ps.duration.diff()
        ps['overall%s' % name] = dividend / ps.duration
        return ps


    def __finalize_job_data(self):
        if self.__job_data_finalized:
            return True
        if self.ps is not None:
            self.ps = self.ps.reset_index()
        if self.pd is not None:
            self.pd = self.pd.reset_index()
        if self.fd is not None:
            self.fd = self.fd.reset_index()

        if self.ps is None or self.pd is None:
            print 'No process data!  Perhaps the procmon system has not deposited data yet.'
            return False

        self.pd.startTime = self.pd.startTime + self.pd.startTimeUSec * 10**-6
        self.ps.startTime = self.ps.startTime + self.ps.startTimeUSec * 10**-6
        self.fd.startTime = self.fd.startTime + self.fd.startTimeUSec * 10**-6
        self.pd.recTime = self.pd.recTime + self.pd.recTimeUSec * 10**-6
        self.ps.recTime = self.ps.recTime + self.ps.recTimeUSec * 10**-6
        self.fd.recTime = self.fd.recTime + self.fd.recTimeUSec * 10**-6

        self.earliestStart = self.pd.sort("startTime",ascending=1).ix[0]['startTime']
        self.duration = self.ps.recTime.max() - self.ps.startTime.min()
        self.ps['duration'] = self.ps.recTime - self.ps.startTime
        self.ps['timeoffset'] = self.ps.recTime - self.earliestStart
        self.ps['blockedIO'] = self.ps.delayacctBlkIOTicks / ticksPerSec
        self.ps['blockedIOPct'] = self.ps['blockedIO'] / self.ps.duration
        self.ps = self.ps.sort('recTime', ascending=1)

        self.__job_data_finalized = True
        return True

    def add_job_data(self, tps, tpd, tfd):
        if self.__job_data_finalized:
            return None

        ## concatenate existing procstat records; procstat data is strictly
        ## additive since these are the changing process counters, thus include
        ## recTime in the index

        if tps is not None and not tps.empty:
            lps = tps.sort('recTime', ascending=0).set_index(['pid','startTime', 'recTime'])
            lps = lps.groupby(level=range(len(lps.index.levels))).first()
            self.ps = self.__merge_records(self.ps, lps)

        ## merge existing procdata records with new ones;  these need to be
        ## handled carefully to get a mostly unique set of data
        ##      keyed on pid, starttime, executable name
        if tpd is not None and not tpd.empty:
            lpd = tpd.sort('recTime', ascending=0).set_index(['pid','startTime'])#,'exePath','cmdArgs'])
            lpd = lpd.groupby(level=range(len(lpd.index.levels))).first()
            self.pd = self.__merge_records(self.pd, lpd)

        ## merge existing procfd records with new ones;  these need to be
        ## handled carefully to get a unique set of data
        ##      keyed on pid, starttime, filename, mode
        if tfd is not None and not tfd.empty:
            lfd = tfd.sort('recTime',ascending=0).set_index(['pid','startTime','path','fd','mode'])
            lfd = lfd.groupby(level=range(len(lfd.index.levels))).first()
            self.fd = self.__merge_records(self.fd, lfd)

    def __build_pstree(self, ppids_set, pd):
        process_tree = []
        for (idx,proc) in ppids_set.iterrows():
            children = pd.ix[numpy.in1d(pd.ppid, proc['pid'])]
            children_array = self.__build_pstree(children, pd)
            ps = self.ps.ix[self.ps.pid == proc['pid']].sort('recTime', ascending=1)
            ps = self.__calculate_rate(ps, 'CPURate', (ps.utime + ps.stime)/ticksPerSec)
            ps = self.__calculate_rate(ps, 'IORead', ps.io_rchar*1.)
            ps = self.__calculate_rate(ps, 'IOWrite', ps.io_wchar*1.)
            ps = self.__calculate_rate(ps, 'IOReadBytes', ps.io_readBytes*1.)
            ps = self.__calculate_rate(ps, 'IOWriteBytes', ps.io_writeBytes*1.)
            process_tree.append( (proc, children_array, ps) )
        return process_tree

    def __print_pstree(self, curr, level):
        global enable_color
        red_color = "\x1b[31;1m"
        normal_color = "\x1b[0m"
        if not enable_color:
            red_color = ""
            normal_color = ""
        prefix = ""
        if level > 0:
            prefix = " " * level
        for node in curr:
            pd = node[0]
            ps = node[2]
            if ps is None:
                print "missing ps data!"
                continue
            startSec = pd['startTime'] - self.earliestStart
            startPct = (startSec / self.duration)*100 
            ps =  ps.sort('duration', ascending=0).reset_index()
            psLatest = ps.ix[0]
            durationPct = (psLatest['duration'] / self.duration)*100
            print "%s+-- %s%s %s%s (pid: %d) Start: %1.1fs (%1.2f%%);  Duration: %1.1fs (%s%1.2f%%%s); MaxThreads: %d" % (prefix, red_color, pd['execName'], pd['exePath'], normal_color, pd['pid'], startSec, startPct, psLatest['duration'], red_color if durationPct >= 5 else "", durationPct, normal_color if durationPct >= 5 else "", ps.numThreads.max())
            print "%s \--- CPU Time: %fs; Max CPU %%: %1.2f%%;  Mean CPU %%: %1.2f%%" % (prefix, (psLatest['utime']+psLatest['stime'])/ticksPerSec, ps.runningCPURate.max()*100, psLatest['overallCPURate']*100)
            print "%s |--- Virtual Memory Peak: %s; Resident Memory Peak: %s" % (prefix, formatBytes(psLatest['vmpeak']), formatBytes(psLatest['rsspeak']))
            print "%s |--- IO read(): %s; IO write(): %s; FS Read*: %s; FS Write*: %s; Time Blocked on IO: %1.2fs (%1.2f%%)" % (prefix, formatBytes(psLatest['io_rchar']), formatBytes(psLatest['io_wchar']), formatBytes(psLatest['io_readBytes']), formatBytes(psLatest['io_writeBytes']), psLatest['blockedIO'], psLatest['blockedIOPct']*100)
            if show_cmdline_args:
                print "%s |--- CmdLine: %s" % (prefix, pd['cmdArgs'].replace("|", " "))
            if show_cwd:
                print "%s |--- cwdPath: %s" % (prefix, pd['cwdPath'])
            self.__print_pstree(node[1], level+1)

    def print_job(self):
        if not self.__finalize_job_data():
            return
        pd = self.pd.sort(columns=('startTime','pid'), ascending=(1,1))

        # find processes with absentee parents (i.e., not of this job)
        root_procs_set = pd.ix[numpy.invert(numpy.in1d(pd.ppid, pd.pid))]

        ## build process hierarchy
        process_tree = self.__build_pstree(root_procs_set, pd)

        ## display the process hierarchy
        self.__print_pstree(process_tree, 1)

class ProcmonH5Cache:
    def __init__(self, basePath, system):
        self.localcache = []
        self.__filesystem_query_setup(basePath, system)

    def query(self, job_start, job_end):
        ret = []
        for rec in self.localcache:
            if ((rec['recording_start'] <= job_start and rec['recording_stop'] >= job_start) or
                    (rec['recording_start'] <= job_end and rec['recording_stop'] >= job_end) or
                    (rec['recording_start'] >= job_start and rec['recording_stop'] <= job_end)):

                ret.append(rec)

        #if len(ret) == 0:
        #    job_start = job_start.strftime("%Y-%m-%dT%H:%M:%S")
        #    job_end = job_end.strftime("%Y-%m-%dT%H:%M:%S")
        #    return self.__remote_query(job_start, job_end)
        return ret
        

            
#def __remote_query(self, job_start, job_end):
#        ret = []
#        remote_files = sdm.post('api/metadata/query',data={'file_type':'procmon_reduced_h5',
#            '$or': [
#                {'metadata.procmon.recording_start':{'$lte':job_start},'metadata.procmon.recording_stop':{'$gte':job_start}},
#                {'metadata.procmon.recording_start':{'$lte':job_end},'metadata.procmon.recording_stop':{'$gte':job_end}},
#                {'metadata.procmon.recording_start':{'$gte':job_start},'metadata.procmon.recording_stop':{'$lte':job_end}},
#            ]
#        })
#        for record in remote_files:
#            newrec = {}
#            newrec['recording_start'] = datetime.strptime(record['metadata']['procmon']['recording_start'], "%Y-%m-%dT%H:%M:%S.%f")
#            newrec['recording_stop'] = datetime.strptime(record['metadata']['procmon']['recording_stop'], "%Y-%m-%dT%H:%M:%S.%f")
#            newrec['data'] = record
#            newrec['path'] = '%s/%s' % (record['file_path'],record['file_name'])
#            self.localcache.append(newrec)
#            ret.append(newrec)
#        return ret

    def __filesystem_query_setup(self, basePath, system):
        files = os.listdir(basePath)
        procmonMatcher = re.compile(r'procmon_([a-zA-Z0-9]+).([0-9]+).h5')
        for filename in files:
            procmonMatch = procmonMatcher.match(filename)
            if procmonMatch is not None:
                l_system = procmonMatch.groups()[0]
                l_date = procmonMatch.groups()[1]
                date = datetime.strptime(l_date, '%Y%m%d%H%M%S')
                newrec = {}
                newrec['recording_start'] = date
                newrec['recording_stop'] = date + timedelta(hours=1)
                newrec['path'] = '%s/%s' % (basePath, filename)
                newrec['data'] = None
                self.localcache.append(newrec)

def get_qqacct_data(args, h5cache):
    qqacct_args = ['qqacct']
    qqacct_args.extend(args)
    qqacct_args.extend(['-c','start,end,job,task,user,project,hostname,cpu,maxvmem'])
    process = subprocess.Popen(qqacct_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    (stdout,stderr) = process.communicate()
    jobs = []
    if process.returncode == 0:
        lines = stdout.splitlines()
        for line in lines:
            jobs.append(JobRecord(line, h5cache))
        return jobs
    else:
        sys.stderr.write('Failed to run qqacct!')
        return None

def get_qs_data(args, h5cache):
    qs_args = ['qs', '--style','dmj']
    qs_args.extend(args)

def usage():
    print "catjob <arguments> <query>"
    print "Arguments:"
    print "  --no-color    Do not use ANSI coloring"
    print "  --args        Display command-line arguments (up to 1024 characters)"
    print "  --cwd         Disploy working directories" 
    print "  --help        Display this help message"
    print "Query:"
    print "   All remaining arguments will be passed to qqacct;  see qqacct manpage"
    print "   for more details."
    sys.exit(0)

h5cache = ProcmonH5Cache('/global/projectb/statistics/procmon/genepool', 'genepool')
args = []
idx = 1
enable_qqacct = True
enable_qs = False
enable_color = True
show_cmdline_args = False
show_cwd = False
while idx < len(sys.argv):
    if sys.argv[idx] == "--qs":
        enable_qs = True
        enable_qqacct = False
    elif sys.argv[idx] == "--no-color":
        enable_color = False
    elif sys.argv[idx] == "--args":
        show_cmdline_args = True
    elif sys.argv[idx] == "--cwd":
        show_cwd = True
    elif sys.argv[idx] == "--help":
        usage()
    else:
        args.append(sys.argv[idx])
    idx += 1

if len(args) == 0:
    print "ERROR: no arguments to pass to qqacct"
    print "catjob needs a valid qqacct query!"
    sys.exit(1)

jobs = []
if enable_qqacct:
    jobs = get_qqacct_data(args, h5cache)
elif enable_qs:
    jobs = get_qs_data(args, h5cache)

h5files = {}
for job in jobs:
    for h5file in job.get_h5files():
        filename = h5file['path']
        if filename not in h5files:
            h5files[filename] = {}
        if job.host not in h5files[filename]:
            h5files[filename][job.host] = []
        h5files[filename][job.host].append(job)

files = sorted(h5files.keys())
for h5file in files:
    for host in h5files[h5file].keys():
        (procstat, procdata, procfd) = (None, None, None)
        h5 = pandas.io.pytables.HDFStore(h5file, mode='r')

        try:
            procstat = h5.select('/%s/procstat' % host)
        except:
            pass
        try:
            procdata = h5.select('/%s/procdata' % host)
        except:
            pass
        try:
            procfd = h5.select('/%s/procfd' % host)
        except:
            pass
        h5.close()
        for job in h5files[h5file][host]:
            (ps,pd,fd) = (None,None,None)
            if procstat is not None and not procstat.empty:
                ps = procstat.ix[(procstat.identifier == str(job.job)) & (procstat.subidentifier == str(job.task))]
            if procdata is not None and not procdata.empty:
                pd = procdata.ix[(procdata.identifier == str(job.job)) & (procdata.subidentifier == str(job.task))]
            if procfd is not None and not procfd.empty:
                fd = procfd.ix[(procfd.identifier == str(job.job)) & (procfd.subidentifier == str(job.task))]
            job.add_job_data(ps,pd,fd)
            
for job in jobs:
    print job
    job.print_job()
    print 

