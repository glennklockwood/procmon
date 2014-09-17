#!/usr/bin/env python

import sys
import os
import re
import subprocess
import json
import procmon
from datetime import datetime,timedelta
ticksPerSec       = 100.


import numexpr
numexpr.set_num_threads(1)
import pandas
import numpy

class NumpyJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, numpy.int32):
            return int(obj)
        if isinstance(obj, numpy.int64):
            return int(obj)
        return json.JSONEncoder.default(self, obj)

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
            self.obs = None
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
        if self.obs is not None:
            self.obs = self.obs.reset_index()

        if self.ps is None or self.pd is None:
            print 'No process data!  Perhaps the procmon system has not deposited data yet.'
            return False

        self.pd.startTime = self.pd.startTime + self.pd.startTimeUSec * 10**-6
        self.ps.startTime = self.ps.startTime + self.ps.startTimeUSec * 10**-6
        self.fd.startTime = self.fd.startTime + self.fd.startTimeUSec * 10**-6
        self.obs.startTime = self.obs.startTime + self.obs.startTimeUSec * 10**-6
        self.pd.recTime = self.pd.recTime + self.pd.recTimeUSec * 10**-6
        self.ps.recTime = self.ps.recTime + self.ps.recTimeUSec * 10**-6
        self.fd.recTime = self.fd.recTime + self.fd.recTimeUSec * 10**-6
        self.obs.recTime = self.obs.recTime + self.obs.recTimeUSec * 10**-6

        self.earliestStart = self.pd.sort("startTime",ascending=1).ix[0]['startTime']
        self.duration = self.ps.recTime.max() - self.ps.startTime.min()
        self.ps['duration'] = self.ps.recTime - self.ps.startTime
        self.ps['timeoffset'] = self.ps.recTime - self.earliestStart
        self.ps['blockedIO'] = self.ps.delayacctBlkIOTicks / ticksPerSec
        self.ps['blockedIOPct'] = self.ps['blockedIO'] / self.ps.duration
        self.ps = self.ps.sort('recTime', ascending=1)

        self.__job_data_finalized = True
        return True

    def add_job_data(self, tps, tpd, tfd, tobs):
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

        ## merge existing procobs records with new ones; procobs data is strictly
        ## additive since these mark each observation even if it was removed 
        ## for being redundant
        if tobs is not None and not tobs.empty:
            lobs = tobs.sort('recTime',ascending=0).set_index(['pid','startTime','recTime'])
            lobs = lobs.groupby(level=range(len(lobs.index.levels))).first()
            self.obs = self.__merge_records(self.obs, lobs)

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
    print "  --host <host> --identifier <job> [--subidentifier <task>] --start YYYYMMDDHHMMSS [--end YYYYMMDDHHMMSS]"
    print "Query:"
    print "   All remaining arguments will be passed to qqacct;  see qqacct manpage"
    print "   for more details."
    sys.exit(0)

class Config:
    """ Read configuration from config file and command line. """

    def __init__(self, args):
        """ Construct ConfigParser class by parsing args/configs """
        self.config = self.read_configuration(args)

    def __split_args(self, arg_str, splitRegex):
        """ Internal utility fxn for splitting arg values

            @param arg_str string representation of argument values
            @param splitRegex string representation of splitting regex
            @return list of split strings
        """
        items = re.split(splitRegex, arg_str)
        ret_items = []
        for item in items:
            item = item.strip()
            if len(item) > 0:
                ret_items.append(item)
        return ret_items

    def split_path(self, arg_str):
        """ Split paths delimited by newlines or colons.

            @param arg_str string representation of argument values
            @return list of 
        """
        return self.__split_args(arg_str, '[:\n]')

    def parse_datetime(self, arg_str):
        return datetime.strptime(arg_str, '%Y%m%d%H%M%S')

    def read_configuration(self, args):
        global procmonInstallBase

        parser = argparse.ArgumentParser(add_help=False)
        parser.add_argument('-f', '--config', help="Specify configuration file instead of default at $PROCMON_DIR/etc/workloadAnalysis.conf", default='%s/etc/workloadAnalysis.conf' % procmonInstallBase, metavar="FILE")
        args, remaining_args = parser.parse_known_args()
        defaults = {
            "h5_path": "%s/var/procmon" % procmonInstallBase,
            "h5_prefix": "procmon",
            "base_hostlist": "",
            "host": None,
            "identifier": None,
            "subidentifier": None,
            "start": None,
            "end": None,
            "args": False,
            "no-color": False,
            "cwd": False,
        }
        if args.config and os.path.exists(args.config):
            config = SafeConfigParser()
            config.read([args.config])

        parser = argparse.ArgumentParser(parents=[parser])
        parser.set_defaults(**defaults)
        parser.add_argument('-o','--output', type=str, help="Specify output summary filename", default='output.h5')
        parser.add_argument('files', metavar='N', type=str, nargs='+', help='Processes h5 files to summarize')
        args = parser.parse_args(remaining_args, args)
        return args



h5cache = procmon.H5Cache.H5Cache('/global/projectb/statistics/procmon/genepool', 'procmon_genepool')
args = []
idx = 1
enable_qqacct = True
enable_qs = False
enable_color = True
show_cmdline_args = False
show_cwd = False
save_h5 = None
host=None
identifier=None
subidentifier=None
start=None
end=None
while idx < len(sys.argv):
    if sys.argv[idx] == "--qs":
        enable_qs = True
        enable_qqacct = False
    elif sys.argv[idx] == "--no-color":
        enable_color = False
    elif sys.argv[idx] == "--save":
        if len(sys.argv) > idx:
            idx += 1
            save_h5 = sys.argv[idx]
        else:
            usage()
            sys.exit(1)
    elif sys.argv[idx] == "--args":
        show_cmdline_args = True
    elif sys.argv[idx] == "--cwd":
        show_cwd = True
    elif sys.argv[idx] == "--help":
        usage()
    elif sys.argv[idx] == "--host":
        if len(sys.argv) > idx:
            idx += 1
            host = sys.argv[idx]
        else:
            usage()
            sys.exit(1)
    elif sys.argv[idx] == "--identifier":
        if len(sys.argv) > idx:
            idx += 1
            identifier = sys.argv[idx]
        else:
            usage()
            sys.exit(1)
    elif sys.argv[idx] == "--subidentifier":
        if len(sys.argv) > idx:
            idx += 1
            subidentifier = sys.argv[idx]
        else:
            usage()
            sys.exit(1)
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
    print h5file
    for host in h5files[h5file].keys():
        (procstat, procdata, procfd, procobs) = (None, None, None, None)
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
        try:
            procobs = h5.select('/%s/procobs' % host)
        except:
            pass
        h5.close()
        for job in h5files[h5file][host]:
            (ps,pd,fd,obs) = (None,None,None,None)
            if procstat is not None and not procstat.empty:
                ps = procstat.ix[(procstat.identifier == str(job.job)) & (procstat.subidentifier == str(job.task))]
            if procdata is not None and not procdata.empty:
                pd = procdata.ix[(procdata.identifier == str(job.job)) & (procdata.subidentifier == str(job.task))]
            if procfd is not None and not procfd.empty:
                fd = procfd.ix[(procfd.identifier == str(job.job)) & (procfd.subidentifier == str(job.task))]
            if procobs is not None and not procobs.empty:
                obs = procobs.ix[(procobs.identifier == str(job.job)) & (procobs.subidentifier == str(job.task))]
            job.add_job_data(ps,pd,fd,obs)
            
for job in jobs:
    print job
    job.print_job()
    print 

if save_h5 is not None:
    data = []
    for job in jobs:
        datum = {}
        datum['job'] = "%s%s" % (job.job, "" if job.display_task == "" else ".%s" % job.display_task)
        datum['host'] = job.host
        datum['start'] = job.start.strftime('%Y-%m-%dT%H:%M:%S')
        datum['end'] = job.end.strftime('%Y-%m-%dT%H:%M:%S')
        datum['user'] = job.user
        datum['project'] = job.project
        datum['cpu'] = job.cpu
        datum['maxvmem'] = job.maxvmem
        datum['processes'] = []
        processes = job.pd.sort('recTime', ascending=1)[['pid','ppid','startTime','recTime','execName','exePath','cmdArgs']]
        processes.cmdArgs = processes.cmdArgs.str.replace('|', ' ')
        for (idx,tprocess) in processes.iterrows():
            process = tprocess.to_dict()
            psCols = ['recTime','state','utime','stime','priority','numThreads','vsize','rss','rsslim','vmpeak','rsspeak','io_rchar','io_wchar','io_readBytes','io_writeBytes','m_size','m_resident','m_share','m_text','m_data','duration','timeoffset','blockedIO','blockedIOPct']
            psRecords = job.ps.ix[(job.ps.pid == process['pid']) & (job.ps.startTime == process['startTime'])].sort('recTime', ascending=1)[psCols].reset_index()
            process['ps'] = {}
            for col in psCols:
                process['ps'][col] = list(psRecords[col])
            process['ps']['state'] = [ chr(x) for x in process['ps']['state'] ]
            
            fdCols = ['recTime','path','fd','mode']
            fdRecords = job.fd.ix[(job.fd.pid == process['pid']) & (job.fd.startTime == process['startTime'])].sort(['recTime','fd'],ascending=[1,1])[fdCols].reset_index()
            process['fd'] = {}
            for col in fdCols:
                process['fd'][col] = list(fdRecords[col][...])

            obsRecords = job.obs.ix[(job.obs.pid == process['pid']) & (job.obs.startTime == process['startTime'])].sort('recTime',ascending=1)['recTime'].reset_index()
            process['obs'] = list(obsRecords.recTime)
            datum['processes'].append(process)
        data.append(datum)

    outfile = open(save_h5, 'w')
    outfile.write(json.dumps(data, cls=NumpyJsonEncoder))
    outfile.write('\n')
    outfile.close()
