#!/usr/bin/env python

import subprocess
import os
import sys
import argparse
import procmon
from ConfigParser import SafeConfigParser
import ConfigParser
from xml.parsers import expat
from datetime import datetime,date,timedelta,time
import shutil

procmonInstallBase = '@CMAKE_INSTALL_PREFIX@'
if 'PROCMON_DIR' in os.environ:
    procmonInstallBase = os.environ['PROCMON_DIR']

def QQAcctJobs(start, end, maxqueue):
    adjstart = start - timedelta(hours=24)
    adjend = end + timedelta(hours=maxqueue)
    cmd = ["qqacct", "-d", "dtask=max(1,task)","-S", adjstart.strftime("%Y-%m-%d"), "-E", adjend.strftime("%Y-%m-%d"), "-q", "1 == 1", "-c", "job,dtask,user,project"]
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    (stdout, stderr) = process.communicate()
    return stdout

class UgeCurrentQueue:
    ugeJob = None
    ugeJobSetting = False
    ugeJobs = []
    xmlKey = None
    xmlAttributes = None

    def runQstat(self, options):
        cmd = ["qstat", "-u", "*", "-ext", "-xml"]
        cmd.extend(options)
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        (stdout,stderr) = process.communicate()

        xmlParser = expat.ParserCreate()
        xmlParser.StartElementHandler = self.XmlStart
        xmlParser.EndElementHandler = self.XmlEnd
        xmlParser.CharacterDataHandler = self.CharData
        xmlParser.Parse(stdout, 1)

    def XmlStart(self, name, attributes):
        if name == "job_list":
            self.ugeJobSetting = True
            self.ugeJob = dict()
        elif self.ugeJobSetting:
            self.xmlKey = name
            self.xmlAttributes = attributes

    def XmlEnd(self, name):
        if name == "job_list":
            self.ugeJobSetting = False
            self.ugeJobs.append(self.ugeJob)
        elif self.ugeJobSetting:
            item = self.ugeJob
            if item is not None:
                if self.xmlAttributes is not None and self.xmlAttributes.has_key("name"):
                    data = (self.xmlAttributes["name"], self.xmlData)
                else:
                    data = self.xmlData
                self.ugeJob[self.xmlKey] = data
        self.xmlKey = None
        self.xmlData = None
        self.xmlAttributes = None

    def CharData(self, data):
        if self.ugeJobSetting:
            self.xmlData = data

    def getActiveJobs(self):
        self.runQstat(["-s", "r"])
        jobs = []
        for job in self.ugeJobs:
            user = job['JB_owner'] if 'JB_owner' in job else 'Unknown'
            project = job['JB_project'] if 'JB_project' in job else 'Unknown'
            jobnumber = job['JB_job_number'] if 'JB_job_number' in job else 'Unknown'
            task = job['tasks'] if 'tasks' in job else '1'
            jobstr =  "%s,%s,%s,%s" % (jobnumber, task, user, project)
            jobs.append(jobstr)
        return '\n'.join(jobs)

class Config:
    """ Read configuration from config file and command line. """

    def __init__(self, args):
        self.config = self.read_configuration(args)

    def read_configuration(self, args):
        global procmonInstallBase
        yesterday = date.today() - timedelta(days=1)
        start_time = datetime.combine(yesterday, time(0,0,0))
        end_time = datetime.combine(date.today(), time(0,0,0)) - timedelta(seconds=1)

        parser = argparse.ArgumentParser(add_help=False)
        parser.add_argument('-f', '--config', help="Specify configuration file instead of default at $PROCMON_DIR/etc/processAnalyzer_controller.conf", default='%s/etc/processAnalyzer_contorller.conf' % procmonInstallBase, metavar='FILE')
        args,remaining_args = parser.parse_known_args()
        defaults = {
            "output_suffix"  : "summary.h5",
            "h5_path" : "%s/var/procmon" % procmonInstallBase,
            "h5_prefix" : "procmon",
            "start": start_time.strftime("%Y%m%d%H%M%S"),
            "end":   end_time.strftime("%Y%m%d%H%M%S"),
            "intermediatePrefix": "processSummarizer.calc",
            "maxQueueLength": 48,
            "fsMonitor": None,
            "system": "custom",
            "localdir":"",
            "batchType":"",
        }
        if args.config and os.path.exists(args.config):
            config = SafeConfigParser(defaults)
            config.optionxform = str
            config.read([args.config])
            for key in defaults:
                try:
                    val = config.get('controller', key)
                    if val is not None:
                        defaults[key] = val
                except ConfigParser.NoOptionError:
                    pass

        parser = argparse.ArgumentParser(parents=[parser])
        parser.set_defaults(**defaults)
        parser.add_argument('--h5_path', type=str, help="path to raw hdf5 files")
        parser.add_argument('--h5_prefix', type=str, help="prefix for naming hdf5 files e.g., <prefix>.<datetime>.h5")
        parser.add_argument('--start', type=str, help="start time in format YYYYmmddHHMMSS")
        parser.add_argument('--end',   type=str, help="end time in format YYYYmmddHHMMSS")
        parser.add_argument('--output_suffix', type=str, help="output file")
        parser.add_argument('--batchType', type=str, help="one of UGE, torque, slurm")
        parser.add_argument('--intermediatePrefix', type=str, help="prefix of all intermediate generated intermediate files")
        parser.add_argument('--maxQueueLength', type=int, help="maximum number of hours of any batch queue")
        parser.add_argument('--fsMonitor', type=str, nargs='*', help="filesystems to monitor")
        parser.add_argument('--system',type=str)
        parser.add_argument('--localdir',type=str)
        args = parser.parse_args(remaining_args, args)

        args.start_time = datetime.strptime(args.start, "%Y%m%d%H%M%S")
        args.end_time = datetime.strptime(args.end, "%Y%m%d%H%M%S")
        args.output = "%s.%s.%s" % (args.intermediatePrefix, args.start_time.strftime("%Y%m%d%H%M%S"), args.output_suffix) 
        args.fs = []
        if type(args.fsMonitor) is str:
            for fs in args.fsMonitor.split('\n'):
                args.fs.append(fs.strip())
        else:
            for fs in args.fsMonitor:
                args.fs.append(fs.strip())
        return args

def getBatchData(config):
    batchFilename = "%s.%s.batchdata" % (config.intermediatePrefix, config.start_time.strftime("%Y%m%d%H%M%S"))
    fd = open(batchFilename, "w")
    if (config.batchType == "UGE"):
        uge = UgeCurrentQueue()
        jobs = uge.getActiveJobs()
        existing_jobs = QQAcctJobs(config.start_time, config.end_time, config.maxQueueLength)
        fd.write(jobs)
        fd.write(existing_jobs)
        fd.close()
    return batchFilename

if __name__ == "__main__":
    config = Config(sys.argv[1:]).config
    print config

    procmon_h5cache = procmon.H5Cache.H5Cache(config.h5_path, config.h5_prefix)
    filenames = [ x['path'] for x in procmon_h5cache.query(config.start_time, config.end_time) ]
    baseline_filenames = [ x['path'] for x in procmon_h5cache.query(config.start_time - timedelta(minutes=20), config.start_time - timedelta(minutes=1)) ]

    if len(filenames) == 0:
        print "Found no procmon h5 files to process"
        sys.exit(1)
    if len(baseline_filenames) == 0:
        print "Found no baseline procmon h5 files"
        sys.exit(1)

    batchFile = getBatchData(config)
    if batchFile is None:
        print "No valid batch data"
        sys.exit(1)

    print filenames
    print baseline_filenames
    print batchFile

    summarizer_config_filename = "%s.%s.summarizer.conf" % (config.intermediatePrefix, config.start_time.strftime("%Y%m%d%H%M%S"))
    fd = open(summarizer_config_filename, 'w')
    for fname in filenames:
        if config.localdir != "":
            (root,partial_fname) = os.path.split(fname)
            new_fname = os.path.join(config.localdir, partial_fname)
            if not os.path.exists(new_fname):
                shutil.copy2(fname, new_fname)
            fname = new_fname
            print fname
            
        fd.write("input = %s\n" % fname)
    for fname in baseline_filenames:
        if config.localdir != "":
            (root,partial_fname) = os.path.split(fname)
            new_fname = os.path.join(config.localdir, partial_fname)
            if not os.path.exists(new_fname):
                shutil.copy2(fname, new_fname)
            fname = new_fname
            print fname
        fd.write("baseline = %s\n" % fname)

    fd.write("start = %s\n" % config.start_time.strftime("%s"))
    fd.write("batch = %s\n" % batchFile)
    fd.write("system = %s\n" % config.system)
    for fs in config.fs:
        fd.write("fsMonitor = %s\n" % fs)
    fd.write("output = %s\n" % config.output)
