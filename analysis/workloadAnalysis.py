import os
import sys
import argparse
import ConfigParser
import re
import procmon
from datetime import date,datetime,timedelta,time
import h5py

from mpi4py import MPI

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

procmonInstallBase = ''
procmon_h5cache = None
if 'PROCMON_DIR' in os.environ:
    procmonInstallBase = os.environ['PROCMON_DIR']

class ConfigParser:
    def __init__(self, args):
        self.config = read_configuration(args)

    def __split_args(arg_str, splitRegex):
        items = re.split(splitRegex, arg_str)
        ret_items = []
        for item in items:
            item = item.strip()
            if len(item) > 0:
                ret_items.append(item)
        return ret_items

    def split_hostlist(arg_str):
        return __split_args(arg_str, '[,\n]')

    def split_path(arg_str):
        return __split_args(arg_str, '[:\n]')

    def parse_datetime(arg_str):
        return datetime.strptime(arg_str, '%Y%m%d%H%M%S')

    def read_configuration(args):
        global procmonInstallBase
        yesterday = date.today() - timedelta(days=1)
        start_time = datetime.combine(yesterday, time(0,0,0))
        end_time = datetime.combine(date.today(), time(0,0,0)) - timedelta(seconds=1)

        parser = argparse.ArgumentParser(add_help=False)
        parser.add_argument('-f', '--config', help="Specify configuration file instead of default at $PROCMON_DIR/etc/workloadAnalysis.conf", default='%s/etc/workloadAnalysis.conf' % procmonInstallBase, metavar="FILE")
        args, remaining_args = parser.parse_known_args()
        defaults = {
            "h5_path": "%s/var/procmon" % procmonInstallBase,
            "h5_prefix": "procmon",
            "base_hostlist": "",
            "start": start_time.strftime("%Y%m%d%H%M%S"),
            "end": end_time.strftime("%Y%m%d%H%M%S"),
        }
        if args.config and os.path.exists(args.config):
            config = ConfigParser.SafeConfigParser()
            config.read([args.config])
            new_defaults = dict(config.items("workloadAnalysis"))
            for key in new_defaults:
                if key in defaults:
                    defaults[key] = new_defaults[key]

        parser = argparse.ArgumentParser(parents=[parser])
        parser.set_defaults(**defaults)
        parser.add_argument("--start", help="Start time for analysis (YYYYmmddHHMMSS format)", type=parse_datetime)
        parser.add_argument("--end", help="End time for analysis (YYYYmmddHHMMSS format)", type=parse_datetime)
        parser.add_argument("--h5_path", help="Search path for h5 files", type=split_path)
        parser.add_argument("--h5_prefix", help="Prefix for h5 file names (e.g., h5-path/<prefix>.YYYYMmddhHMMSS.h5)")
        parser.add_argument("--base_hostlist", help="Core set of known hosts in case h5 index is corrupted (RARE!)", type=split_hostlist)
        args = parser.parse_args(remaining_args)
        return args

def get_h5_files(config):
    global procmon_h5cache
    if procmon_h5cache is None:
        procmon_h5cache = procmon.H5Cache.H5Cache(config.h5_path, config.h5_prefix)
    h5files = [ x['path'] for x in procmon_h5cache.query(config.start, config.end) ]
    baseline = [ x['path'] for x in procmon_h5cache.query(config.start - timedelta(minutes=20), config.start - timedelta(minutes=1)) ]
    return (baseline, h5files)

def get_hostlist(config, h5files):
    h5files = sorted(h5files)

    sum_records = {}
    for h5file in h5files:
        print "opening %s" % h5file
        fd = h5py.File(h5file, 'r')
        lhostnames = None
        try:
            lhostnames = fd.keys()
        except:
            if config.base_hostlist:
                lhostnames = config.base_hostlist
        if lhostnames is not None:
            for host in lhostnames:
                if host not in fd:
                    continue
                try:
                    hostgroup = fd[host]
                    nprocdata = hostgroup['procdata'].size
                    if host not in sum_records:
                        sum_records[host] = nprocdata
                    else:
                        sum_records[host] += nprocdata
                except:
                    sys.stderr.write('Unable to retrive data for %s; skipping\n' % host)

        fd.close()
    return sum_records
    
def main(args):
    config = ConfigParser(args[1:]).config
    if rank == 0:
        (baseline_h5files, analysis_h5files) = get_h5_files(config)
    print get_hostlist(config, analysis_h5files)

if __name__ == "__main__":
    main(sys.argv)
