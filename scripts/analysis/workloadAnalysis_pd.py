import os
import sys
import re
import argparse
from ConfigParser import SafeConfigParser
from datetime import date,datetime,timedelta,time
import pandas as pd

import h5py
import numpy as np
from mpi4py import MPI

import procmon

werenull = 0
cpu_sum = 0.
group_cpu_sum = 0.
global_rows = 0
found_groups = 0
groups_rows = 0

## TODO, add support for commandRemaps like (('command','bwa',),('cmdArgs',r'\S+\s+(\S+)\s*.*')) = ('bwa ',r'\1',)

procmonInstallBase = ''
procmon_h5cache = None
if 'PROCMON_DIR' in os.environ:
    procmonInstallBase = os.environ['PROCMON_DIR']

def generate_mpi_type_simple(np_dtype):
    offsets = [0]
    counts  = [np_dtype.itemsize]
    mpi_types = [MPI.BYTE]
    mpidt = MPI.Datatype.Create_struct(counts, offsets, mpi_types)
    mpidt_final = mpidt.Create_resized(0, np_dtype.itemsize).Commit()
    mpidt.Free()

    return mpidt_final

class Config:
    """ Read configuration from config file and command line. """

    class multidict(dict):
        """ Custom dict to let config have same named sections """
        _unique = 0

        def __setitem__(self, key, val):
            if isinstance(val, dict):
                self._unique += 1
                key += "_%d" % self._unique
            dict.__setitem__(self,key,val)

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

    def __add_summary(self, config, listargs):
        args = {k:v for (k,v) in listargs}
        add = {}
        if 'identifier' in args:
            add['identifier'] = args['identifier']
        else:
            raise ValueError('Identifier not specified for summary')
        if 'column' in args:
            add['column'] = args['column']
        else:
            raise ValueError('Column not specified for summary')
        if 'bins' in args:
            t = args['bins'].split(',')
            bins = []
            for bin_val in t:
                item = eval(bin_val.strip())
                bins.append(item)
            add['bins'] = bins
        else:
            raise ValueError('Bins not specified for summary')
        if not hasattr(config, 'summary'):
            setattr(config, 'summary', {})
        config.summary[add['identifier']] = add
        if not hasattr(config, 'summaries'):
            setattr(config, 'summaries', [])
        config.summaries.append(add)

    def __add_dimension(self, config, listargs):
        args = {k:v for (k,v) in listargs}
        add = {}
        if 'name' in args:
            add['name'] = args['name']
        else:
            raise ValueError('must specify name for a dimension')
        if 'column' in args:
            add['column'] = args['column']
        if 'type' in args:
            add['type'] = args['type']
        else:
            add['type'] = '|S48'

        if 'value' in args:
            value = eval(args['value'])
            newvalue = {}
            for key in value:
                v = value[key]
                if isinstance(v, list):
                    for x in v:
                        newvalue[re.compile(x)] = key
                else:
                    newvalue[re.compile(x)] = key
            add['value'] = newvalue

        if not hasattr(config, 'dimensions'):
            setattr(config, 'dimensions', [])
        config.dimensions.append(add)

    def __add_generalization(self, config, listargs):
        add = {
            'other_category': False,
            'ignore_category': False,
        }
        def_eval_order = []

        def add_category(name, regexes):
            if 'categories' not in add:
                add['categories'] = {}
            add['categories'][name] = regexes
            def_eval_order.append(name)

        def add_csv_categories(filename, transpose=False):
            category_data = np.loadtxt(v, str, delimiter=',')
            if transpose:
                category_data = np.transpose(category_data)
            for cat in category_data[:]:
                cat = filter(lambda x: len(x) > 0, cat)
                if len(cat) > 1:
                    add_category(cat[0], map(lambda x: '^'+x.strip()+'$', sorted(cat[1:])))

        for (k,v) in listargs:
            if k == "name":
                add[k] = v
            elif k == "column":
                add[k] = v
            elif k == "other_category" or k == "ignore_category":
                add[k] = bool(v)
            elif k == "eval_order":
                add[k] = [ v_.strip() for v_ in v.split(",") ]
            elif k == "csvFileTranspose":
                add_csv_categories(v, True)
            elif k == "csvFile":
                add_csv_categories(v, False)
            else:
                regexes = [ "^" + v_.strip() + "$" for v_ in v.split(",") ]
                add_category(k, regexes)

        if 'eval_order' in add:
            for value in def_eval_order:
                if value not in add['eval_order']:
                    add['eval_order'].append(value)
        else:
            add['eval_order'] = def_eval_order
        if not hasattr(config, 'generalizations'):
            config.generalizations = []
        config.generalizations.append(add)

    def __add_analysis(self, config, listargs):
        add = { 'axes': [] }
        for (k,v) in listargs:
            if k == 'name':
                add[k] = v
            if k == "axes":
                add['axisNames'] = [ x.strip() for x in v.split(",") ]
            if k.startswith('axis'):
                match = re.match('axis(\d+)', k)
                if match is None:
                    continue
                pos = int(match.groups()[0])
                while len(add['axes']) <= pos:
                    add['axes'].append(None)
                add['axes'][pos] = [ v_.strip() for v_ in v.split(',') ]
        if not hasattr(config, 'analyses'):
            config.analyses = []
        config.analyses.append(add)

    def __add_commandRemap(self, config, listargs):
        add = []
        for (k,v) in listargs:
            col,search = eval(k)
            replace = eval(v)
            add.append( (col,re.compile(search),replace,) )
        if not hasattr(config, 'commandRemap'):
            config.commandRemap = []
        config.commandRemap.extend(add) 

    def read_configuration(self, args):
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
            config = SafeConfigParser(None, Config.multidict)
            config.optionxform = str  # make options case sensitive
            config.read([args.config])
            for section in config.sections():
                if section.startswith("summary"):
                    self.__add_summary(args, config.items(section))
                if section.startswith("dimension"):
                    self.__add_dimension(args, config.items(section))
                if section.startswith("generalization"):
                    self.__add_generalization(args, config.items(section))
                if section.startswith("workloadAnalysis"):
                    self.__add_analysis(args, config.items(section))
                if section.startswith("commandRemap"):
                    self.__add_commandRemap(args, config.items(section))

        parser = argparse.ArgumentParser(parents=[parser])
        parser.set_defaults(**defaults)
        parser.add_argument('-o','--output', type=str, help="Specify output summary filename", default='output.h5')
        parser.add_argument('-s','--summary', type=str, help="Summary dataset to merge", default="useful")
        parser.add_argument('files', metavar='N', type=str, nargs='+', help='Processes h5 files to summarize')
        args = parser.parse_args(remaining_args, args)
        return args



def choose_processes(rank, size, count):
    count_per_rank = count / size
    start = rank*count_per_rank
    end = (rank+1)*count_per_rank
    if (rank == size-1):
        end == count
    return (start,end)

def summarizeH5(filename, config):
    global werenull
    global cpu_sum
    global group_cpu_sum
    global global_rows
    global found_groups
    global groups_rows

    comm = MPI.COMM_WORLD
    mpi_rank = comm.Get_rank()
    mpi_size = comm.Get_size()

    dset = None
    try:
        fd = h5py.File(filename, 'r')
        dset = fd['processes']
        dset_count = dset.len()
    except Exception, e:
        sys.stderr.write('Failed to read h5 file: %s, Exiting.' % filename)
        raise e
    if dset is None:
        return None

    (start,end) = choose_processes(mpi_rank, mpi_size, dset_count)
    print "[%d] start,end: %d,%d; TOTAL: %d" % (mpi_rank, start,end, dset_count)
    base_idx = start

    useful_summary = None
    while base_idx < end:
        limit = base_idx + 10000
        limit = min(limit, end)
        data = dset[base_idx:limit]
        print "[%d] read %d:%d of %d" % (mpi_rank, base_idx, limit, end)
        base_idx = limit

        cpu_nan = np.isnan(data['cputime_net'])
        if np.sum(cpu_nan) > 0:
            print "cpu_nan", data[cpu_nan]

        data = data[np.invert(cpu_nan)]

        ## remap any commands:
        for cmdR in config.commandRemap:
            column, search, replace = cmdR
            searchfxn = np.vectorize(lambda x: search.match(x) is not None)
            mask = searchfxn(data[column])
            if np.sum(mask) > 0:
                searchrep = np.vectorize(lambda x: search.sub(replace, x))
                data['command'][mask] = searchrep(data[column][mask])

        #mask = data['command'] == "pb"
        #if np.sum(mask) > 0:
        #    print data['exePath'][mask]
        #    print data['cmdArgs'][mask]
        
        ancestors = data['isParent'] == 1
        highVol   = np.greater_equal(data['volatilityScore'], 0.1)
        highCpu   = np.greater_equal(data['cputime_net'], data['duration']*0.5)
        useful = highVol | ~ancestors | highCpu

        data = data[useful]

        global_rows += data.size

        ## if there are any negative durations, fix those
        negDuration = data['duration'] < 0.
        if np.sum(negDuration) > 0:
            data['duration'][negDuration] = 0. ## this simply isn't true, and is an artifact of procmon scanning

        for dim in config.dimensions:
            nanmask = data[dim['column']] == "nan"
            if np.sum(nanmask) > 0:
                print "null %s: " % dim['column'], np.sum(nanmask)
                data[dim['column']][nanmask] = "Unknown"

        cpu_sum += np.sum(data['cputime_net'])


        l_useful_summary = summarizeData_pd(data, config)

        if useful_summary is None:
            useful_summary = l_useful_summary
        else:
            useful_summary = l_useful_summary.combineAdd(useful_summary)
        print "Summary shape: ", useful_summary.shape

    #print "about to convert summary to numpy"
    #useful_summary = convert_np(useful_summary.reset_index(), config)
    #print "done.  about to write output %s" % config.output
    print "found %d null values (set to zero!)" % werenull
    print "cpu_sum %f" % cpu_sum
    print "group_cpu_sum %f" % group_cpu_sum
    print "total rows: ", global_rows
    print "group rows: ", groups_rows
    print "groups: ", found_groups
    useful_summary.to_hdf(config.output, 'useful')
    #output = h5py.File(config.output, 'w')
    #dset = output.create_dataset('useful', (useful_summary.size,), dtype=useful_summary.dtype)
    #dset[:] = useful_summary[:]
    #output.close()

def convert_np(summary, config):
    types = []
    for dim in config.dimensions:
        types.append((dim['column'], dim['type'],))
    for s in config.summaries:
        types.append(('%s_count'%s['identifier'], np.uint64,))
        types.append(('%s_sum'%s['identifier'], np.float64,))
        types.append(('%s_histogram'%s['identifier'], '%di8' % (len(s['bins'])-1),))
    mtype = np.dtype(types)
    data = np.zeros(shape=summary.shape[0], dtype=mtype)
    for dim in config.dimensions:
        data[dim['column']] = summary[dim['column']]
    for s in config.summaries:
        count = '%s_count' % s['identifier']
        sumcol = '%s_sum' % s['identifier']
        histogram = '%s_histogram' % s['identifier']
        data[count] = summary[count]
        data[sumcol] = summary[sumcol]
        for idx,hist in enumerate(summary[histogram]):
            data[histogram][idx] = np.array(hist, 'i8')
    return data

def summaryFunc(data, config):
    global werenull
    global group_cpu_sum
    global groups_rows
    ret = {}
    groups_rows += data.shape[0]
    for s in config.summaries:
        empty = pd.isnull(data[s['column']])
        if np.sum(empty) > 0:
            data[s['column']][empty] = 0
            werenull += np.sum(empty)
        hist = np.histogram(np.array(data[s['column']]), bins=s['bins'])
        sum = data[s['column']].sum()
        count = np.sum(np.invert(pd.isnull(data[s['column']])))
        ret['%s_count'%s['identifier']] = count
        ret['%s_sum'%s['identifier']] = sum
        for idx in xrange(len(s['bins'])-1):
            ret['%s_%d_hist' % (s['identifier'], idx)] = hist[0][idx]
    group_cpu_sum += np.sum(data['cputime_net'])
    return pd.Series(ret)


def summarizeData_pd(data, config):
    global found_groups
    data = pd.DataFrame(data)
    groups = data.groupby([dim['column'] for dim in config.dimensions])
    found_groups += len(groups.groups)
    summary = groups.apply(summaryFunc, config)
    return summary


def main(args):
    config = Config(args[1:]).config
    for fname in config.files:
        summaries = summarizeH5(fname, config)

if __name__ == "__main__":
    main(sys.argv)
