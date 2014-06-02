import os
import sys
import re
import argparse
from ConfigParser import SafeConfigParser
from datetime import date,datetime,timedelta,time
import re

import h5py
import numpy as np
from mpi4py import MPI

import procmon
import procmon.Scriptable

werenull = 0
cpu_sum = 0.
selected_cpu_sum = 0.
group_cpu_sum = 0.
global_rows = 0
selected_rows = 0
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
        if 'units' in args:
            add['units'] = args['units']
        else:
            add['units'] = ''
            
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

    def __prepare_summary_dtype(self, config):
        dtype_list = []
        for dim in config.dimensions:
            dtype_list.append((dim['name'], dim['type'],))
        for s in config.summaries:
            name = s['identifier']
            dtype_list.append(('%s_count' % name,np.uint64,))
            dtype_list.append(('%s_sum' % name, np.float64))
            dtype_list.append(('%s_histogram' % name, '%du8' % (len(s['bins']) - 1)))
        config.summaryDtype = np.dtype(dtype_list)

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

        self.__prepare_summary_dtype(args)

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
    if rank == (size-1):
        end = count
    print "[%d] choose_processes: size: %d, count: %d, start: %d, end: %d" % (rank, size, count, start, end)
    return (start,end)

def sumSummaries(data):
    item = np.zeros(1, dtype=data.dtype)
    for col in data.dtype.names:
        if col.endswith('_count') or col.endswith('_sum'):
            item[col] = np.sum(data[col])
        elif col.endswith('_histogram'):
            item[col] = np.sum(data[col], axis=0)
    return item

def mergeSummaries(a, b, config):
    columns = [dim['name'] for dim in config.dimensions]
    data = np.concatenate([a,b])
    return mergeDataset(data, columns)

def mergeDataset(data, keys):
    data.sort(order=keys)
    groups = np.unique(data[keys])
    ret = np.zeros(groups.size, dtype=data.dtype)
    startIdx = 0
    endIdx = 0
    retIdx = 0
    lastItem = None
    while endIdx < data.size:
        if data[keys][endIdx] != lastItem:
            if endIdx != 0:
                item = data[startIdx]
                if endIdx-startIdx > 1:
                    item = sumSummaries(data[startIdx:endIdx])
                    for c in keys:
                        item[c] = data[c][startIdx]
                ret[retIdx] = item
                retIdx += 1
            lastItem = data[keys][endIdx]
            startIdx = endIdx
        endIdx += 1
    item = data[startIdx]
    if endIdx - startIdx > 1:
        item = sumSummaries(data[startIdx:endIdx])
        for c in keys:
            item[c] = data[c][startIdx]
    ret[retIdx] = item
    retIdx += 1
    return ret

def mpiMergeDataset(data, keys, comm, mpi_rank, mpi_size):
    ## the goal here is to have each rank merge a segment of the data
    ## then transmit that data globally
    data.sort(order=keys)  ## assume all ranks have the same data
    groups = np.unique(data[keys])
    (gStart,gEnd) = choose_processes(mpi_rank, mpi_size, groups.size)
    sMask = data[keys] == groups[gStart]
    dataStart = np.min(np.nonzero(sMask)[0])
    dataEnd = data.size
    if gEnd < groups.size:
        eMask = data[keys] == groups[gEnd]
        dataEnd = np.min(np.nonzero(eMask)[0])
    print "[%d] mpiMerge myRange: %d, %d, %d; %d, %d, %d" % (mpi_rank, dataStart, dataEnd, data.size, gStart, gEnd, groups.size)
    myData = mergeDataset(data[dataStart:dataEnd], keys)
    allsizes = comm.allgather(myData.size)
    counts = np.array(allsizes)
    offsets = np.hstack((0, np.cumsum(counts)))[0:-1]
    alldata = np.zeros(np.sum(allsizes), dtype=data.dtype)
    mpi_dtype = generate_mpi_type_simple(myData.dtype)
    comm.Allgatherv(
        [myData, myData.size, mpi_dtype],
        [alldata, counts, offsets, mpi_dtype]
    )
    return alldata


def mpiMergeSummaries(summary, config, comm, mpi_rank, mpi_size):
    print "[%d] Starting mpi merge with %d items" % (mpi_rank, summary.size)
    columns = [dim['name'] for dim in config.dimensions]
    allsizes = comm.allgather(summary.size)
    mpi_dtype = generate_mpi_type_simple(summary.dtype)
    alldata = np.zeros(np.sum(allsizes), dtype=summary.dtype)
    counts = np.array(allsizes)
    offsets = np.hstack((0, np.cumsum(counts)))[0:-1]
    comm.Allgatherv(
        [summary, summary.size, mpi_dtype],
        [alldata, counts, offsets, mpi_dtype]
    )
    print "[%d] received data, starting sort" % mpi_rank
    alldata.sort(order=columns)
    groups = np.unique(alldata[columns])
    print "[%d] have %d groups, starting masking and selection" % (mpi_rank, groups.size)
    (gStart,gEnd) = choose_processes(mpi_rank, mpi_size, groups.size)
    sMask = alldata[columns] == groups[gStart]
    dataStart = np.min(np.nonzero(sMask)[0])
    dataEnd = alldata.size
    if gEnd < groups.size:
        eMask = alldata[columns] == groups[gEnd]
        dataEnd = np.min(np.nonzero(eMask)[0])
    print "[%d] finished masking, starting merge" % (mpi_rank)
    myData = mergeSummaries(alldata[dataStart:dataEnd], np.empty(0, dtype=summary.dtype), config)
    print "[%d] merge complete" % mpi_rank
    return myData

def identify_scripts(processes):
    """For known scripting interpreter processes, work out what the executed script was."""
    executables = np.unique(processes['exePath'])
    scripts = np.zeros(processes.size, 'S1024')

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
                scripts[idx] = script
    processes['script'] = scripts
    return scripts

def identify_userCommand(processes, scripts):
    """ Use a simple heuristic to identify the intended command for each process
        depends on indentify_scripts already having been run """

    execCommand = np.zeros(processes.size, 'S1024')
    command = np.zeros(processes.size, 'S1024')
    getExecCommand = np.vectorize(lambda x: x.split('/')[-1])
    execCommand = getExecCommand(processes['exePath'])
    command[:] = getExecCommand(scripts)
    mask = (command == "COMMAND") | (command == "")
    command[mask] = execCommand[mask]
    processes['command'] = command
    processes['execCommand'] = execCommand
    return (command,execCommand)

def fixType(rawdata):
    dtype_list = []
    for key in rawdata.dtype.fields:
        if key in ('script','execCommand','command'):
            dtype_list.append( (key, 'S1024',) )
        else:
            dtype_list.append( (key, rawdata.dtype.fields[key][0],) )
    data = np.zeros(rawdata.size, dtype=np.dtype(dtype_list))
    for col in rawdata.dtype.names:
        data[col] = rawdata[col]
    return data

def summarizeH5(filename, config):
    global werenull
    global cpu_sum
    global selected_cpu_sum
    global group_cpu_sum
    global global_rows
    global selected_rows
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
    nonuseful_summary = None
    useful_summary_padded = None
    while base_idx < end:
        limit = base_idx + 10000
        limit = min(limit, end)
        data = dset[base_idx:limit]
        print "[%d] read %d:%d of %d" % (mpi_rank, base_idx, limit, end)
        base_idx = limit

        ## repair some defects in the current system...
        data['cputime_net'] = (data['utime_net'] + data['stime_net'])/100.
        scripts = identify_scripts(data)
        (command,execCommand) = identify_userCommand(data, scripts)

        ## remap any commands:
        for cmdR in config.commandRemap:
            column, search, replace = cmdR
            searchfxn = np.vectorize(lambda x: search.match(x) is not None)
            mask = None
            if column == "script":
                mask = searchfxn(scripts)
            elif column == "command":
                mask = searchfxn(command)
            elif column == "execCommand":
                mask = searchfxn(execCommand)
            else:
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
        reallyHighCpu = np.greater_equal(data['cputime_net'], 86400*16) ## just ignore single process that used more than 1 day of cpu
        useful = highVol | ~ancestors | highCpu
        if np.sum(reallyHighCpu) > 0:
            print "[%d] FOUND %d really high cpu records." % (mpi_rank, np.sum(reallyHighCpu))
            useful &= ~reallyHighCpu ## screen out the really high cpu records
        reallyHighDuration = np.greater_equal(data['duration'], 86700)
        if np.sum(reallyHighDuration) > 0:
            print "[%d] FOUND %d really high duration records. " % (mpi_rank, np.sum(reallyHighDuration))
            useful &= ~reallyHighDuration

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
        selected_cpu_sum += np.sum(data[useful]['cputime_net'])
        selected_rows += np.sum(useful)

        l_useful_summary = summarizeData(data[useful], config)
        if useful_summary is None:
            useful_summary = l_useful_summary
        else:
            useful_summary = mergeSummaries(useful_summary, l_useful_summary, config)
        print "[%d] useful summary shape: " % mpi_rank, useful_summary.shape
        continue

        l_nonuseful_summary = summarizeData(data[np.invert(useful)], config)
        if nonuseful_summary is None:
            nonuseful_summary = l_nonuseful_summary
        else:
            nonuseful_summary = mergeSummaries(nonuseful_summary, l_nonuseful_summary, config)
        print "[%d] nonuseful summary shape: " % mpi_rank, nonuseful_summary.shape
        if nonuseful_summary.size == 0:
            nonuseful_summary = None

        old_duration = data['duration'].copy()
        zerodur_mask = old_duration == 0.
        temp_duration = old_duration.copy()
        old_cputime = data['cputime_net'].copy()
        temp_duration[zerodur_mask] = 1.0
        # pad duration to try to account for procmon random sampling
        data['duration'] += np.random.uniform(low = 0.0, high = 30.0, size = data.size)

        # adjust a few of the rate counters based on average usage
        data['cputime_net'] += (data['cputime_net']/temp_duration)*(data['duration'] - old_duration)
        data['cputime_net'][zerodur_mask] = old_cputime[zerodur_mask]

        l_useful_summary_padded = summarizeData(data[useful], config)
        if useful_summary_padded is None:
            useful_summary_padded = l_useful_summary_padded
        else:
            useful_summary_padded = mergeSummaries(useful_summary_padded, l_useful_summary_padded, config)
        print "[%d] useful padded summary shape: " % mpi_rank, useful_summary_padded.shape

    #print "about to convert summary to numpy"
    #useful_summary = convert_np(useful_summary.reset_index(), config)
    #print "done.  about to write output %s" % config.output
    print "found %d null values (set to zero!)" % werenull
    print "cpu_sum %f" % cpu_sum
    print "selected_cpu_sum %f" % selected_cpu_sum
    print "group_cpu_sum %f" % group_cpu_sum
    print "total rows: ", global_rows
    print "selected rows: ", selected_rows
    print "group rows: ", groups_rows
    print "groups: ", found_groups
    if useful_summary is not None:
        useful_summary = mpiMergeSummaries(useful_summary, config, comm, mpi_rank, mpi_size)
    if nonuseful_summary is not None:
        nonuseful_summary = mpiMergeSummaries(nonuseful_summary, config, comm, mpi_rank, mpi_size)
    if useful_summary_padded is not None:
        useful_summary_padded = mpiMergeSummaries(useful_summary_padded, config, comm, mpi_rank, mpi_size)

    output = h5py.File(config.output, 'w', driver='mpio', comm=comm)
    def writeSummary(name, summary, comm, mpi_rank, mpi_size):
        allsizes = comm.allgather(summary.size)
        dset = output.create_dataset(name, (np.sum(allsizes),), dtype=summary.dtype)
        offsets = np.hstack((0,np.cumsum(allsizes)))[0:-1]
        start = offsets[mpi_rank]
        end = start + summary.size
        dset[start:end] = summary[:]
    if useful_summary is not None:
        writeSummary('useful', useful_summary, comm, mpi_rank, mpi_size)
    if useful_summary_padded is not None:
        writeSummary('usefulpadded', useful_summary_padded, comm, mpi_rank, mpi_size)
    if nonuseful_summary is not None:
        writeSummary('nonuseful', nonuseful_summary, comm, mpi_rank, mpi_size)
    output.close()

def summaryFunc(data, config):
    global werenull
    global group_cpu_sum
    global groups_rows
    ret = np.zeros(1, dtype=config.summaryDtype)
    groups_rows += data.size
    for s in config.summaries:
        empty = np.isnan(data[s['column']])
        if np.sum(empty) > 0:
            data[s['column']][empty] = 0
            werenull += np.sum(empty)
        hist = np.histogram(np.array(data[s['column']]), bins=s['bins'])
        sum = np.nansum(data[s['column']])
        count = np.nansum(np.invert(np.isnan(data[s['column']])))
        ret['%s_count'%s['identifier']] = count
        ret['%s_sum'%s['identifier']] = sum
        ret['%s_histogram'%s['identifier']] = hist[0]
    group_cpu_sum += np.sum(data['cputime_net'])
    return ret


def summarizeData(data, config):
    global found_groups
    columns = [dim['column'] for dim in config.dimensions]
    scolumns = [dim['name'] for dim in config.dimensions]

    data.sort(order=columns)
    lastItem = None
    startIdx = 0
    endIdx = 0
    groupIdx = 0
    nGroups = np.unique(data[columns]).size
    summary = np.zeros(nGroups, dtype=config.summaryDtype)
    while endIdx < data.size:
        if data[columns][endIdx] != lastItem:
            if endIdx != 0:
                summary[groupIdx] = summaryFunc(data[startIdx:endIdx], config)
                for (cIdx,col) in enumerate(columns):
                    summary[scolumns[cIdx]][groupIdx] = data[col][startIdx]
                groupIdx += 1
            startIdx = endIdx
            lastItem = data[columns][endIdx]
        endIdx += 1
    if endIdx != startIdx:
        summary[groupIdx] = summaryFunc(data[startIdx:endIdx], config)
        for (cIdx,col) in enumerate(columns):
            summary[scolumns[cIdx]][groupIdx] = data[col][startIdx]
        groupIdx += 1

    return summary


def main(args):
    config = Config(args[1:]).config
    for fname in config.files:
        summaries = summarizeH5(fname, config)

if __name__ == "__main__":
    main(sys.argv)
