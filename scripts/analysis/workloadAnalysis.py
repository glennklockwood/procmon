import os
import sys
import re
import argparse
from ConfigParser import SafeConfigParser
from datetime import date,datetime,timedelta,time

import h5py
import numpy as np
from mpi4py import MPI

import procmon

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
            config.read([args.config])
            for section in config.sections():
                if section.startswith("summary"):
                    self.__add_summary(args, config.items(section))
                if section.startswith("dimension"):
                    self.__add_dimension(args, config.items(section))

        parser = argparse.ArgumentParser(parents=[parser])
        parser.set_defaults(**defaults)
        parser.add_argument('-o','--output', type=str, help="Specify output summary filename", default='output.h5')
        parser.add_argument('files', metavar='N', type=str, nargs='+', help='Processes h5 files to summarize')
        args = parser.parse_args(remaining_args, args)
        return args

class Summaries:
    def __init__(self, config, group, dimensions):
        self.summary = []
        self.dimensions = dimensions
        self.block_size = 2048
        self.blocks = []
        self.block_counts = []
        self.result_map = {}
        self.__combined = False

        for c in config.summary:
            local = {k:config.summary[c][k] for k in config.summary[c]}
            self.summary.append(local)

        self.__add_block()
        self.__curr_block = 0

        self.group = group

    def __add_block(self):
        add = []
        for (idx,a) in enumerate(self.summary):
            type_list = [
                ('type', '|S48',),
                ('sum', np.float64),
                ('count', np.int64),
                ('histogram','%dint64' % (len(a['bins'])-1))
            ]
            for dim in self.dimensions:
                type_list.append((dim['name'], dim['type']))
            array = np.zeros(shape=self.block_size, dtype=np.dtype(type_list))
            add.append(array)
        self.blocks.append(add)
        self.block_counts.append(0)

    def add_data(self, dimensions, data):
        loc = None
        if self.__combined:
            raise ValueError("Cannot add data after Summaries data are combined.")
        if data.size == 0:
            return
        ident = 'X'.join(map(str,dimensions))
        if ident not in self.result_map:
            if self.block_counts[self.__curr_block] == self.block_size:
                self.__add_block()
                self.__curr_block += 1
            loc = (self.__curr_block, self.block_counts[self.__curr_block])
            for c in xrange(len(self.summary)):
                self.blocks[loc[0]][c][loc[1]]['type'] = self.group
                for (d_idx,dim) in enumerate(self.dimensions):
                    self.blocks[loc[0]][c][loc[1]][dim['name']] = dimensions[d_idx]
            self.result_map[ident] = loc
            self.block_counts[self.__curr_block] += 1
        else:
            loc = self.result_map[ident]

        for (idx,c) in enumerate(self.summary):
            mask = data[c['column']] < 0
            data[c['column']][mask] = 0
            d = np.histogram(data[c['column']], bins=c['bins'])
            self.blocks[loc[0]][idx][loc[1]]['histogram'] += d[0]
            self.blocks[loc[0]][idx][loc[1]]['sum'] += np.nansum(data[c['column']])
            self.blocks[loc[0]][idx][loc[1]]['count'] += np.sum(np.invert(np.isnan(data[c['column']])))

    def combine_data(self):
        if self.__combined:
            raise ValueError("Data are already combined.")
        alldata = [[] for s in self.summary]
        for (s_idx,s) in enumerate(self.summary):
            for (b_idx,b) in enumerate(self.blocks):
                alldata[s_idx].append(b[s_idx][0:self.block_counts[b_idx]])
        for s_idx in xrange(len(alldata)):
            alldata[s_idx] = np.concatenate(alldata[s_idx])
        del self.blocks
        self.blocks = None
        self.block_counts = None
        self.__combined = True
        self.combined = alldata

    def __mpi_transfer_dataset(self, comm, s_idx, mpi_rank, mpi_size):
        allsizes = comm.allgather(self.combined[s_idx].size)
        mpi_dtype = generate_mpi_type_simple(self.combined[s_idx].dtype)
        alldata = np.zeros(shape=sum(allsizes), dtype=self.combined[s_idx].dtype)
        counts = np.array(allsizes)
        offsets = np.hstack((0,np.cumsum(counts)))[0:-1]
        comm.Allgatherv(
            [self.combined[s_idx], self.combined[s_idx].size, mpi_dtype],
            [alldata, counts, offsets, mpi_dtype]
        )
        alldata.sort(order=['identifier','subidentifier'])
        ids = np.unique(alldata[['identifier','subidentifier']])
        ids.sort(order=['identifier','subidentifier'])

        (start,end) = choose_processes(mpi_rank, mpi_size, ids.size)
        finaldata = np.zeros(shape=(end-start),dtype=self.combined[s_idx].dtype)

        for pos in xrange(start,end):
            mask = alldata[['identifier','subidentifier']] == ids[pos]
            typecol = alldata[mask]['type'][0]
            sumcol = np.sum(alldata[mask]['sum'])
            countcol = np.sum(alldata[mask]['count'])
            histcol = np.sum(alldata[mask]['histogram'], axis=0)
            finaldata[pos-start]['identifier'] = ids[pos]['identifier']
            finaldata[pos-start]['subidentifier'] = ids[pos]['subidentifier']
            finaldata[pos-start]['type'] = typecol
            finaldata[pos-start]['sum'] = sumcol
            finaldata[pos-start]['count'] = countcol
            finaldata[pos-start]['histogram'] += histcol
        self.combined[s_idx] = finaldata


    def mpi_transfer_datasets(self, comm):
        if not self.__combined:
            raise ValueError('cannot transfer uncombined data')
        mpi_size = comm.Get_size()
        mpi_rank = comm.Get_rank()

        for s_idx in xrange(len(self.summary)):
            self.__mpi_transfer_dataset(comm, s_idx, mpi_rank, mpi_size)

    def write_data(self, comm, h5_group):
        mpi_size = comm.Get_size()
        mpi_rank = comm.Get_rank()
        for s_idx in xrange(len(self.summary)):
            allsizes = comm.allgather(self.combined[s_idx].size)
            total_records = sum(allsizes)
            write_indices = np.hstack((0,np.cumsum(allsizes)))[0:-1]
            dset = h5_group.create_dataset(
                self.summary[s_idx]['identifier'],
                (total_records,),
                dtype=self.combined[s_idx].dtype
            )
            base = write_indices[mpi_rank]
            limit = base + allsizes[mpi_rank]
            dset[base:limit] = self.combined[s_idx][:]
            dset.attrs['bins'] = np.array(self.summary[s_idx]['bins'])
            dset.attrs['column'] = self.summary[s_idx]['column']

def choose_processes(rank, size, count):
    count_per_rank = count / size
    start = rank*count_per_rank
    end = (rank+1)*count_per_rank
    if (rank == size-1):
        end == count
    return (start,end)

def summarizeH5(filename, config):
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
    summaries = Summaries(config, "useful", config.dimensions)

    all_commands = None
    while base_idx < end:
        limit = base_idx + 10000
        limit = min(limit, end)
        data = dset[base_idx:limit]
        print "[%d] read %d:%d of %d" % (mpi_rank, base_idx, limit, end)
        base_idx = limit
        
        ancestors = data['isParent'] == 1
        highVol   = np.greater_equal(data['volatilityScore'], 0.1)
        highCpu   = np.greater_equal(data['cputime_net'], data['duration']*0.5)
        useful = highVol | ~ancestors | highCpu
        summarizeData(data[useful], summaries, config)
        #summarizeData(data[~useful], summaries['nonuseful'], config)

    fd.close()

    output = h5py.File(config.output, 'w', driver='mpio',comm=comm)

    for key in summaries:
        sGroup = output.create_group(key)
        for summ in summaries[key]:
            summary = summaries[key][summ]
            summary.combine_data()
            summary.mpi_transfer_datasets(comm)
            group = sGroup.create_group(summary.group)
            summary.write_data(comm, group)

    output.close()

def summarizeData(data, summaries, config):
    dim_items = []
    for dim in config.dimensions:
        dimmap = {}
        unique = np.unique(data[dim['column']])
        for item in unique:
            mask = data[dim['column']] == item
            if 'value' in dim:
                found = False
                for reg in dim['value']:
                    if reg.match(item):
                        key = dim['value'][reg]
                        if key in dimmap:
                            dimmap[key] |= mask
                        else:
                            dimmap[key] = mask
                        found = True
                        break
                if not found:
                    if 'Other' in dimmap:
                        dimmap['Other'] |= mask
                    else:
                        dimmap['Other'] = mask
            else:
                dimmap[item] = mask
        dim_items.append(dimmap)

    def runDimensions(data, d_idx, mask, keys):
        dimmap = dim_items[d_idx]
        for key in dimmap:
            local_mask = mask & dimmap[key]
            keys.append(key)
            if d_idx == len(config.dimensions)-1:             
                if np.sum(local_mask) > 0:
                    summaries.add_data(keys, data[local_mask])
            else:
                runDimensions(data, d_idx+1, local_mask, keys)
            keys.pop()

    runDimensions(data, 0, np.ones(data.size, dtype=bool), [])
    return

"""
     users = np.unique(data['username'])
     projects = np.unique(data['project'])
     hosts = np.unique(data['host'])
     for command in commands:
         command_mask = data['command'] == command
         commandSummaries.add_data(command, "raw", data[command_mask])

         for user in users:
             user_mask = data['username'] == user
             mask = command_mask & user_mask
             if np.sum(mask) > 0:
                 userSummaries.add_data(user, command, data[mask])

         for project in projects:
             project_mask = data['project'] == project
             mask = command_mask & project_mask
             if np.sum(mask) > 0:
                 projectSummaries.add_data(project, command, data[mask])

         for host in hosts:
             host_mask = data['host'] == host
             mask = command_mask & host_mask
             if np.sum(mask) > 0:
                 hostSummaries.add_data(host, command, data[mask])   
"""

def main(args):
    config = Config(args[1:]).config
    for fname in config.files:
        summaries = summarizeH5(fname, config)

if __name__ == "__main__":
    main(sys.argv)
