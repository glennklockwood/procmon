"""

"""
import h5py
import numpy as np

class RawH5Parser:
    def __init__(self):
        self.base_hostlist = []
        self.read_datasets = ['procdata','procstat','procfd','procobs']

        ## fields filled out by the parse() function
        self.filenames = []
        self.hosts = []
        self.datasets = {}
        self.host_offsets = {}
        self.host_counts  = {}
        self.dset_types   = {}

    def __repr__(self):
        return "RawH5Parser:ReadFiles_%d;Hosts_%d" % (len(self.filenames), len(self.hosts))

    def __get_h5_host_counts(self, filename, host_counts, ref_hosts = None):
        hostnames = []
        fd = h5py.File(filename, 'r')
        try:
            hostnames = fd.keys()
        except:
            sys.stderr.write("Couldn't get hostnames from %s, problem with "
                "file?; falling back on hard-coded host list." % filename)
            hostnames = self.base_hostlist

        for host in hostnames:
            if host not in fd:
                continue
            if ref_hosts is not None and host not in ref_hosts:
                continue

            hostgroup = fd[host]
            if host not in host_counts:
                host_counts[host] = {x:0 for x in self.read_datasets}

            for dset in self.read_datasets:
                if dset not in hostgroup:
                    continue
                nRec = hostgroup[dset].attrs['nRecords']
                host_counts[host][dset] += nRec #hostgroup[dset].len()
                
        fd.close()

    def __detectParents(self, data): 
        parentPids = []
        if data.size > 0:
            parentPids = np.unique(np.intersect1d(data['pid',data['ppid']]))
        isParent = np.zeros(shape=data.size, dtype=np.int32)
        for pid in parentPids:
            mask = data['pid'] == pid
            isParent[mask] = 1
        return isParent

    def parse(self, filenames, ref_hosts = None):
        """Read all the data from the input h5 files."""

        ## step 1: get the list of all the hosts and how many records of each
        ##  dataset are stored across all the files for which we are responsible
        host_counts = {}
        for filename in filenames:
            self.__get_h5_host_counts(filename, host_counts, ref_hosts)

        ## step 2: for each dataset, allocate sufficient space and start reading
        ##  the data
        self.datasets = {}
        self.dset_types = {}
        self.hosts = sorted(host_counts.keys())
        self.host_offsets = {}
        self.host_counts  = {}

        for dset in self.read_datasets:
            self.host_counts[dset] = np.zeros(shape=len(self.hosts), dtype=np.uint64)
            self.host_offsets[dset] = np.zeros(shape=len(self.hosts), dtype=np.uint64)
            self.host_offsets[dset][:] = np.hstack(
                (0,np.cumsum([host_counts[x][dset] for x in self.hosts])[0:-1])
            )

        for filename in filenames:
            self.filenames.append(filename)

            fd = h5py.File(filename, 'r')
            for (h_idx,host) in enumerate(self.hosts):
                if host not in fd:
                    continue
                hostgroup = fd[host]

                for dset in self.read_datasets:
                    # starting position to write is the offset, plus however much has been written for this host
                    offset = self.host_offsets[dset][h_idx]
                    offset += self.host_counts[dset][h_idx]

                    if dset not in hostgroup or hostgroup[dset].len() == 0:
                        continue

                    if dset not in self.datasets:
                        if dset not in self.dset_types:
                            ## get type by reading first entry for dset we can find
                            ltype = hostgroup[dset][0].dtype
                            newtype = sorted([ (x,ltype.fields[x][0]) for x in ltype.fields ], key=lambda y: ltype.fields[y[0]][1])
                            newtype.append( ('host', '|S36', ) )
                            newtype.append( ('sortidx', np.uint32,) )
                            if dset == "procdata":
                                newtype.append(('isParent', np.int32))
                            self.dset_types[dset] = np.dtype(newtype)

                        total = sum([host_counts[x][dset] for x in host_counts])
                        self.datasets[dset] = np.zeros(total, dtype=self.dset_types[dset])

                    nRec = hostgroup[dset].attrs['nRecords']
                    limit = offset + nRec
                    ldata = fd[host][dset][0:nRec]
                    ltype = ldata.dtype
                    if dset == "procdata":
                        self.datasets[dset][offset:limit]['isParent'] = self.__detectParents(ldata)
                    for col in ltype.names:
                        self.datasets[dset][offset:limit][col] = ldata[col]
                    self.datasets[dset][offset:limit]['host'] = host
                    self.host_counts[dset][h_idx] += nRec
            fd.close()

    def get(host, dataset):
        if dataset not in self.dset_types:
            raise ValueError("%s is unknown datset identifier" % dataset)

        h_idx = self.hosts.index(host)
        if h_idx < 0:
            return np.empty(shape=0, dtype=self.dset_types[dataset])

    def free(self, dset):
        if dset in self.datasets and self.datasets[dset] is not None:
            del self.datasets[dset]

    def count_processes(self, host):
        h_idx = self.hosts.index(host)
        if h_idx < 0:
            return 0

        host_offset = self.host_offsets['procdata'][h_idx]
        host_count  = self.host_counts['procdata'][h_idx]

        if host_count == 0:
            return 0

        limit = host_offset + host_count
        unique_processes = np.unique(self.datasets['procdata'][host_offset:limit][['pid','startTime']])
        return unique_processes.size