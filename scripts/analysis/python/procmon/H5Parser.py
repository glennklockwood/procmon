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
                host_counts[host][dset] += hostgroup[dset].len()
                
        fd.close()

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
                            self.dset_types[dset] = hostgroup[dset][0].dtype
                        
                        total = sum([host_counts[x][dset] for x in host_counts])
                        self.datasets[dset] = np.zeros(total, dtype=self.dset_types[dset])

                    nRec = hostgroup[dset].attrs['nRecords']
                    limit = offset + nRec

                    self.datasets[dset][offset:limit] = fd[host][dset][0:nRec]
                    self.host_counts[dset][h_idx] += nRec
            fd.close()

    def get(host, dataset):
        if dataset not in self.dset_types:
            raise ValueError("%s is unknown datset identifier" % dataset)

        h_idx = self.hosts.index(host)
        if h_idx < 0:
            return np.empty(shape=0, dtype=self.dset_types[dataset])

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