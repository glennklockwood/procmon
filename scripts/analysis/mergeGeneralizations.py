import numpy as np
import os
import sys
import workloadAnalysis_pd
import h5py
import re
from mpi4py import MPI

def main(args):
	config = workloadAnalysis_pd.Config(args).config
	comm = MPI.COMM_WORLD
	mpi_rank = comm.Get_rank()
	mpi_size = comm.Get_size()
	datasets = {}
	types = {}
	fds = []
	for fname in config.files:
		print "reading data from %s" % fname
		fd = h5py.File(fname, 'r')
		for dset in fd:
			data = fd[dset][:]
			if dset not in datasets:
				datasets[dset] = []
			if dset not in types:
				types[dset] = data.dtype
			if data.dtype != types[dset]:
				print "ERROR! FAIL! dtypes for %s do not match!" % dset, data.dtype, types[dset]
			datasets[dset].append(data)
		fd.close()

	fd = h5py.File(config.output, 'w')
	for dset in datasets:
		data = np.concatenate(datasets[dset])
		keys = []
		print data
		for col in data.dtype.names:
			if not col.endswith("_sum") and not col.endswith("_count") and not col.endswith("_histogram"):
				keys.append(col)
		print "merging %s on " % dset, keys
		data = workloadAnalysis_pd.mpiMergeDataset(data, keys, comm, mpi_rank, mpi_size)
		print data
		outdata = fd.create_dataset(dset, (data.size,), dtype=data.dtype)
		outdata[:] = data[:]

	fd.close()

if __name__ == "__main__":
	main(sys.argv[1:])



