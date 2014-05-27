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

	print config

	dataset = []
	for filename in config.files:
		fd = h5py.File(filename, 'r')
		dset = fd['useful'][:]
		dataset.append(dset)
		fd.close()
	dataset = np.concatenate(dataset)

	login_mask = np.zeros(dataset.size, dtype=bool)
	gens = {}
	## perform any needed generalizations
	for gen in config.generalizations:
		print "Starting on generalization: " + gen['name']
		col = 'generalization_%s' % gen['name']
		gens[col] = dataset[gen['column']].copy()

		overall_mask = np.zeros(dataset.size, dtype=bool)
		for catname in gen['eval_order']:
			regexes = gen['categories'][catname]
			mask = np.zeros(dataset.size, dtype=bool)
			sys.stdout.write("starting on category:%s; " % catname)
			for regex in regexes:
				reg = re.compile(regex)
				regMatch = np.vectorize(lambda x: bool(reg.match(x)))
				if col in gens:
					mask |= regMatch(gens[col])
				else:
					mask |= regMatch(dataset[col])
				sys.stdout.write("%d " % np.sum(mask))
			overall_mask |= mask
			print "; (%d)" % np.sum(overall_mask)
			gens[col][mask] = "category_%s" % catname
		#if gen['other_category']:
		#	overall_mask = np.invert(overall_mask)
		#	gens[col][overall_mask] = "category:other"
		print "have %s entries: " % col, np.unique(gens[col])

	print
	print "types: ", dataset.dtype
	print

	if mpi_rank == 0:
		fd = h5py.File(config.output, "w")

	index_cols = []
	for analysis in config.analyses:
		analysis_data = dataset.copy()
		index_cols = []
		for axis in analysis['axes']:
			for idxcol in axis:
				if idxcol not in index_cols:
					index_cols.append(idxcol)
		remove_list = []
		for col in analysis_data.dtype.names:
			if col.endswith("_sum") or col.endswith("_count") or col.endswith("_hist") or col in index_cols:
				pass
			else:
				remove_list.append(col)

		## copy generalizations into dataset
		dtype_list = []
		for key in analysis_data.dtype.fields:
			if key not in remove_list:
				dtype_list.append( (key, analysis_data.dtype.fields[key][0],) )
		dtype_list.extend([ (key, gens[key].dtype,) for key in gens ])
		new_dataset = np.zeros(analysis_data.size, dtype=np.dtype(dtype_list))
		for col in analysis_data.dtype.names:
			if col in remove_list:
				continue
			new_dataset[col] = analysis_data[col]
		for col in gens:
			new_dataset[col] = gens[col][:]
		analysis_data = new_dataset

		print "making most detailed version of %s analysis using " % analysis['name'], index_cols
		count = analysis_data.size
		print index_cols
		print analysis_data.dtype.names
		analysis_data = workloadAnalysis_pd.mpiMergeDataset(analysis_data, index_cols, comm, mpi_rank, mpi_size)
		print "done. went from %d to %d entries" % (count, analysis_data.size)
		for col in index_cols:
			print col, np.unique(analysis_data[col])

		print "starting axis summarizations:"
		for idx,axis in enumerate(analysis['axes']):
			dtype_list = []
			for name in analysis_data.dtype.names:
				if name in axis or name.endswith("_sum") or name.endswith("_count") or name.endswith("_histogram"):
					dtype_list.append( (name, analysis_data[name].dtype,) )
			axis_data = np.zeros(analysis_data.size, dtype=np.dtype(dtype_list))
			for name in axis_data.dtype.names:
				axis_data[name] = analysis_data[name].copy()
			axis_data = workloadAnalysis_pd.mpiMergeDataset(axis_data, axis, comm, mpi_rank, mpi_size)
			print "completed axis %s with %d records, writing data" % (analysis['axisNames'][idx], axis_data.size)
			if mpi_rank == 0:
				dset = fd.create_dataset('%s_%s' % (analysis['name'], analysis['axisNames'][idx]), (axis_data.size,), dtype=axis_data.dtype)
				dset[:] = axis_data[:]
			print "completed writing data out."
	if mpi_rank == 0:
		fd.close()

if __name__ == "__main__":
	main(sys.argv[1:])