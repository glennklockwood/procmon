import numpy as np
import pandas as pd
import os
import sys
import workloadAnalysis_pd


def main(args):
	config = workloadAnalysis_pd.Config(args).config

	print config

	dataset = []
	for filename in config.files:
		dataset.append(pd.read_hdf(filename,'useful'))
	dataset = pd.concat(dataset)
	dataset = dataset.reset_index()

	login_mask = np.zeros(dataset.shape[0], dtype=bool)
	## perform any needed generalizations
	for gen in config.generalizations:
		print "Starting on generalization: " + gen['name']
		col = 'generalization:%s' % gen['name']
		dataset[col] = dataset[gen['column']].copy()

		overall_mask = np.zeros(dataset.shape[0], dtype=bool)
		for catname in gen['eval_order']:
			regexes = gen['categories'][catname]
			mask = np.zeros(dataset.shape[0], dtype=bool)
			sys.stdout.write("starting on category:%s; " % catname)
			for regex in regexes:
				mask = dataset[col].str.contains(regex) | mask
				sys.stdout.write("%d " % np.sum(mask))
			overall_mask = mask | overall_mask
			print "; (%d)" % np.sum(overall_mask)
			dataset[col][mask] = "category:%s" % catname
		#if gen['other_category']:
		#	overall_mask = np.invert(overall_mask)
		#	dataset[col][overall_mask] = "category:other"
		print "have %s entries: " % col, dataset[col].unique()

	print
	print "types: ", dataset.dtypes
	print

	index_cols = []
	for analysis in config.analyses:
		analysis_data = dataset.copy()
		index_cols = []
		for axis in analysis['axes']:
			for idxcol in axis:
				if idxcol not in index_cols:
					index_cols.append(idxcol)
		remove_list = []
		for col in analysis_data.columns:
			if col.endswith("_sum") or col.endswith("_count") or col.endswith("_hist") or col in index_cols:
				pass
			else:
				remove_list.append(col)
		print "nuking columns: ", remove_list
		for col in remove_list:
			del dataset[col]

		print "making most detailed version of %s analysis using " % analysis['name'], index_cols
		for col in index_cols:
			nullvals = analysis_data[col].isnull()
			if np.sum(nullvals) > 0:
				analysis_data[col][nullvals] = "NoValue"
			print col, analysis_data[col].unique()
		count = analysis_data.shape[0]
		analysis_data.to_hdf(config.output, 'raw_data')
		analysis_groups = analysis_data.groupby(index_cols)
		analysis_data = analysis_groups.sum()
		analysis_data = analysis_data.reset_index()
		analysis_data.to_hdf(config.output, 'reduced_data')
		print "done. went from %d to %d entries" % (count, analysis_data.shape[0])
		for col in index_cols:
			print col, analysis_data[col].unique()

		print "starting axis summarizations:"
		for idx,axis in enumerate(analysis['axes']):
			axis_groups = analysis_data.groupby(axis)
			axis_data = axis_groups.sum()
			print "completed axis %s with %d records, writing data" % (analysis['axisNames'][idx], axis_data.shape[0])
			axis_data.to_hdf(config.output, '%s_%s' % (analysis['name'], analysis['axisNames'][idx]))
			print "completed writing data out."


if __name__ == "__main__":
	main(sys.argv[1:])