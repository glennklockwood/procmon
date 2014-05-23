import os
import sys
import re
import argparse

import numpy as np
import pandas as pd
import h5py

import procmon

procmonInstallBase = ''
procmon_h5cache = None
if 'PROCMON_DIR' in os.environ:
    procmonInstallBase = os.environ['PROCMON_DIR']

class Config:
	def __init__(self, args):
		self.config = self.__parse_args(args)

	def __parse_args(self, args):
		global procmonInstallBase

		parser = argparse.ArgumentParser(add_help=False)
		parser.add_argument('-f', '--config', 
			help="Specify configuration file instead of default at $PROCMON_DIR/etc/workloadAnalysis.conf",
			default='%s/etc/workloadAnalysis.conf' % procmonInstallBase,
			metavar="FILE"
		)
		args, remaining_args = parser.parse_known_args()

		parser = argparse.ArgumentParser(parents=[parser])
		#parser.set_defaults(**defaults)
		parser.add_argument('-o','--output', type=str, help="Specify output summary filename", default='output.h5')
		parser.add_argument('-s','--summary', type=str, help="Summary dataset to merge", default="useful")
		parser.add_argument('files', metavar='N', type=str, nargs='+', help='Processes h5 files to summarize')
		args = parser.parse_args(remaining_args, args)
		return args

def checkSummary(filename, summary):
	summary = summary.reset_index()
	ret = True
	mask = summary.username.str.contains('^$')
	if np.sum(mask) > 0:
		print "%s ERROR, null usernames" % filename
		ret = False
	mask = summary.host.str.contains('^$')
	if np.sum(mask) > 0:
		print "%s ERROR, null hostnames" % filename
		ret = False
	mask = summary.project.str.contains('^$')
	if np.sum(mask) > 0:
		print "%s ERROR, null project names" % filename
		ret = False
	mask = summary.command.str.contains('^$')
	if np.sum(mask) > 0:
		print "%s ERROR, null command names" % filename
		ret = False
	return ret

def fixTypes(summary):
	for col in summary.columns:
		if col.endswith("_sum") or col.endswith("_count"):
			summary[col] = summary[col].astype(np.float64)
	return summary

def main(args):
	config = Config(args).config
	summary = None
	running_sum = 0
	for filename in config.files:
		print "Parsing file %s" % filename
		t_summary = pd.read_hdf(filename, config.summary)
		if not checkSummary(filename, t_summary):
			print "FAILED, errors check %s" % filename
			sys.exit(1)
		running_sum += t_summary.shape[0]
		t_summary = fixTypes(t_summary)
		if summary is None:
			summary = t_summary
		else:
			summary = summary.combineAdd(t_summary)
		print "summary size: %d / %d" % (summary.shape[0], running_sum)
	print "BEFORE: ", summary.dtypes
	summary = fixTypes(summary)
	print "AFTER: ", summary.dtypes
	total_size = summary.shape[0]
	base_idx = 0
	fpart = 1
	while base_idx < total_size:
		base = base_idx
		limit = min(base+500000, total_size)
		base_idx = limit

		write_data = summary.ix[base:limit]
		fname = "%s_pt%d.h5" % (config.output, fpart)
		fpart += 1
		print "about to write to %s" % fname
		write_data.to_hdf(fname, config.summary)
	


if __name__ == "__main__":
	main(sys.argv[1:])
