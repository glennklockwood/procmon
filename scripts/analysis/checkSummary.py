import pandas as pd
import numpy as np
import os
import sys

def checkSummary(filename, dataset):
	summary = pd.read_hdf(filename, dataset)
	summary = summary.reset_index()
	print "%s has %d records" % (filename, summary.shape[0])
	mask = summary.username.str.contains('^$')
	if np.sum(mask) > 0:
		print "ERROR, null usernames"

checkSummary(sys.argv[1], sys.argv[2])