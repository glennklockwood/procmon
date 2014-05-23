import numpy as np
import h5py
from datetime import datetime,time,timedelta
import pandas as pd
import sys

import procmon

dataType = None

def parse_h5(filename):
	global dataType
	print "parsing file %s" % filename
	input = h5py.File(filename, 'r')
	ret = []
	for host in input:
		hostgroup = input[host]
		if 'procdata' not in hostgroup:
			continue

		nRec = hostgroup['procdata'].attrs['nRecords']
		procdata = hostgroup['procdata'][0:nRec]
		recs = []
		if dataType is None:
			dataType = np.dtype([('startTime', procdata['startTime'].dtype,),('pid',procdata['pid'].dtype,),('isParent', np.int8,),('host','|S36',)])
		parents = np.unique(np.intersect1d(procdata['pid'], procdata['ppid']))
		allprocs = np.unique(procdata[['pid','startTime']])
		l_areparents = np.zeros(shape=allprocs.size, dtype=dataType)
		for idx,id_ in enumerate(allprocs):
			l_areparents['pid'][idx] = id_[0]
			l_areparents['startTime'][idx] = id_[1]
		l_areparents['host'] = host
		parentmask = np.zeros(shape=allprocs.size, dtype=bool)
		for parent in parents:
			mask = l_areparents['pid'] == parent
			parentmask |= mask
		l_areparents['isParent'][parentmask] = 1
		ret.append(l_areparents)
	input.close()
	return np.concatenate(ret)




def main(args):
	## initialize configurable variables
    h5_path = "/global/projectb/statistics/procmon/genepool"
    #h5_path = "/scratch/proc"
    h5_prefix = "procmon_genepool"
    start_time = args[0]
    output_file = args[1]


    start_time = datetime.strptime(start_time, "%Y%m%d%H%M%S")
    end_time = start_time + timedelta(days=1)
    end_time -= timedelta(seconds=1)
    procmon_h5cache = procmon.H5Cache.H5Cache(h5_path, h5_prefix)
    filenames = [ x['path'] for x in procmon_h5cache.query(start_time, end_time) ]
    #filenames = [ filenames[0] ]
    parents = []
    for filename in filenames:
    	f_parents = parse_h5(filename)
    	parents.append(f_parents)
    parents = np.concatenate(parents)
    parents = pd.DataFrame(parents)
    groups = parents.groupby(['startTime','pid','host'])
    parents = groups.sum()
    parents = parents.reset_index()
    np_parents = np.zeros(shape=parents.shape[0], dtype=dataType)
    np_parents['startTime'] = parents['startTime']
    np_parents['pid'] = parents['pid']
    np_parents['host'] = parents['host']
    np_parents['isParent'] = parents['isParent']

    print "writing output"
    output = h5py.File(output_file, 'w')
    dset = output.create_dataset('parents', (np_parents.size,), dtype=dataType)
    dset[:] = np_parents[:]
    output.close()

if __name__ == "__main__":
  	main(sys.argv[1:])






