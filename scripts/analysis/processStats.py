#!/usr/bin/env python

import h5py
import numpy as np
import os
import sys
import re

from sklearn.cluster import MiniBatchKMeans, KMeans
from sklearn.metrics.pairwise import euclidean_distances

fd = h5py.File(sys.argv[1], 'r')
dset = fd['processes']
dset_count = dset.len()
print "has %d elements" % dset_count

#indices = np.random.random_integers(0, dset_count-1, size=10000)
#t = dset[0]
#cluster_data = np.zeros(shape=10000, dtype=t.dtype)
#print "reading random sample"
#for i in xrange(10000):
#    cluster_data[i] = dset[indices[i]]
#
#sys.exit(0)

strlen = np.vectorize(len)
ishousemon = re.compile('usearch')
housemon = np.vectorize(lambda x: bool(ishousemon.match(x)))

base_idx = 0
useful_commands = np.empty(shape=0, dtype='|S64')
not_commands = np.empty(shape=0, dtype='|S64')
hosts = np.empty(shape=0, dtype='|S64')
parentBins = np.zeros(50, np.int64)
leafBins = np.zeros(50, np.int64)
durBins = np.zeros(22, np.int64)
notdurBins = np.zeros(22, np.int64)
units = None
indices = None
usefulDur = 0.

def id_unknowns(row):
    if row[indices['nObservations']] > 1:
        print row[indices['nObservations']],row

while base_idx < dset_count:
    limit = base_idx + 10000
    limit = min(limit, dset_count)
    data = dset[base_idx:limit]
    base_idx = limit
    if indices is None:
        indices = {}
        for (idx,col) in enumerate(data.dtype.names):
            indices[col] = idx
    hosts = np.unique(np.concatenate((data['host'], hosts)))
    continue

    singleton = data['nObservations'] == 1
    ancestors = data['isParent'] == 1
    nanmask = data['duration'] > 86400
    if np.sum(nanmask) > 0:
        print data[nanmask]
        print data[nanmask][['nObservations','volatilityScore','isParent','startTime','recTime']]
    highVol   = np.greater_equal(data['volatilityScore'], 0.1)
    highCpu   = np.greater_equal(data['cputime_net'], data['duration']*0.5)
    useful = highVol 
    notuseful = np.invert(useful)
    usefulDur += np.nansum(data[useful]['duration'])

    leaves = np.invert(ancestors)
    hms = housemon(data['command'])
#    if np.sum(hms & notuseful) > 0:
#print "USEFUL:"
#        print data[hms & useful]
#        print "NOT USEFUL:"
#        print data[hms & notuseful]
#        print data[hms & notuseful][['volatilityScore','isParent','cputime','cputime_net','duration']]

    h1 = np.histogram(data[ancestors]['volatilityScore'], bins=50, range=(0.,1.))
    h2 = np.histogram(data[leaves]['volatilityScore'], bins=50, range=(0.,1.))
    dur = np.histogram(data[useful]['duration'], bins=[0,1,3,5,15,30,60,120,300,600,1200,1800,3600,7200,14400,21600,28800,36000,43200,43500,64800,86700,2**63])
    notdur = np.histogram(data[notuseful]['duration'], bins=[0,1,3,5,15,30,60,120,300,600,1200,1800,3600,7200,14400,21600,28800,36000,43200,43500,64800,86700,2**63])
    parentBins += h1[0]
    leafBins += h2[0]
    durBins += dur[0]
    notdurBins += notdur[0]
    if units is None:
        units = h1[1]
    useful_commands = np.unique(np.concatenate((data['command'][useful], useful_commands)))
    not_commands = np.unique(np.concatenate((data['command'][notuseful], not_commands)))

print "hosts:" , hosts
sys.exit(0)
print "USEFUL!"
print useful_commands.size, list(useful_commands)
print "\nNOT USEFUL!"
print not_commands.size, list(not_commands)
print "Duration Bins   : ", list(durBins)
print "Duration BinsNOT: ", list(notdurBins)
print [0,1,3,5,15,30,60,120,300,600,1200,1800,3600,7200,14400,21600,28800,36000,43200,43500,64800,86400,2**63]
print "Have Children volatility: ", list(parentBins)
print "Are Leaves    volatility: ", list(leafBins)
print "units                   : ", list(units)
print "type : ", data.dtype
print "usefultime : ", usefulDur, " s"
