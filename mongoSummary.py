#!/usr/bin/env python

import numexpr
numexpr.set_num_threads(1)

from pymongo import MongoClient
import pandas
import tables
import sys
import os

collection_label = sys.argv[1]
summary_file = sys.argv[2]
date = sys.argv[3]

client = MongoClient('128.55.56.19', 27017)
db = client.procmon
db.authenticate('procmon', 'nomcorp')
collection = db[collection_label]

summary_labels = ['execProject', 'execUser', 'executables', 'projects',
    'scriptProject', 'scriptUser', 'scripts', 'users' ]


for summary_label in summary_labels:
    print summary_label
    df = pandas.read_hdf(summary_file, summary_label).reset_index()

    d = [ 
        dict([
            (colname, row[i]) 
            for i,colname in enumerate(df.columns)
        ])
        for row in df.values
    ]
    collcount = collection.find({'date':date, 'type':summary_label}).count()
    if collcount == len(d):
        print "Got same # of records in database, skipping."
        continue
    elif collcount > 0 and len(d) > 0:
        print "There are %d records in database, %d records in h5." % (collcount, len(d))
        print "Printing removing from database and re-writing."
        collection.remove({'date':date, 'type':summary_label})
    for v in d:
        v['date'] = date
        v['type'] = summary_label
        collection.insert(v)
    collcount = collection.find({'date':date, 'type':summary_label}).count()
    if collcount != len(d):
        print "WARNING: not all records inserted to database! (db: %d, h5: %d)" % (collcount, len(d))
