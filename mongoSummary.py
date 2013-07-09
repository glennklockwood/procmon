#!/usr/bin/env python

import numexpr
numexpr.set_num_threads(1)

from pymongo import MongoClient
import pandas
import tables
import sys
import os

client = MongoClient('genepool12.nersc.gov', 27017)
db = client.procmon
db.authenticate('procmon', 'nomcorp')
house_collection = db['houseHunter']

summary_labels = ['execProject', 'execUser', 'executables', 'projects',
    'scriptProject', 'scriptUser', 'scripts', 'users' ]

summary_file = sys.argv[1]
date = sys.argv[2]

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
    for v in d:
        v['date'] = date
        v['type'] = summary_label

    house_collection.insert(d)

