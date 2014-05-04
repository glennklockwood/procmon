#!/usr/bin/env python
## procFinder.py -- query the procmon data
##
## Author: Doug Jacobsen <dmj@nersc.gov>
## Copyright (C) 2013 - The Regents of the University of California

## discovered that numexpr was starting as many threads as there are cores;
## this, in combination with MPI was causing far too many running processes at
## the same time.  Also, the code ran ~2x faster without these excess threads
import numexpr
numexpr.set_num_threads(1)

import numpy as np
import numpy.lib.recfunctions as nprec
import h5py
import sys
import os
import traceback
import pandas
from datetime import date,datetime,timedelta,time
import re
import cPickle
import pwd
import subprocess
import tempfile
from mpi4py import MPI
import procmon
import procmon.Scriptable
import stat

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

monitored_filesystems = {
    'projectb':'^(/chos)?/global/projectb/.*',
    'project':'^(/chos)?/global/project/.*',
    'scratch2':'^(/chos)?/global/scratch2/.*',
    'u1':'^(/chos)?/global/u1/.*',
    'u2':'^(/chos)?/global/u2/.*',
    'seqfs':'^(/chos)?/global/seqfs/.*',
    'dna':'^(/chos)?/global/dna/.*',
    'common':'^(/chos)?/global/common/.*',
    'local_scratch':'^/scratch/.*',
    'local_tmp':'^/tmp/.*'
}

def generate_mpi_type_simple(np_dtype):
    offsets = [0]
    counts  = [np_dtype.itemsize]
    mpi_types = [MPI.BYTE]
    mpidt = MPI.Datatype.Create_struct(counts, offsets, mpi_types)
    mpidt_final = mpidt.Create_resized(0, np_dtype.itemsize).Commit()
    mpidt.Free()

    return mpidt_final


def generate_mpi_type(np_dtype):
    fields = sorted([ np_dtype.fields[x] for x in np_dtype.fields ], key=lambda y: y[1])
    offsets = [ x[1] for x in fields ]
    counts = []
    mpi_types = []
    for field in fields:
        stype = field[0].str
        byte_count = int(stype[2:])
        str_type = stype[1]
        field_count = 1
        mpi_type = MPI.BYTE
        if str_type == 'S':
            field_count = byte_count
            mpi_type = MPI.CHAR
        if str_type == 'u' and byte_count == 4:
            mpi_type = MPI.UNSIGNED
        if str_type == 'u' and byte_count == 8:
            mpi_type = MPI.UNSIGNED_LONG_LONG
        if str_type == 'i' and byte_count == 4:
            mpi_type = MPI.INT
        if str_type == 'i' and byte_count == 8:
            mpi_type = MPI.LONG_LONG
        if byte_count == 1:
            mpi_type = MPI.BYTE

        if mpi_type == MPI.BYTE:
            field_count = byte_count

        counts.append(field_count)
        mpi_types.append(mpi_type)
    mpidt = MPI.Datatype.Create_struct(counts, offsets, mpi_types)
    mpidt_final = mpidt.Create_resized(0, np_dtype.itemsize).Commit()
    mpidt.Free()

    return mpidt_final
            
class HostProcesses:
    def __init__(self, hostname):
        self.hostname = hostname
        self.read_datasets = ['procdata','procstat','procfd','procobs']
        self.gentype = None
        self.procmon = {
            'procdata': None,
            'procstat': None,
            'procfd' : None,
            'procobs'  : None,
        }
        self.procmon_keys = {
            'procdata': ['pid','startTime'],
            'procstat': ['pid','startTime','recTime'],
        }
        self.mpi_types = {}

    def add_data(self, hostgroup, query):
        tmpdata = {}
        global monitored_filesystems
        try:
            for dataset in self.read_datasets:
                nRec = hostgroup[dataset].attrs['nRecords']
                data = hostgroup[dataset][0:nRec]
                   
                if dataset not in self.procmon or self.procmon[dataset] is None:
                    self.procmon[dataset] = data
                else:
                    self.procmon[dataset] = np.concatenate((self.procmon[dataset],data))
                if data is not None and len(data) > 0:
                    tmpdata[dataset] = data
        except:
            sys.stderr.write('[%d] unable to retrieve data for %s; skipping\n' % (rank, hostname))
            traceback.print_exc(file=sys.stderr)
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback, limit=1, file=sys.stderr)
            return

        return

    
    def reduce_data(self, datasets):
        if ('procdata' not in datasets or datasets['procdata'] is None) or ('procstat' not in datasets or datasets['procstat'] is None) or ('procfd' not in datasets or datasets['procfd'] is None) or ('procobs' not in datasets or datasets['procobs'] is None):

            print "[%d] %s missing a dataset! I have: "%(rank,self.hostname),datasets.keys(), 
            return

        print "[%d] starting to reduce %s" % (rank, self.hostname)
        pd = datasets['procdata']
        ps = datasets['procstat']
        po = datasets['procobs']
        pf = datasets['procfd']

        self.orig_types = {
            'procdata': pd.dtype,
            'procstat': ps.dtype,
            'procobs' : po.dtype,
            'procfd'  : pf.dtype,
        }

        pd.sort(order=['recTime'])
        ps.sort(order=['recTime'])
        po.sort(order=['recTime'])
        pf.sort(order=['recTime'])

        ## identify pids which have ancestors, done by transposing pidv
        ## vector and subtracting ppid vector, and looking for indices
        ## that are zero-value
        ## the underlying assumption is that we won't see any duplication
        ## of pids (or ppids) at this point
        def detectChildProcesses(pids, ppids):
            out = []
            it = np.nditer([pids, ppids], ['external_loop'], [['readonly'], ['readonly']],
                    op_axes=[range(pids.ndim)+[-1]*ppids.ndim, [-1]*pids.ndim+range(ppids.ndim)])
            for (lpids, lppids) in it:
                out.append(np.unique( lpids[np.nonzero(np.subtract(lpids,lppids) == 0)[0]]))
            return np.unique(np.concatenate(out))
        parentPids = detectChildProcesses(np.unique(pd['pid']), np.unique(pd['ppid']))

        ## detect which filesystems were written to and which were read from
        fs_masks = {}
        fs_types = [('pid',ps['pid'].dtype), ('startTime',ps['startTime'].dtype), ('recTime',ps['recTime'].dtype),('fread',bool),('fwrite',bool)]
        for fs in monitored_filesystems:
            fsq = re.compile(monitored_filesystems[fs])
            fsmatch = np.vectorize(lambda x: bool(fsq.match(x)))
            fs_mask = fsmatch(pf['path'])
            fs_masks[fs] = fs_mask
            fs_types.append(('%s' % fs, np.bool,))
        fsData = np.zeros(shape=pf.size, dtype=np.dtype(fs_types))
        fsData['pid'] = pf['pid']
        fsData['startTime'] = pf['startTime']
        fsData['recTime'] = pf['recTime']
        fsData['fread'] = (stat.S_IRUSR & pf['mode']) > 0
        fsData['fwrite'] = (stat.S_IWUSR & pf['mode']) > 0
        for fs in fs_masks:
            fsData[fs] = fs_masks[fs]

        # convert to pandas DataFrames for more efficient
        # grouping and summarization    
        pd = pandas.DataFrame(pd)#.sort(['recTime','recTimeUSec'])
        ps = pandas.DataFrame(ps)#.sort(['recTime','recTimeUSec'])
        po = pandas.DataFrame(po)#.sort(['recTime','recTimeUSec'])
        fs = pandas.DataFrame(fsData)#.sort(['recTime','recTimeUSec'])

        pd['hasChildren'] = False
        parentIdx = pd.query('pid in parentPids').index
        pd['hasChildren'][parentIdx] = True

        pd_group = pd.groupby(['pid','startTime'])
        ps_group = ps.groupby(['pid','startTime'])
        po_group = po.groupby(['pid','startTime'])
        fs_group = fs.groupby(['pid','startTime'])

        def summarize_timeseries(ps_rec):
            ps_rec = ps_rec.sort(['recTime'])
            cpu = (ps_rec.utime + ps_rec.stime) / 100.
            iosum = ps_rec.io_wchar + ps_rec.io_rchar
            duration = ps_rec.recTime - ps_rec.startTime
            durDiff = duration.diff()
            cpuRate = cpu.diff() / durDiff
            iowRate = ps_rec.io_wchar.diff() / durDiff
            iorRate = ps_rec.io_rchar.diff() / durDiff
            ioRate = iosum.diff() / durDiff
            msizeRate = ps_rec.m_size.diff() / durDiff
            mresidentRate = ps_rec.m_resident.diff() / durDiff

            rates = pandas.DataFrame({'cpuRate':cpuRate, 'iowRate':iowRate, 'iorRate':iorRate, 'ioRate':ioRate, 'msizeRate': msizeRate, 'mresidentRate': mresidentRate})
            correlation = rates.corr()
            retData = {
                'cpuRateMax': cpuRate.max(),
                'iowRateMax': iowRate.max(),
                'iorRateMax': iorRate.max(),
                'msizeRateMax': msizeRate.max(),
                'mresidentRateMax': mresidentRate.max(),
                'nRecords': ps_rec.shape[0],
            }
            res = ['cpu','iow','ior','io','msize','mresident']
            resIdx1 = 0
            resIdx2 = 0
            while resIdx1 < len(res):
                resIdx2 = resIdx1 + 1
                while resIdx2 < len(res):
                    retData['cor_%sX%s'%(res[resIdx1],res[resIdx2])] = correlation['%sRate'%res[resIdx1]]['%sRate'%res[resIdx2]]
                    resIdx2 += 1
                resIdx1 += 1
                    
            return pandas.Series(retData)

        def summarize_procobs(po_rec):
            return pandas.Series({'nRecords': po_rec.shape[0]})

        def summarize_fs(fs_rec):
            ret = {}
            for fs in monitored_filesystems:
                ret['fs_%s_write' % fs] = sum(fs_rec.fwrite & fs_rec[fs]) > 0
                ret['fs_%s_read' % fs] = sum(fs_rec.fread & fs_rec[fs]) > 0
            return pandas.Series(ret)
                
        def pd_last_record(x):
            retRec = x.ix[x.index[-1]]
            if x.shape[0] > 1:
                prevRec = x.ix[x.index[-2]]
                if retRec['exePath'] == 'Unknown':
                    prevRec = prevRec.copy(True)
                    prevRec['recTime'] = retRec['recTime']
                    prevRec['recTimeUSec'] = retRec['recTimeUSec']
                    return prevRec
            return retRec

        ps_summaries = ps_group.apply(summarize_timeseries)
        po_summaries = po_group.apply(summarize_procobs)
        fs_summaries = fs_group.apply(summarize_fs)

        pd_final = pd_group.apply(pd_last_record)
        ps_final = ps_group.last()
        combined = pd_final.join(other=ps_final, how='left', rsuffix='_ps').join(other=ps_summaries,how='left').join(other=fs_summaries, how='left')
        combined['volatilityScore'] = 0.
        combined['volatilityScore'] = (ps_summaries['nRecords'] - 1) / po_summaries['nRecords']
        combined['nObservations'] = po_summaries['nRecords']
        cols = combined.columns
        for col in cols:
            if col.endswith('_ps'):
                del combined[col]

        combined['host'] = self.hostname
        self.procmon['processes'] = combined

    def set_baseline(self, baseline, start_time):
        baseline_ps = pandas.DataFrame(baseline['procstat']).sort(['recTime'])
        baseline_ps = baseline_ps.groupby(['pid','startTime']).last()
        processes = self.procmon['processes']
        processes['startTime_baseline'] = processes.startTime.copy(True)
        processes['utime_baseline'] = np.zeros(processes.shape[0],dtype=processes.utime.dtype)
        processes['stime_baseline'] = np.zeros(processes.shape[0],dtype=processes.stime.dtype)
        processes['io_rchar_baseline'] = np.zeros(processes.shape[0],dtype=processes.io_rchar.dtype)
        processes['io_wchar_baseline'] = np.zeros(processes.shape[0],dtype=processes.io_wchar.dtype)
        early_starters = processes.ix[processes.startTime < start_time].index
        if baseline_ps is not None and not baseline_ps.empty and len(early_starters) > 0:
            # identify processes which started before start_time
            processes.utime_baseline[early_starters] = baseline_ps.ix[early_starters].utime
            processes.stime_baseline[early_starters] = baseline_ps.ix[early_starters].stime
            processes.io_rchar_baseline[early_starters] = baseline_ps.ix[early_starters].io_rchar
            processes.io_wchar_baseline[early_starters] = baseline_ps.ix[early_starters].io_wchar
            processes.startTime_baseline[early_starters] = start_time
        
            ## in case the baseline data is missing some things we think should be there
            processes.utime_baseline[np.invert(processes.utime_baseline.notnull())] = 0
            processes.stime_baseline[np.invert(processes.stime_baseline.notnull())] = 0

    def generate_nptype(self, processes):
        if self.gentype is not None:
            return self.gentype
        types = []
        fixedTypes = {
            'nRecords': np.uint64,
            'nObservations': np.uint64,
            'host': '|S48',
            'script': '|S1024',
            'execCommand': '|S1024',
            'command': '|S64',
            'username': '|S12',
            'project': '|S20',
            'job': '|S64',
        }
        for col in processes.columns:
            if col in self.orig_types['procdata'].names:
                types.append( (col, self.orig_types['procdata'].fields[col][0]) )
            elif col in self.orig_types['procstat'].names:
                types.append( (col, self.orig_types['procstat'].fields[col][0]) )
            elif col == "hasChildren" or col.startswith("fs_"):
                types.append( (col, bool) )
            elif col.startswith("cor_") or col.endswith("Max") or col == "volatilityScore":
                types.append( (col, np.float64) )
            elif col.endswith('_baseline'):
                realcol = re.match('(.*)_baseline', col).groups()[0]
                found_type = np.int64
                if realcol in self.orig_types['procdata'].names:
                    found_type = self.orig_types['procdata'].fields[realcol][0]
                elif realcol in self.orig_types['procstat'].names:
                    found_type = self.orig_types['procstat'].fields[realcol][0]
                types.append( (col, found_type) )
            elif col in fixedTypes:
                types.append( (col, fixedTypes[col]) )
            else:
                print 'missing type for col: ', col
        self.gentype = np.dtype(types)
        return self.gentype


    def count_processes(self):
        if 'procdata' in self.procmon and self.procmon['procdata'] is not None:
            unique_processes = np.unique(self.procmon['procdata'][['pid','startTime']])
            return unique_processes.size
        return 0


def parse_h5(filename, id_processes, query):
    """Parse hdf5 file, store data (according to query) in id_processes"""
    print "parsing: %s" % filename
    fd = h5py.File(filename, 'r')

    totalRecords = 0
    finalRecords = 0
    hostnames = None
    base_hostlist = ['b2r2ibm1t-02', 'gpconvey00', 'gpconvey01', 'gpht-01', 'gpht-02', 'gpht-03', 'gpht-04', 'gptb-01', 'mc0155', 'mc0156', 'mc0157', 'mc0158', 'mc0159', 'mc0160', 'mc0161', 'mc0162', 'mc0163', 'mc0164', 'mc0165', 'mc0166', 'mc0167', 'mc0168', 'mc0169', 'mc0170', 'mc0171', 'mc0172', 'mc0173', 'mc0174', 'mc0175', 'mc0176', 'mc0201', 'mc0202', 'mc0203', 'mc0204', 'mc0205', 'mc0206', 'mc0207', 'mc0208', 'mc0209', 'mc0210', 'mc0211', 'mc0212', 'mc0213', 'mc0214', 'mc0215', 'mc0216', 'mc0217', 'mc0218', 'mc0219', 'mc0220', 'mc0221', 'mc0222', 'mc0223', 'mc0224', 'mc0225', 'mc0226', 'mc0227', 'mc0228', 'mc0229', 'mc0230', 'mc0231', 'mc0232', 'mc0233', 'mc0234', 'mc0235', 'mc0236', 'mc0237', 'mc0238', 'mc0239', 'mc0240', 'mc0241', 'mc0242', 'mc0243', 'mc0244', 'mc0245', 'mc0246', 'mc0247', 'mc0248', 'mc0249', 'mc0250', 'mc0251', 'mc0252', 'mc0253', 'mc0254', 'mc0255', 'mc0256', 'mc0257', 'mc0258', 'mc0259', 'mc0260', 'mc0261', 'mc0262', 'mc0263', 'mc0264', 'mc0265', 'mc0266', 'mc0267', 'mc0268', 'mc0401', 'mc0402', 'mc0403', 'mc0404', 'mc0405', 'mc0406', 'mc0407', 'mc0408', 'mc0409', 'mc0410', 'mc0411', 'mc0412', 'mc0413', 'mc0414', 'mc0415', 'mc0416', 'mc0417', 'mc0418', 'mc0419', 'mc0420', 'mc0421', 'mc0422', 'mc0423', 'mc0424', 'mc0425', 'mc0426', 'mc0427', 'mc0428', 'mc0429', 'mc0430', 'mc0431', 'mc0432', 'mc0433', 'mc0434', 'mc0435', 'mc0436', 'mc0437', 'mc0438', 'mc0439', 'mc0440', 'mc0441', 'mc0442', 'mc0443', 'mc0444', 'mc0445', 'mc0446', 'mc0447', 'mc0448', 'mc0449', 'mc0450', 'mc0451', 'mc0452', 'mc0453', 'mc0454', 'mc0455', 'mc0456', 'mc0457', 'mc0458', 'mc0459', 'mc0460', 'mc0461', 'mc0462', 'mc0463', 'mc0464', 'mc0501', 'mc0502', 'mc0503', 'mc0504', 'mc0505', 'mc0506', 'mc0507', 'mc0508', 'mc0509', 'mc0510', 'mc0511', 'mc0512', 'mc0513', 'mc0514', 'mc0515', 'mc0516', 'mc0517', 'mc0518', 'mc0519', 'mc0520', 'mc0521', 'mc0522', 'mc0523', 'mc0524', 'mc0525', 'mc0526', 'mc0527', 'mc0528', 'mc0529', 'mc0530', 'mc0531', 'mc0532', 'mc0533', 'mc0534', 'mc0535', 'mc0536', 'mc0537', 'mc0538', 'mc0539', 'mc0540', 'mc0541', 'mc0542', 'mc0543', 'mc0544', 'mc0545', 'mc0546', 'mc0547', 'mc0548', 'mc0549', 'mc0550', 'mc0551', 'mc0552', 'mc0553', 'mc0554', 'mc0555', 'mc0556', 'mc0557', 'mc0558', 'mc0559', 'mc0560', 'mc0561', 'mc0562', 'mc0563', 'mc0564', 'mc0565', 'mc0567', 'mndlhm0101', 'mndlhm0201', 'mndlhm0203', 'mndlhm0205', 'mndlhm0501', 'mndlhm0503', 'mndlhm0505', 'sgi01a01', 'sgi01a02', 'sgi01a03', 'sgi01a04', 'sgi01a05', 'sgi01a06', 'sgi01a07', 'sgi01a08', 'sgi01a09', 'sgi01a10', 'sgi01a11', 'sgi01a12', 'sgi01a13', 'sgi01a14', 'sgi01a15', 'sgi01a16', 'sgi01a17', 'sgi01a18', 'sgi01a19', 'sgi01a20', 'sgi01a21', 'sgi01a22', 'sgi01a23', 'sgi01a24', 'sgi01a25', 'sgi01a26', 'sgi01a27', 'sgi01a28', 'sgi01a29', 'sgi01a30', 'sgi01a31', 'sgi01a32', 'sgi01a33', 'sgi01a34', 'sgi01a35', 'sgi01a36', 'sgi01a37', 'sgi01a38', 'sgi01a39', 'sgi01a40', 'sgi01b01', 'sgi01b02', 'sgi01b03', 'sgi01b04', 'sgi01b05', 'sgi01b06', 'sgi01b07', 'sgi01b08', 'sgi01b09', 'sgi01b10', 'sgi01b11', 'sgi01b12', 'sgi01b13', 'sgi01b14', 'sgi01b15', 'sgi01b16', 'sgi01b17', 'sgi01b18', 'sgi01b19', 'sgi01b20', 'sgi01b21', 'sgi01b22', 'sgi01b23', 'sgi01b24', 'sgi01b25', 'sgi01b26', 'sgi01b27', 'sgi01b28', 'sgi01b29', 'sgi01b31', 'sgi01b32', 'sgi01b33', 'sgi01b34', 'sgi01b35', 'sgi01b36', 'sgi01b37', 'sgi01b38', 'sgi01b39', 'sgi01b40', 'sgi02a01', 'sgi02a02', 'sgi02a03', 'sgi02a04', 'sgi02a05', 'sgi02a06', 'sgi02a07', 'sgi02a08', 'sgi02a09', 'sgi02a10', 'sgi02a11', 'sgi02a12', 'sgi02a13', 'sgi02a14', 'sgi02a15', 'sgi02a16', 'sgi02a17', 'sgi02a18', 'sgi02a19', 'sgi02a20', 'sgi02a21', 'sgi02a22', 'sgi02a23', 'sgi02a24', 'sgi02a25', 'sgi02a26', 'sgi02a27', 'sgi02a28', 'sgi02a29', 'sgi02a30', 'sgi02a31', 'sgi02a32', 'sgi02a33', 'sgi02a34', 'sgi02a35', 'sgi02a36', 'sgi02a37', 'sgi02a38', 'sgi02a39', 'sgi02a40', 'sgi02b01', 'sgi02b02', 'sgi02b03', 'sgi02b04', 'sgi02b05', 'sgi02b06', 'sgi02b07', 'sgi02b08', 'sgi02b09', 'sgi02b10', 'sgi02b11', 'sgi02b12', 'sgi02b13', 'sgi02b14', 'sgi02b15', 'sgi02b16', 'sgi02b17', 'sgi02b18', 'sgi02b19', 'sgi02b20', 'sgi02b21', 'sgi02b22', 'sgi02b23', 'sgi02b24', 'sgi02b25', 'sgi02b26', 'sgi02b27', 'sgi02b28', 'sgi02b29', 'sgi02b30', 'sgi02b31', 'sgi02b32', 'sgi02b33', 'sgi02b34', 'sgi02b35', 'sgi02b36', 'sgi02b37', 'sgi02b38', 'sgi02b39', 'sgi02b40', 'sgi03a01', 'sgi03a02', 'sgi03a03', 'sgi03a04', 'sgi03a05', 'sgi03a06', 'sgi03a07', 'sgi03a08', 'sgi03a09', 'sgi03a10', 'sgi03a11', 'sgi03a12', 'sgi03a13', 'sgi03a14', 'sgi03a15', 'sgi03a16', 'sgi03a17', 'sgi03a18', 'sgi03a19', 'sgi03a20', 'sgi03a21', 'sgi03a22', 'sgi03a23', 'sgi03a24', 'sgi03a25', 'sgi03a26', 'sgi03a27', 'sgi03a28', 'sgi03a29', 'sgi03a30', 'sgi03a31', 'sgi03a32', 'sgi03a33', 'sgi03a34', 'sgi03a35', 'sgi03a36', 'sgi03a37', 'sgi03a38', 'sgi03a39', 'sgi03a40', 'sgi03b01', 'sgi03b02', 'sgi03b03', 'sgi03b04', 'sgi03b05', 'sgi03b06', 'sgi03b07', 'sgi03b08', 'sgi03b09', 'sgi03b10', 'sgi03b11', 'sgi03b12', 'sgi03b13', 'sgi03b14', 'sgi03b15', 'sgi03b16', 'sgi03b17', 'sgi03b18', 'sgi03b19', 'sgi03b20', 'sgi03b21', 'sgi03b22', 'sgi03b23', 'sgi03b24', 'sgi03b25', 'sgi03b26', 'sgi03b27', 'sgi03b28', 'sgi03b29', 'sgi03b30', 'sgi03b31', 'sgi03b32', 'sgi03b33', 'sgi03b34', 'sgi03b35', 'sgi03b36', 'sgi03b37', 'sgi03b38', 'sgi03b39', 'sgi03b40', 'sgi04a01', 'sgi04a02', 'sgi04a03', 'sgi04a04', 'sgi04a05', 'sgi04a06', 'sgi04a07', 'sgi04a08', 'sgi04a09', 'sgi04a10', 'sgi04a11', 'sgi04a12', 'sgi04a13', 'sgi04a14', 'sgi04a15', 'sgi04a16', 'sgi04a17', 'sgi04a18', 'sgi04a19', 'sgi04a20', 'sgi04a21', 'sgi04a22', 'sgi04a23', 'sgi04a24', 'sgi04a25', 'sgi04a26', 'sgi04a27', 'sgi04a28', 'sgi04a29', 'sgi04a30', 'sgi04a31', 'sgi04a32', 'sgi04a33', 'sgi04a34', 'sgi04a35', 'sgi04a36', 'sgi04a37', 'sgi04a38', 'sgi04a39', 'sgi04a40', 'sgi04b01', 'sgi04b02', 'sgi04b03', 'sgi04b04', 'sgi04b05', 'sgi04b06', 'sgi04b07', 'sgi04b08', 'sgi04b09', 'sgi04b10', 'sgi04b11', 'sgi04b12', 'sgi04b13', 'sgi04b14', 'sgi04b15', 'sgi04b16', 'sgi04b17', 'sgi04b18', 'sgi04b19', 'sgi04b20', 'sgi04b21', 'sgi04b22', 'sgi04b23', 'sgi04b24', 'sgi04b25', 'sgi04b26', 'sgi04b27', 'sgi04b28', 'sgi04b29', 'sgi04b30', 'sgi04b31', 'sgi04b32', 'sgi04b33', 'sgi04b34', 'sgi04b35', 'sgi04b36', 'sgi04b37', 'sgi04b38', 'sgi04b39', 'sgi04b40', 'sgi05a01', 'sgi05a02', 'sgi05a03', 'sgi05a04', 'sgi05a05', 'sgi05a06', 'sgi05a07', 'sgi05a08', 'sgi05a09', 'sgi05a10', 'sgi05a11', 'sgi05a12', 'sgi05a13', 'sgi05a14', 'sgi05a15', 'sgi05a16', 'sgi05a17', 'sgi05a18', 'sgi05a19', 'sgi05a20', 'sgi05a21', 'sgi05a22', 'sgi05a23', 'sgi05a24', 'sgi05a25', 'sgi05a26', 'sgi05a27', 'sgi05a28', 'sgi05a29', 'sgi05a30', 'sgi05a31', 'sgi05a32', 'sgi05a33', 'sgi05a34', 'sgi05a35', 'sgi05a36', 'sgi05a37', 'sgi05a38', 'sgi05a39', 'sgi05a40', 'sgi05b01', 'sgi05b02', 'sgi05b03', 'sgi05b04', 'sgi05b05', 'sgi05b06', 'sgi05b07', 'sgi05b08', 'sgi05b09', 'sgi05b10', 'sgi05b11', 'sgi05b12', 'sgi05b13', 'sgi05b14', 'sgi05b15', 'sgi05b16', 'sgi05b17', 'sgi05b18', 'sgi05b19', 'sgi05b20', 'sgi05b21', 'sgi05b22', 'sgi05b23', 'sgi05b24', 'sgi05b25', 'sgi05b26', 'sgi05b27', 'sgi05b28', 'sgi05b29', 'sgi05b30', 'sgi05b31', 'sgi05b32', 'sgi05b33', 'sgi05b34', 'sgi05b35', 'sgi05b36', 'sgi05b37', 'sgi05b38', 'sgi05b39', 'sgi05b40', 'sgi06a16', 'sgi06a17', 'sgi06a18', 'sgi06a19', 'sgi06a20', 'sgi06a21', 'sgi06a22', 'sgi06a23', 'sgi06a24', 'sgi06a25', 'sgi06a26', 'sgi06a27', 'sgi06a28', 'sgi06a29', 'sgi06a30', 'sgi06a31', 'sgi06a32', 'sgi06a33', 'sgi06a34', 'sgi06a35', 'sgi06a36', 'sgi06a37', 'sgi06a38', 'sgi06a39', 'sgi06a40', 'sgi06b16', 'sgi06b17', 'sgi06b18', 'sgi06b19', 'sgi06b20', 'sgi06b21', 'sgi06b22', 'sgi06b23', 'sgi06b24', 'sgi06b25', 'sgi06b26', 'sgi06b27', 'sgi06b28', 'sgi06b29', 'sgi06b30', 'sgi06b31', 'sgi06b32', 'sgi06b33', 'sgi06b34', 'sgi06b35', 'sgi06b36', 'sgi06b37', 'sgi06b38', 'sgi06b39', 'sgi06b40', 'uv10-1', 'uv10-2', 'uv10-3', 'uv10-4', 'uv10-5', 'uv10-6', 'uv10-7', 'uv10-8', 'x4170a01', 'x4170a02', 'x4170a03', 'x4170a04', 'x4170a05', 'x4170a06', 'x4170a07', 'x4170a08', 'x4170a09', 'x4170a10', 'x4170a11', 'x4170a12', 'x4170a13', 'x4170a14', 'x4170a15', 'x4170a16', 'x4170a17', 'x4170a18', 'genepool01', 'genepool02', 'genepool03', 'genepool04', 'genepool10', 'genepool11', 'genepool12', 'gpint01', 'gpint02', 'gpint03', 'gpint04', 'gpint05', 'gpint06', 'gpint07', 'gpint08', 'gpint09', 'gpint10', 'gpint11', 'gpint12', 'gpint13', 'gpint14', 'gpint15', 'gpint16', 'gpint17', 'gpint18', 'gpint19', 'gpint20', 'gpint21', 'gpint22', 'gpint23', 'gpint24', 'gpint25', 'gpint26', 'gpint27',]
    try:
        hostnames = fd.keys()
    except:
        sys.stderr.write("Couldn't get hostnames from %s, problem with file?; falling back on hard-coded host list." % filename)
        hostnames = base_hostlist

    count = 0
    for hostname in hostnames:
        if hostname not in fd:
            continue

#if count > 10:
#            break        
            
        hostgroup = fd[hostname]
        hostdata = None
        if hostname in id_processes:
            hostdata = id_processes[hostname]
        else:
            hostdata = HostProcesses(hostname)
            id_processes[hostname] = hostdata

        hostdata.add_data(hostgroup, query)
        count += 1
        
    finalRecords = 0
    for hostname in id_processes:
        finalRecords += id_processes[hostname].count_processes()

    print "[%d] ===== %d =====" % (rank, finalRecords)

def get_job_data(start, end, qqacct_file):
    """Read job (qqacct) data from a file and return pandas DataFrame of it."""
    ret = -1
    qqacct_data = None
    try:
        qqacct_data = pandas.read_table(qqacct_file, sep=':', header=None, names=['user','project','job','task','hostname','h_rt','wall','hvmem','maxvmem','ppn','sge_status','exit_status'])
        ret = 0
    except:
        pass
    return (ret, qqacct_data)

def identify_scripts(processes):
    """For known scripting interpreter processes, work out what the executed script was."""
    executables = processes.exePath.unique()
    prefixes = ["perl","python","ruby","bash","sh","tcsh","csh","java"]
    processes['script'] = None
    for exe in executables:
        executable = os.path.split(exe)[1]
        scriptable = False
        for prefix in prefixes:
            pattern = "^%s.*" % prefix
            if re.match(pattern, executable) is not None:
                scriptable = True
        if scriptable:
            selection = processes.exePath == exe
            subset = processes.ix[selection]
            args = subset.cmdArgs.str.split("|")
            scripts = args.apply(lambda x: procmon.Scriptable.Scriptable(executable, x[1::]))
            processes.script[selection] = scripts

def identify_userCommand(processes):
    """ Use a simple heuristic to identify the intended command for each process
        depends on indentify_scripts already having been run """

    processes['execCommand'] = processes.exePath.str.split('/').str.get(-1)
    command = processes.script.str.split('/').str.get(-1)
    mask = (command == "COMMAND" ) | (command == "") | (np.invert(command.notnull()))
    command[mask] = processes.ix[mask].execCommand
    processes['command'] = command 

def identify_files(filenames):
    """Determine which mpi rank should parse which hdf5 files.

    Arguments:
    filenames -- list of absolute paths

    Returns:
    list [mpi.size length] of hashs which include in the 'file' key a
      list of files for the rank in to parse
    
    Note:  this function must be deterministic for a given set of inputs
    and mpi.size.  This is because all ranks will calculate this list, and
    all must arrive at the same answer.

    Note 2: this function must ensure that the earliest timestamped files
    are evaluated by the lowest-order ranks, and that the files are evaluated
    in order by rank.

    Note 3: the load balancing performed by this function could be improved.
    The size of the file should be proportional to the time taken to parse it,
    so, trying to optimize the size distribution amung the ranks is the planned
    improvement for load-balancing

    """
    global comm
    global rank
    global size

    used_ranks = size
    if len(filenames) < size:
        used_ranks = len(filenames)

    filesizes = [os.path.getsize(x) for x in filenames]
    tgt_size_per_rank = sum(filesizes) / used_ranks

    ## spread out files
    files_per_rank = len(filenames) / size
    n_ranks_with_one_extra = len(filenames) % size
    rank_files = []
    f = 0
    for i in xrange(size):
        t = {'file_idx': [], 'size': 0}
        files_to_add = files_per_rank
        if i < n_ranks_with_one_extra:
            files_to_add += 1
        for j in xrange(files_to_add):
            t['file_idx'].append(f)
            t['size'] += filesizes[f]
            f += 1
        rank_files.append(t)

    for r in rank_files:
        r['file'] = []
        for idx in r['file_idx']:
            r['file'].append(filenames[idx])

    print "rank %d files, resid %02.f, %0.2f bytes: ; %d files" % (rank,float(rank_files[rank]['size'] - tgt_size_per_rank)/1024**2, float(rank_files[rank]['size'])/1024**2, len(rank_files[rank]['file']))
    return rank_files

def identify_hosts(host_list, host_proc_cnt):
    """ Identify which hosts will be processed by each rank.

        @param host_list - master list of all hostnames in analysis
        @param host_proc_cnt - quantity of procdata records by host (this rank)

        This function is sent the master host_list (list of hostnames (str))
        which is the complete list of all hosts (was constructed via allgather
        in previous step).  This function also receives host_proc_cnt, a 1d 
        numpy array of the quantity of procdata records contained in this
        rank foreach host (ordered same as host_list).

        @returns 1d numpy array of number of hosts for each rank
    """

    global comm
    global rank
    global size

    ## if there are fewer hosts than ranks, we'll assign one host per rank and
    ## leave the higher numbered ranks idle, otherwise use them all
    used_ranks = size
    if len(host_list) < size:
        used_ranks = len(host_list)

    rank_host_cnt = np.zeros(dtype=np.int64, shape=used_ranks)
    rank_proc_cnt = np.zeros(dtype=np.int64, shape=used_ranks)
    totalprocs = sum(host_proc_cnt)
    goal_procs_per_rank = totalprocs / used_ranks

    ## load balancing by assigning a set of hosts per rank trying to optimize
    ## the same number of total processes per rank (the processes within each
    ## host)
    print "[%d] ranks: %d, hosts: %d, goal_processes_per_rank: %d" % (rank, used_ranks, len(host_list), goal_procs_per_rank)
    h_idx = 0
    for lrank in xrange(used_ranks):
        t = {'host': [], 'size': 0}
        done = False
        cnt = 0
        while not done and h_idx < len(host_list):
            proc_count = host_proc_cnt[h_idx]
            operand = proc_count if lrank < used_ranks/2 else 0
            done = cnt > 0 and rank_proc_cnt[lrank] + operand > goal_procs_per_rank
            if not done:
                rank_host_cnt[lrank] += 1
                rank_proc_cnt[lrank] += proc_count
                h_idx += 1
                cnt += 1
    lrank = used_ranks-1
    while h_idx < len(host_list):
        proc_count = host_proc_cnt[h_idx]
        rank_host_cnt[lrank] += 1
        rank_proc_cnt[lrank] += proc_count       
        h_idx += 1


    print "[%d] rank_hosts: " % rank, rank_host_cnt, rank_proc_cnt
    return rank_host_cnt

def divide_data(host_list, rank_hosts, host_processes, dataset):
    """Combine datasets into massive single arrays for alltoallv transmission.
       @param host_list - ordered list of hosts
       @param rank_hosts - list of how many hosts per rank (as ordered in host_list)
       @param host_processes - all the host records this process has collected

       This function needs to iterate through each dataset and construct
       a very large single array of each dataset, as well as an index indicating which
       records are for which host.
    """
    global comm
    global size
    global rank
    
    h_idx = 0
    retdata = [[],np.zeros(dtype=np.int64, shape=len(host_list))]
    while h_idx < len(host_list):
        host_data = None
        hostname = host_list[h_idx]
        if hostname in host_processes:
            host_data = host_processes[hostname]
        if host_data is not None:
            retdata[0].append(host_data.procmon[dataset])
            retdata[1][h_idx] = host_data.procmon[dataset].size
        else:
            retdata[1][h_idx] = 0
        h_idx += 1
   
    print "[%d] combining %d %s datasets" % (rank, len(retdata[0]), dataset)
    if len(retdata[0]) > 0:
        retdata[0] = np.concatenate(retdata[0])
    rank_row_counts = np.zeros(dtype=np.int64, shape=len(rank_hosts))
    cumsum = 0
    idx = 0
    while idx < len(rank_hosts):
        rank_row_counts[idx] = np.sum(retdata[1][cumsum:(cumsum+rank_hosts[idx])])
        cumsum += rank_hosts[idx]
        idx += 1
    retdata.append(rank_row_counts)

    return retdata

def mpi_transfer_datasets(root, host_list, rank_hosts, host_processes):
    global comm
    global rank
    global size

    complete_host_processes = {}
    for dataset in ['procdata','procstat','procobs','procfd']:
        print '[%d] getting ready to transmit %s' % (rank, dataset)
        data = divide_data(host_list, rank_hosts, host_processes, dataset)
        outbound_counts = data[2]

        np_type = None
        if rank == root:
            np_type = data[0].dtype

        np_type = comm.bcast(np_type, root=root)
        mpi_type = generate_mpi_type_simple(np_type)

        # if this process didn't read an hdf5 file, it won't have any data
        # to send, so put in some place-holder empties
        if len(data[0]) == 0:
            data[0] = np.zeros(shape=size, dtype=np_type)
            outbound_counts = np.array([1] * size, dtype=np.int64)

        data_recv_counts = np.zeros(shape=size, dtype=np.int64)
        ## transmit the counts via alltoall
        comm.Alltoallv([outbound_counts, [1]*size, range(size), MPI.LONG], [data_recv_counts, [1]*size, range(size), MPI.LONG])

        recv_data = np.zeros(shape=np.sum(data_recv_counts), dtype=np_type)
        outbound_offsets = np.hstack((0, np.cumsum(outbound_counts)))[0:-1]
        data_recv_offsets = np.hstack((0, np.cumsum(data_recv_counts)))[0:-1]
        print "[%d] data_recv_counts: "% rank, list(outbound_counts), list(data_recv_counts)
        print "[%d] data_recv_offsets: "% rank, list(outbound_offsets), list(data_recv_offsets)
        print "[%d] data: "%rank, data[0].size
        print "[%d] recv_data: "%rank, recv_data.size
        ## transmit the actual dataset
        comm.Alltoallv([data[0], outbound_counts, outbound_offsets, mpi_type], [recv_data, data_recv_counts, data_recv_offsets, mpi_type])

        ## prepare to send host-index of the data
        dset_idx = data[1]
        outbound_counts = rank_hosts
        outbound_offsets = np.hstack((0, np.cumsum(outbound_counts)))[0:-1]
        recv_counts = [outbound_counts[rank]]*size
        recv_offsets = np.hstack((0, np.cumsum(recv_counts)))[0:-1]
        recv_idx = np.zeros(shape=sum(recv_counts), dtype=np.int64)
        comm.Alltoallv([dset_idx, rank_hosts, outbound_offsets, MPI.LONG],[recv_idx, recv_counts, recv_offsets, MPI.LONG])

        ## reconstruct the per-host data structures
        dataset_base_idx = 0
        host_base_indices = np.hstack((0, np.cumsum(rank_hosts)))[0:-1]
        components = {}
        for rank_idx in xrange(len(rank_hosts)):
            if dataset_base_idx > data_recv_offsets[rank_idx]:
                print "WARNING! DANGER! It looks we over-ran into another rank's data! DANGER! WARNING!"
            if dataset_base_idx < data_recv_offsets[rank_idx]:
                print "INFO! USEFUL! Moving up %d records, hopefully accounting for data from an empty rank. USEFUL! INFO!" % (data_recv_offsets[rank_idx] - dataset_base_idx)
            dataset_base_idx = data_recv_offsets[rank_idx]
            for host_cnt in xrange(rank_hosts[rank]):
                host_idx = host_base_indices[rank] + host_cnt
                host = host_list[host_idx]
                if host not in complete_host_processes:
                    complete_host_processes[host] = {}
                if host not in components:
                    components[host] = []
                limit = recv_idx[ (rank_idx * rank_hosts[rank]) + host_cnt ]

                components[host].append( recv_data[dataset_base_idx:(dataset_base_idx + limit)] )
                dataset_base_idx += limit
        for host in complete_host_processes:
            complete_host_processes[host][dataset] = np.concatenate( components[host] )
        del recv_data
    return complete_host_processes

def get_processes(filenames, baseline_filenames, query, start_time, qqacct_data):
    status = 0
    processes = None
    start_time = int(start_time.strftime("%s"))

    global comm
    global size
    global rank

    host_processes = {}
    baseline_processes = {}
    rank_files = identify_files(filenames)
    filenames = rank_files[rank]['file']

    for filename in filenames:
        parse_h5(filename, host_processes, query)

    if rank == (size - 1):
        for filename in baseline_filenames:
            print "[%d] parsing baseline file: %s" % (rank, filename) 
            parse_h5(filename, baseline_processes, query)

    # build dictionary of hosts and known processes per host
    hosts = {}
    for x in host_processes:
        hosts[x] = host_processes[x].count_processes()

    print "[%d] beginning collective hostname comparison" % rank
    allhosts = comm.allgather(hosts)
        
    for i in range(size):
        if i == rank:
            continue
        for host in allhosts[i]:
            if host not in hosts:
                hosts[host] = allhosts[i][host]
            else:
                hosts[host] += allhosts[i][host]
    procsum = 0
    for host in hosts:
        procsum += hosts[host]

    ## screen out any hosts from the baseline data that aren't in the main
    ## dataset
    remove_list = []
    for host in baseline_processes:
        if host not in hosts:
            remove_list.append(host)
    for host in remove_list:
        del baseline_processes[host]

    print "[%d] got %d total hosts for %d processes" % (rank, len(hosts.keys()), procsum)

    # master list of hosts sorted alphabetically
    host_list = sorted(hosts.keys())

    # numpy array of process counts per host in this rank
    host_proc_cnt = np.array( [hosts[x] for x in host_list], dtype=np.int64 )
    rank_hosts = identify_hosts(host_list, host_proc_cnt)

    complete_host_processes = mpi_transfer_datasets(0, host_list, rank_hosts, host_processes)
    baseline_host_processes = mpi_transfer_datasets(size-1, host_list, rank_hosts, baseline_processes)
            
    del host_processes
    host_processes = {}
    print "[%d] reducing data" % (rank)
    for host in complete_host_processes:
        hostprocs = HostProcesses(host)
        hostprocs.reduce_data(complete_host_processes[host])
        host_processes[host] = hostprocs
    print "[%d] data reduction complete, starting baseline" % rank
    for host in host_processes:
        host_processes[host].set_baseline(baseline_host_processes[host], start_time)
    print "[%d] baselining complete" % rank

    for host in host_processes:
        processes = host_processes[host].procmon['processes']
        identify_scripts(processes)
        identify_userCommand(processes)
        identify_users(processes)
        host_processes[host].procmon['processes'] = integrate_job_data(processes, qqacct_data)

    ret_processes = {}
    hosts = host_processes.keys()
    for host in hosts:
        processes = host_processes[host].procmon['processes']
        np_type = host_processes[host].generate_nptype(processes)
        npprocs = np.zeros(shape=processes.shape[0], dtype=np_type)
        for col in processes.columns:
            coltype = npprocs[col].dtype
            try:
                if coltype in (np.uint8, np.uint16, np.uint32, np.uint64, np.int8, np.int16, np.int32, np.int64):
                    nandex = pandas.isnull(processes[col])
                    if sum(nandex) > 0:
                        processes[col][nandex] = 0
                npprocs[col] = processes[col].astype(npprocs[col].dtype, copy=True)
            except:
                print '[%d] %s issue setting column %s\n' % (rank, host, col), npprocs[col].dtype, processes[col].dtype
                print list(processes[col])
                print processes[col]
                traceback.print_exc(file=sys.stdout)
                exc_type, exc_value, exc_traceback = sys.exc_info()
                traceback.print_tb(exc_traceback, limit=1, file=sys.stdout)
                sys.exit(1)
                
        ret_processes[host] = npprocs
        del host_processes[host]

    return ret_processes

def identify_users(processes):
    uids = processes.realUid.unique()
    processes['username'] = 'unknown uid'
    user_hash = {}
    for uid in uids:
        uid_processes_idx = processes.realUid == uid
        try:
            uid = int(uid)
            if uid not in user_hash:
                user_hash[uid] = pwd.getpwuid(uid)
            processes.username[uid_processes_idx] = user_hash[uid].pw_name
        except:
            pass
    return user_hash

def integrate_job_data(processes, qqacct_data):
    if qqacct_data is None:
        processes['project'] = 'unknown project'
        return processes
    qqacct_data.job = qqacct_data.job.astype('str')
    qqacct_data.task[qqacct_data.task == 0] = 1
    qqacct_data.task = qqacct_data.task.astype('str')
    # get the last state for each job/task/hostname combination that was observed
    qqacct_data = qqacct_data.groupby(['job']).last().reset_index()
    processes = processes.merge(right=qqacct_data[['job','project']], how='left', left_on='identifier', right_on='job', suffixes=['','qq'])
    return processes
    
def summarize_processes(group):
    rowhash = {
        'cpu_time' : (group.utime + group.stime).sum() / 100.,
        'net_cpu_time': ((group.utime + group.stime) - (group.utime_baseline + group.stime_baseline)).sum() / 100.,
        'walltime' : (group.recTime - group.startTime).sum(),
        'net_walltime' : (group.recTime - group.startTime_baseline).sum(),
        'nobs'     : group.utime.count()
    }
    return pandas.Series(rowhash)

def summarize_data(processes, summaries = {
        'executables' : ['exePath'],
        'execUser' : ['username', 'exePath'],
        'execProject' : ['project', 'exePath'],
        'script' : ['script', 'exePath', 'execName'],
        'scriptUser' : ['username', 'script', 'exePath', 'execName'],
        'scriptProject' : ['project', 'script', 'exePath', 'execName'],
        'projects' : ['project'],
        'users' : ['username']
    }):

    ret = {}
    for summ_type in summaries:
        groups = processes.groupby(summaries[summ_type], as_index=False)
        summary = groups.apply(lambda x: summarize_processes(x))
        if type(summary) is pandas.core.series.Series:
            t_data = summary.to_dict()
            data = []
            data.extend(t_data.keys()[0][0:len(summaries[summ_type])])
            data.extend(t_data.values())
            keys = []
            keys.extend(summaries[summ_type])
            keys.extend(['cpu_time','walltime','nobs'])
            t_data = {}
            for (idx,key) in enumerate(keys):
                t_data[key] = data[idx]
            summary = pandas.DataFrame([t_data]).set_index(summaries[summ_type])
             
        ret[summ_type] = summary

    return ret

def usage(ret):
    print "procFinder.py [--h5-path <path=/global/projectb/statistics/procmon/genepool>] [--start YYYYMMDDHHMMSS] [--end YYYYMMDDHHMMSS] [--prefix <h5prefix=procmon_genepool>] [--save-all-processes] --qqacct-data <filename> --save-prefix <save_filename_prefix> <col> <regex> [<col> <regex> ...]"
    print "  Start time defaults to yesterday at Midnight (inclusive)"
    print "  End time defaults to today at Midnight (non-inclusive)"
    print "  Therefore, the default is to process all the files from yesterday!"
    sys.exit(ret)

def print_process(proc):
    print "%s,%s,%lu" % (row['username'],"%s,%s,%s" % (row['exePath'],row['execName'],row['cwdPath']),row['startTime'])

def main(args):
    ## initialize configurable variables
    yesterday = date.today() - timedelta(days=1)
    start_time = datetime.combine(yesterday, time(0,0,0))
    end_time = datetime.combine(date.today(), time(0,0,0))
    h5_path = "/global/projectb/statistics/procmon/genepool"
#h5_path = "/scratch/proc"
    h5_prefix = "procmon_genepool"
    save_all_processes = False
    qqacct_file = None
    save_prefix = None

    query = {}

    ## parse command line arguments
    i = 0
    while i < len(args):
        if args[i] == "--start":
            i += 1
            if i < len(args):
                try:
                    start_time = datetime.strptime(args[i], "%Y%m%d%H%M%S")
                except:
                    usage(1)
            else:
                usage(1)
        elif args[i] == "--end":
            i += 1
            if i < len(args):
                try:
                    end_time = datetime.strptime(args[i], "%Y%m%d%H%M%S")
                except:
                    usage(1)
            else:
                usage(1)
        elif args[i] == "--qqacct-data":
            i += 1
            if i < len(args):
                qqacct_file = args[i]
                if not os.path.exists(qqacct_file):
                    qqacct_file = None
            else:
                usage(1)
        elif args[i] == "--h5-path":
            i += 1
            if i < len(args):
                h5_path = args[i]
                if not os.path.exists(h5_path):
                    print "%s doesn't exist!" % (h5_path)
                    usage(1)
            else:
                usage(1)
        elif args[i] == "--h5-prefix":
            i += 1
            if i < len(args):
                h5_prefix = args[i]
            else:
                usage(1)
        elif args[i] == "--save-prefix":
            i += 1
            if i < len(args):
                save_prefix = args[i]
            else:
                usage(1)
        elif args[i] == "--save-all-processes":
            save_all_processes = True
        elif args[i] == "--help":
            usage(0)
        else:
            key = args[i]
            value = None
            i += 1
            if i < len(args):
                value = args[i]
                if key not in query:
                    query[key] = []
                query[key].append(value)
        i += 1

    procmon_h5cache = procmon.H5Cache.H5Cache(h5_path, h5_prefix)
    filenames = [ x['path'] for x in procmon_h5cache.query(start_time, end_time) ]
    baseline_filenames = [ x['path'] for x in procmon_h5cache.query(start_time - timedelta(minutes=20), start_time - timedelta(minutes=1)) ]

    filenames = sorted(filenames)
    baseline_filenames = sorted(baseline_filenames)

    if len(filenames) == 0:
        print "procmon data files could not be found"
        sys.exit(1)

    #if qqacct_file is None:
    #    print "qqacct data file must be specified"
    #    sys.exit(1)

    if save_prefix is None:
        print "must specify a save_prefix!"
        sys.exit(1)

    q_status = 0
    h_status = 0
    user_hash = None
    summaries = None

    global comm
    global rank
    qqacct_data = None
    if rank == 0 and qqacct_file is not None:
        (q_status, qqacct_data) =  get_job_data(start_time,end_time, qqacct_file)
    print "distributing qqacct data"
    qqacct_data = comm.bcast(qqacct_data, root=0)
    print "getting process data (may take a long time):"
    host_processes = get_processes(filenames, baseline_filenames, query, start_time, qqacct_data)

    local_processes = 0
    for host in host_processes:
        local_processes += host_processes[host].size
    process_cnts = comm.allgather(local_processes)

    print "[%d] about to begin writing processes in parallel (%d global, %d local)" % (rank, sum(process_cnts), local_processes)
    output = h5py.File('/global/scratch2/sd/dmj/output.h5', 'w', driver='mpio', comm=comm)
    dset = output.create_dataset('processes', (sum(process_cnts),), dtype=host_processes.values()[0].dtype)
    offset = sum(process_cnts[0:rank])
    local_idx = 0
    for host in host_processes:
        base = offset + local_idx
        limit = base + host_processes[host].size
        local_idx += host_processes[host].size

        dset[base:limit] = host_processes[host][:]
    output.close()
        

    sys.exit(0)
    processes = None
    summaries = None
    if processes is not None and not processes.empty:
        identify_scripts(processes)
        identify_userCommand(processes)
        identify_users(processes)
        summ_index = {
            'command' : ['command', 'execCommand'],
            'commandUser' : ['username','command','execCommand'],
            'commandProject' : ['project','command','execCommand'],
            'commandHost' : ['host','command','execCommand'],
            'executables' : ['exePath'],
            'execUser' : ['username', 'exePath'],
            'execProject' : ['project', 'exePath'],
            'execHost' : ['host','exePath'],
            'scripts' : ['script', 'exePath', 'execName'],
            'scriptUser' : ['username', 'script', 'exePath', 'execName'],
            'scriptProject' : ['project', 'script', 'exePath', 'execName'],
            'scriptHost' : ['host','script','exePath','execName'],
            'projects' : ['project'],
            'users' : ['username'],
            'hosts' : ['host'],
        }
        summaries = summarize_data(processes, summ_index)
    all_summaries = comm.gather(summaries, root=0)
    if rank == 0:
        summ_list = {}
        final_summaries = {}
        write_summary_debug = False

        for l_rank in xrange(size):
            if all_summaries[l_rank] is None:
                continue
            for key in all_summaries[l_rank]:
                if key not in summ_list:
                    summ_list[key] = []
                l_summary = all_summaries[l_rank][key]
                summ_list[key].append(l_summary)

        if write_summary_debug:
            cPickle.dump(all_summaries, open('%s.summ_list.debug.pk' % save_prefix,'wb'))
            write_summary_debug = False
            pass

        summary_save_file = '%s.summary.h5' % save_prefix
        for key in summ_list:
            summary = None
            if len(summ_list[key]) > 0:
                summary = pandas.concat(summ_list[key], axis=0)

            if summary is not None:
                nLevel=0
                if type(summary.index) is pandas.core.index.MultiIndex:
                    nLevel = range(len(summary.index.levels))
                else:
                    print "%s: got different type: %s" % (key, str(type(summary.index)))
                summary = summary.groupby(level=nLevel).sum()
                summary.to_hdf(summary_save_file,key)

        if write_summary_debug:
            cPickle.dump(summ_list, open('%s.summ_list.debug2.pk' % save_prefix, 'wb'))
    
    if save_all_processes:
        all_processes = None
        print "[%d] about to send %s processes" % (rank, processes.shape if processes is not None else "None")
        try:
            all_processes = comm.gather(processes, root=0)
        except:
            pass
        if rank == 0 and all_processes is not None:
            processes = pandas.concat(all_processes, axis=0)
            processes.to_hdf('%s.processes.h5' % save_prefix, 'processes')


if __name__ == "__main__":
    main(sys.argv[1:])
    pass
