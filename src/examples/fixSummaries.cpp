/*******************************************************************************
procmon, Copyright (c) 2014, The Regents of the University of California,
through Lawrence Berkeley National Laboratory (subject to receipt of any
required approvals from the U.S. Dept. of Energy).  All rights reserved.

If you have questions about your rights to use or distribute this software,
please contact Berkeley Lab's Technology Transfer Department at  TTD@lbl.gov.

The LICENSE file in the root directory of the source code archive describes the
licensing and distribution rights and restrictions on this software.

Author:   Douglas Jacobsen <dmj@nersc.gov>
*******************************************************************************/

#include "ProcIO2.hh"
#include "ProcessSummary.hh"

#include <algorithm>
#include <signal.h>
#include <string.h>
#include <iostream>
#include <fstream>
#include <deque>
#include <unordered_map>

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <pwd.h>

#include <assert.h>
#define PROCMON_SUMMARIZE_VERSION 2.0

using namespace std;

template <class pmType>
struct myTypeFactory : pmio2::Hdf5TypeFactory<pmType> {
    hid_t operator()(shared_ptr<hdf5Io> file) {
        return -1;
    }
}

template <>
struct myTypeFactory<ProcessSummary> {
    hid_t operator()(shared_ptr<Hdf5Io> file) {
        hid_t h5type = H5Tcreate(H5T_COMPOUND, sizeof(ProcessSummary));
        H5Tinsert(h5type, "identifier",    HOFFSET(ProcessSummary, identifier),    file->strType_idBuffer);
        H5Tinsert(h5type, "subidentifier", HOFFSET(ProcessSummary, subidentifier), file->strType_idBuffer);
        H5Tinsert(h5type, "startTime",     HOFFSET(ProcessSummary, startTime),     H5T_NATIVE_ULONG);
        H5Tinsert(h5type, "startTimeUSec", HOFFSET(ProcessSummary, startTimeUSec), H5T_NATIVE_ULONG);
        H5Tinsert(h5type, "recTime", HOFFSET(ProcessSummary, recTime), H5T_NATIVE_ULONG);
        H5Tinsert(h5type, "recTimeUSec", HOFFSET(ProcessSummary, recTimeUSec), H5T_NATIVE_ULONG);
        H5Tinsert(h5type, "pid",           HOFFSET(ProcessSummary, pid),           H5T_NATIVE_UINT);
        H5Tinsert(h5type, "hostname", HOFFSET(ProcessSummary, host), file->strType_exeBuffer);

        H5Tinsert(h5type, "execName", HOFFSET(ProcessSummary, execName), file->strType_exeBuffer);
        H5Tinsert(h5type, "cmdArgBytes", HOFFSET(ProcessSummary, cmdArgBytes), H5T_NATIVE_ULONG);
        H5Tinsert(h5type, "cmdArgs", HOFFSET(ProcessSummary, cmdArgs), file->strType_buffer);
        H5Tinsert(h5type, "exePath", HOFFSET(ProcessSummary, exePath), file->strType_buffer);
        H5Tinsert(h5type, "cwdPath", HOFFSET(ProcessSummary, cwdPath), file->strType_buffer);
        H5Tinsert(h5type, "ppid", HOFFSET(ProcessSummary, ppid), H5T_NATIVE_UINT);
        H5Tinsert(h5type, "state", HOFFSET(ProcessSummary, state), H5T_NATIVE_CHAR);
        H5Tinsert(h5type, "pgrp", HOFFSET(ProcessSummary, pgrp), H5T_NATIVE_INT);
        H5Tinsert(h5type, "session", HOFFSET(ProcessSummary, session), H5T_NATIVE_INT);
        H5Tinsert(h5type, "tty", HOFFSET(ProcessSummary, tty), H5T_NATIVE_INT);
        H5Tinsert(h5type, "tpgid", HOFFSET(ProcessSummary, tpgid), H5T_NATIVE_INT);
        H5Tinsert(h5type, "realUid", HOFFSET(ProcessSummary, realUid), H5T_NATIVE_ULONG);
        H5Tinsert(h5type, "effUid", HOFFSET(ProcessSummary, effUid), H5T_NATIVE_ULONG);
        H5Tinsert(h5type, "realGid", HOFFSET(ProcessSummary, realGid), H5T_NATIVE_ULONG);
        H5Tinsert(h5type, "effGid", HOFFSET(ProcessSummary, effGid), H5T_NATIVE_ULONG);
        H5Tinsert(h5type, "flags", HOFFSET(ProcessSummary, flags), H5T_NATIVE_UINT);
        H5Tinsert(h5type, "utime", HOFFSET(ProcessSummary, utime), H5T_NATIVE_ULONG);
        H5Tinsert(h5type, "stime", HOFFSET(ProcessSummary, stime), H5T_NATIVE_ULONG);
        H5Tinsert(h5type, "priority", HOFFSET(ProcessSummary, priority), H5T_NATIVE_LONG);
        H5Tinsert(h5type, "nice", HOFFSET(ProcessSummary, nice), H5T_NATIVE_LONG);
        H5Tinsert(h5type, "numThreads", HOFFSET(ProcessSummary, numThreads), H5T_NATIVE_LONG);
        H5Tinsert(h5type, "vsize", HOFFSET(ProcessSummary, vsize), H5T_NATIVE_ULONG);
        H5Tinsert(h5type, "rss", HOFFSET(ProcessSummary, rss), H5T_NATIVE_ULONG);
        H5Tinsert(h5type, "rsslim", HOFFSET(ProcessSummary, rsslim), H5T_NATIVE_ULONG);
        H5Tinsert(h5type, "signal", HOFFSET(ProcessSummary, signal), H5T_NATIVE_ULONG);
        H5Tinsert(h5type, "blocked", HOFFSET(ProcessSummary, blocked), H5T_NATIVE_ULONG);
        H5Tinsert(h5type, "sigignore", HOFFSET(ProcessSummary, sigignore), H5T_NATIVE_ULONG);
        H5Tinsert(h5type, "sigcatch", HOFFSET(ProcessSummary, sigcatch), H5T_NATIVE_ULONG);
        H5Tinsert(h5type, "rtPriority", HOFFSET(ProcessSummary, rtPriority), H5T_NATIVE_UINT);
        H5Tinsert(h5type, "policy", HOFFSET(ProcessSummary, policy), H5T_NATIVE_UINT);
        H5Tinsert(h5type, "delayacctBlkIOTicks", HOFFSET(ProcessSummary, delayacctBlkIOTicks), H5T_NATIVE_ULLONG);
        H5Tinsert(h5type, "guestTime", HOFFSET(ProcessSummary, guestTime), H5T_NATIVE_ULONG);
        H5Tinsert(h5type, "vmpeak", HOFFSET(ProcessSummary, vmpeak), H5T_NATIVE_ULONG);
        H5Tinsert(h5type, "rsspeak", HOFFSET(ProcessSummary, rsspeak), H5T_NATIVE_ULONG);
        H5Tinsert(h5type, "cpusAllowed", HOFFSET(ProcessSummary, cpusAllowed), H5T_NATIVE_INT);
        H5Tinsert(h5type, "io_rchar", HOFFSET(ProcessSummary, io_rchar), H5T_NATIVE_ULLONG);
        H5Tinsert(h5type, "io_wchar", HOFFSET(ProcessSummary, io_wchar), H5T_NATIVE_ULLONG);
        H5Tinsert(h5type, "io_syscr", HOFFSET(ProcessSummary, io_syscr), H5T_NATIVE_ULLONG);
        H5Tinsert(h5type, "io_syscw", HOFFSET(ProcessSummary, io_syscw), H5T_NATIVE_ULLONG);
        H5Tinsert(h5type, "io_readBytes", HOFFSET(ProcessSummary, io_readBytes), H5T_NATIVE_ULLONG);
        H5Tinsert(h5type, "io_writeBytes", HOFFSET(ProcessSummary, io_writeBytes), H5T_NATIVE_ULLONG);
        H5Tinsert(h5type, "io_cancelledWriteBytes", HOFFSET(ProcessSummary, io_cancelledWriteBytes), H5T_NATIVE_ULLONG);
        H5Tinsert(h5type, "m_size", HOFFSET(ProcessSummary, m_size), H5T_NATIVE_ULONG);
        H5Tinsert(h5type, "m_resident", HOFFSET(ProcessSummary, m_resident), H5T_NATIVE_ULONG);
        H5Tinsert(h5type, "m_share", HOFFSET(ProcessSummary, m_share), H5T_NATIVE_ULONG);
        H5Tinsert(h5type, "m_text", HOFFSET(ProcessSummary, m_text), H5T_NATIVE_ULONG);
        H5Tinsert(h5type, "m_data", HOFFSET(ProcessSummary, m_data), H5T_NATIVE_ULONG);

        H5Tinsert(h5type, "nObservations", HOFFSET(ProcessSummary, nObservations), H5T_NATIVE_ULONG);
        H5Tinsert(h5type, "nRecords", HOFFSET(ProcessSummary, nRecords), H5T_NATIVE_ULONG);
        H5Tinsert(h5type, "isParent", HOFFSET(ProcessSummary, isParent), H5T_NATIVE_INT);
        H5Tinsert(h5type, "command", HOFFSET(ProcessSummary, command), file->strType_buffer);
        H5Tinsert(h5type, "execCommand", HOFFSET(ProcessSummary, execCommand), file->strType_buffer);
        H5Tinsert(h5type, "script", HOFFSET(ProcessSummary, script), file->strType_buffer);
        H5Tinsert(h5type, "user", HOFFSET(ProcessSummary, user), file->strType_exeBuffer);
        H5Tinsert(h5type, "project", HOFFSET(ProcessSummary, project), file->strType_exeBuffer);
        H5Tinsert(h5type, "derived_startTime", HOFFSET(ProcessSummary, derived_startTime), H5T_NATIVE_DOUBLE);
        H5Tinsert(h5type, "derived_recTime", HOFFSET(ProcessSummary, derived_recTime), H5T_NATIVE_DOUBLE);
        H5Tinsert(h5type, "baseline_startTime", HOFFSET(ProcessSummary, baseline_startTime), H5T_NATIVE_DOUBLE);
        H5Tinsert(h5type, "orig_startTime", HOFFSET(ProcessSummary, orig_startTime), H5T_NATIVE_DOUBLE);
        H5Tinsert(h5type, "volatilityScore", HOFFSET(ProcessSummary, volatilityScore), H5T_NATIVE_DOUBLE);
        H5Tinsert(h5type, "cpuTime", HOFFSET(ProcessSummary, cpuTime), H5T_NATIVE_DOUBLE);
        H5Tinsert(h5type, "utime_net", HOFFSET(ProcessSummary, utime_net), H5T_NATIVE_ULONG);
        H5Tinsert(h5type, "stime_net", HOFFSET(ProcessSummary, stime_net), H5T_NATIVE_ULONG);
        H5Tinsert(h5type, "io_rchar_net", HOFFSET(ProcessSummary, io_rchar_net), H5T_NATIVE_ULLONG);
        H5Tinsert(h5type, "io_wchar_net", HOFFSET(ProcessSummary, io_wchar_net), H5T_NATIVE_ULLONG);
        H5Tinsert(h5type, "duration", HOFFSET(ProcessSummary, duration), H5T_NATIVE_DOUBLE);
        H5Tinsert(h5type, "cpuRateMax", HOFFSET(ProcessSummary, cpuRateMax), H5T_NATIVE_DOUBLE);
        H5Tinsert(h5type, "iorRateMax", HOFFSET(ProcessSummary, iorRateMax), H5T_NATIVE_DOUBLE);
        H5Tinsert(h5type, "iowRateMax", HOFFSET(ProcessSummary, iowRateMax), H5T_NATIVE_DOUBLE);
        H5Tinsert(h5type, "msizeRateMax", HOFFSET(ProcessSummary, msizeRateMax), H5T_NATIVE_DOUBLE);
        H5Tinsert(h5type, "mresidentRateMax", HOFFSET(ProcessSummary, mresidentRateMax), H5T_NATIVE_DOUBLE);
        H5Tinsert(h5type, "cov_cpuXiow", HOFFSET(ProcessSummary, cov_cpuXiow), H5T_NATIVE_DOUBLE);
        H5Tinsert(h5type, "cov_cpuXior", HOFFSET(ProcessSummary, cov_cpuXior), H5T_NATIVE_DOUBLE);
        H5Tinsert(h5type, "cov_cpuXmsize", HOFFSET(ProcessSummary, cov_cpuXmsize), H5T_NATIVE_DOUBLE);
        H5Tinsert(h5type, "cov_cpuXmresident", HOFFSET(ProcessSummary, cov_cpuXmresident), H5T_NATIVE_DOUBLE);
        H5Tinsert(h5type, "cov_iowXior", HOFFSET(ProcessSummary, cov_iowXior), H5T_NATIVE_DOUBLE);
        H5Tinsert(h5type, "cov_iowXmsize", HOFFSET(ProcessSummary, cov_iowXmsize), H5T_NATIVE_DOUBLE);
        H5Tinsert(h5type, "cov_iowXmresident", HOFFSET(ProcessSummary, cov_iowXmresident), H5T_NATIVE_DOUBLE);
        H5Tinsert(h5type, "cov_iorXmsize", HOFFSET(ProcessSummary, cov_iorXmsize), H5T_NATIVE_DOUBLE);
        H5Tinsert(h5type, "cov_iorXmresident", HOFFSET(ProcessSummary, cov_iorXmresident), H5T_NATIVE_DOUBLE);
        H5Tinsert(h5type, "cov_msizeXmresident", HOFFSET(ProcessSummary, cov_msizeXmresident), H5T_NATIVE_DOUBLE);
        return h5type;
    }
};
}

template <class pmType>
struct Writer {
    size_t operator()(shared_ptr<pmio2::Hdf5Io> output, const string &dsName,  pmType *buffer, const size_t n) {
        return output->write(dsName, buffer, &(buffer[n]));
    }
};

template <>
struct Writer<ProcessSummary> {
    size_t operator()(shared_ptr<pmio2::Hdf5Io> output, const string &dsName, ProcessSummary *buffer, const size_t n) {
        cout << "updating cputime" << endl;
        for (size_t idx = 0; idx < n; ++idx) {
            buffer[idx].cpuTime_net = (buffer[idx].utime_net + buffer[idx].stime_net) / 100.;
        }
        cout << "about to write" << endl;
        return output->write<ProcessSummary>(dsName, buffer, &(buffer[n]));
    }
};

template <class pmType>
struct Collector {
    int obs;
    Collector() {
        obs = 0;
    }
    void operator()(pmType *buffer, const size_t n) {
        this->obs += n;
    }
};

template <>
struct Collector<ProcessSummary> {
    int obs;
    Collector() {
        obs = 0;
    }
    void operator()(ProcessSummary *buffer, const size_t n) {
        for (size_t idx = 0; idx < n; ++idx) {
            obs += buffer[idx].nObservations;
        }
        cout << "obs " << obs << " (" << n << ") " << endl;
    }
};

template <>
struct Collector<IdentifiedFilesystem> {
    int obs;
    Collector() {
        obs = 0;
    }
    void operator()(IdentifiedFilesystem *buffer, const size_t n) {
        for (size_t idx = 0; idx < n; ++idx) {
            obs += buffer[idx].read + buffer[idx].write;
        }
    }
};



template <class pmType>
void copyDataset(size_t n, const size_t nRead, shared_ptr<pmio2::Hdf5Io> input, shared_ptr<pmio2::Hdf5Io> output, const string& dsName, Collector<pmType>& collector, Writer<pmType>& writer) {
    cout << "starting copy of " << dsName << endl;
    pmType *buffer = new pmType[nRead];
    size_t start_idx = 0;
    size_t count = 0;
    while (n > 0) {
        memset(buffer, 0, sizeof(pmType) * nRead);
        size_t l_read = n > nRead ? nRead : n;
        n -= l_read;
        cout << "about to read " << l_read << " (" << (count + l_read) << ")" << endl;
        input->read(dsName, buffer, l_read, start_idx);
        start_idx += l_read;
        cout << "about to call collector: " << l_read << endl;
        collector(buffer, l_read);
        cout << buffer[0].identifier << endl;
        writer(output, dsName, buffer, l_read);
        cout << "done writing" << endl;
        count += l_read;
    }
    delete[] buffer;
}


int main(int argc, char **argv) {
    for (int idx = 0; idx < argc; ++idx) {
        cout << "arg" << idx << ": " << argv[idx] << endl;
    }
    if (argc < 3) {
        cout << "not enough args" << endl;
        exit(1);
    }
    string input_filename(argv[1]);
    string output_filename(argv[2]);

    pmio2::Hdf5TypeFactory<ProcessSummary> damagedTypeInitializer;

    /* open input h5 files, setup datasets */
    shared_ptr<pmio2::Hdf5Io> input = make_shared<pmio2::Hdf5Io>(input_filename, pmio2::IoMode::MODE_READ);
    cout << input->addDataset("ProcessSummary",
        make_shared<pmio2::Hdf5DatasetFactory<ProcessSummary> >(
            input,
            make_shared<pmio2::Hdf5Type<ProcessSummary> >(input, damagedTypeInitializer),
            0, 4096, 0, "ProcessSummary"
        )
    );
    cout << input->addDataset("IdentifiedFilesystem",
        make_shared<pmio2::Hdf5DatasetFactory<IdentifiedFilesystem> >(
            input,
            make_shared<pmio2::Hdf5Type<IdentifiedFilesystem> >(input),
            0,
            4096,
            0,
            "IdentifiedFilesystem"
        )
    );
    cout <<input->addDataset("IdentifiedNetworkConnection",
        make_shared<pmio2::Hdf5DatasetFactory<IdentifiedNetworkConnection> >(
            input,
            make_shared<pmio2::Hdf5Type<IdentifiedNetworkConnection> >(input),
            0,
            4096,
            0,
            "IdentifiedNetworkConnection"
        )
    );
    cout << endl;

    pmio2::Context context("genepool", "processes", "*", "*");
    input->setContext(context);

    size_t n_summary = input->howmany<ProcessSummary>("ProcessSummary");
    size_t n_fs = input->howmany<IdentifiedFilesystem>("IdentifiedFilesystem");
    size_t n_net = input->howmany<IdentifiedNetworkConnection>("IdentifiedNetworkConnection");


    cout << "n summary: " << n_summary << endl;
    cout << "n fs:      " << n_fs << endl;
    cout << "n net:     " << n_net << endl;



    shared_ptr<pmio2::Hdf5Io> output = make_shared<pmio2::Hdf5Io>(output_filename, pmio2::IoMode::MODE_WRITE);
    output->addDataset("ProcessSummary",
        make_shared<pmio2::Hdf5DatasetFactory<ProcessSummary> >(
            output,
            make_shared<pmio2::Hdf5Type<ProcessSummary> >(output),
            0, // unlimited max size
            4096, // 256 processes per block
            5,  // zipLevel 9 (highest)
            "ProcessSummary" // datasetName
        )
    );    
    output->addDataset("IdentifiedFilesystem",
        make_shared<pmio2::Hdf5DatasetFactory<IdentifiedFilesystem> >(
            output,
            make_shared<pmio2::Hdf5Type<IdentifiedFilesystem> >(output),
            0,
            4096,
            5,
            "IdentifiedFilesystem"
        )
    );
    output->addDataset("IdentifiedNetworkConnection",
        make_shared<pmio2::Hdf5DatasetFactory<IdentifiedNetworkConnection> >(
            output,
            make_shared<pmio2::Hdf5Type<IdentifiedNetworkConnection> >(output),
            0,
            4096,
            5,
            "IdentifiedNetworkConnection"
        )
    );
    output->setContext(context);
    output->setContextOverride(true);

    const int nRead = 100000;
    Writer<ProcessSummary> summ_writer;
    Collector<ProcessSummary> summ_stats;
    copyDataset<ProcessSummary>(n_summary, nRead, input, output, "ProcessSummary", summ_stats, summ_writer);

    Writer<IdentifiedFilesystem> fs_writer;
    Collector<IdentifiedFilesystem> fs_stats;
    copyDataset<IdentifiedFilesystem>(n_fs, nRead, input, output, "IdentifiedFilesystem", fs_stats, fs_writer);

    Writer<IdentifiedNetworkConnection> net_writer;
    Collector<IdentifiedNetworkConnection> net_stats;
    copyDataset<IdentifiedNetworkConnection>(n_net, nRead, input, output, "IdentifiedNetworkConnection", net_stats, net_writer);

    cout << "processObservations: " << summ_stats.obs << endl;
    cout << "filesystemObersvations: " << fs_stats.obs << endl;
    cout << "networkObservations: " << net_stats.obs << endl;

    return 0;
}
