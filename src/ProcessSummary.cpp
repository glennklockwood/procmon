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

#include "ProcessSummary.hh"
#include "ProcIO2.hh"
#include <string.h>

ProcessSummary::ProcessSummary() {
    memset(this, 0, sizeof(ProcessSummary));
}
ProcessSummary::ProcessSummary(const string& _hostname, const string& _identifier, const string& _subidentifier, unsigned long _start, int _pid) {
    memset(this, 0, sizeof(ProcessSummary));
    snprintf(identifier, IDENTIFIER_SIZE, "%s", _identifier.c_str());
    snprintf(subidentifier, IDENTIFIER_SIZE, "%s", _subidentifier.c_str());
    snprintf(host, EXEBUFFER_SIZE, "%s", _hostname.c_str());
    startTime = _start;
    pid = _pid;
}

namespace pmio2 {
template <>
hid_t Hdf5Type<IdentifiedFilesystem>::initializeType(shared_ptr<Hdf5Io> file) {
    hid_t h5type = H5Tcreate(H5T_COMPOUND, sizeof(IdentifiedFilesystem));
    H5Tinsert(h5type, "identifier",    HOFFSET(IdentifiedFilesystem, identifier),    file->strType_idBuffer);
    H5Tinsert(h5type, "subidentifier", HOFFSET(IdentifiedFilesystem, subidentifier), file->strType_idBuffer);
    H5Tinsert(h5type, "startTime",     HOFFSET(IdentifiedFilesystem, startTime),     H5T_NATIVE_ULONG);
    H5Tinsert(h5type, "startTimeUSec", HOFFSET(IdentifiedFilesystem, startTimeUSec), H5T_NATIVE_ULONG);
    H5Tinsert(h5type, "pid",           HOFFSET(IdentifiedFilesystem, pid),           H5T_NATIVE_UINT);
    H5Tinsert(h5type, "command",       HOFFSET(IdentifiedFilesystem, command),       file->strType_buffer);
    H5Tinsert(h5type, "hostname",      HOFFSET(IdentifiedFilesystem, host),          file->strType_exeBuffer);
    H5Tinsert(h5type, "filesystem",    HOFFSET(IdentifiedFilesystem, filesystem),    file->strType_buffer);
    H5Tinsert(h5type, "read",          HOFFSET(IdentifiedFilesystem, read),          H5T_NATIVE_UINT);
    H5Tinsert(h5type, "write",         HOFFSET(IdentifiedFilesystem, write),         H5T_NATIVE_UINT);
    H5Tinsert(h5type, "user",          HOFFSET(IdentifiedFilesystem, user),          file->strType_exeBuffer);
    H5Tinsert(h5type, "project",       HOFFSET(IdentifiedFilesystem, project),       file->strType_exeBuffer);
    return h5type;
}

template<>
hid_t Hdf5Type<IdentifiedNetworkConnection>::initializeType(shared_ptr<Hdf5Io> file) {
    hid_t h5type = H5Tcreate(H5T_COMPOUND, sizeof(IdentifiedNetworkConnection));
    H5Tinsert(h5type, "identifier",    HOFFSET(IdentifiedNetworkConnection, identifier),    file->strType_idBuffer);
    H5Tinsert(h5type, "subidentifier", HOFFSET(IdentifiedNetworkConnection, subidentifier), file->strType_idBuffer);
    H5Tinsert(h5type, "startTime",     HOFFSET(IdentifiedNetworkConnection, startTime),     H5T_NATIVE_ULONG);
    H5Tinsert(h5type, "startTimeUSec", HOFFSET(IdentifiedNetworkConnection, startTimeUSec), H5T_NATIVE_ULONG);
    H5Tinsert(h5type, "pid",           HOFFSET(IdentifiedNetworkConnection, pid),           H5T_NATIVE_UINT);
    H5Tinsert(h5type, "command",       HOFFSET(IdentifiedNetworkConnection, command),       file->strType_buffer);
    H5Tinsert(h5type, "protocol",      HOFFSET(IdentifiedNetworkConnection, protocol),      file->strType_idBuffer);
    H5Tinsert(h5type, "remoteAddress", HOFFSET(IdentifiedNetworkConnection, remoteAddress), file->strType_exeBuffer);
    H5Tinsert(h5type, "remotePort",    HOFFSET(IdentifiedNetworkConnection, remotePort),    H5T_NATIVE_INT);
    H5Tinsert(h5type, "localAddress",  HOFFSET(IdentifiedNetworkConnection, localAddress),  file->strType_exeBuffer);
    H5Tinsert(h5type, "localPort",     HOFFSET(IdentifiedNetworkConnection, localPort),     H5T_NATIVE_INT);
    H5Tinsert(h5type, "user",          HOFFSET(IdentifiedNetworkConnection, user),          file->strType_exeBuffer);
    H5Tinsert(h5type, "project",       HOFFSET(IdentifiedNetworkConnection, project),       file->strType_exeBuffer);
    return h5type;
}

template <>
hid_t Hdf5Type<ProcessSummary>::initializeType(shared_ptr<Hdf5Io> file) {
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
    H5Tinsert(h5type, "cpuTime_net", HOFFSET(ProcessSummary, cpuTime_net), H5T_NATIVE_DOUBLE);
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
}

Scriptable *Scriptable::getScriptable(const char *exePath, const char *cmdArgs) {
    const char *execName = exePath;
    const char *last_slash = strrchr(exePath, '/');
    if (last_slash != NULL) execName = last_slash + 1;
    if (strcmp(execName, "java") == 0) {
        return new JavaScriptable(exePath, cmdArgs);
    } else if (strncmp(execName, "python", 6) == 0) {
        return new PythonScriptable(exePath, cmdArgs);
    } else if (strncmp(execName, "perl", 4) == 0) {
        return new PerlScriptable(exePath, cmdArgs);
    } else if (strncmp(execName, "ruby", 4) == 0) {
        return new RubyScriptable(exePath, cmdArgs);
    } else if (strncmp(execName, "sh", 2) == 0 || strncmp(execName, "bash", 4) == 0) {
        return new BashScriptable(exePath, cmdArgs);
    } else if (strncmp(execName, "csh", 3) == 0 || strncmp(execName, "tcsh", 4) == 0) {
        return new CshScriptable(exePath, cmdArgs);
    }
    return NULL;
}
