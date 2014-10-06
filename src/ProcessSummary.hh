#ifndef __PROCESSSUMMARY_HH
#define __PROCESSSUMMARY_HH

#include "ProcData.hh"
#include <boost/tokenizer.hpp>
#include <algorithm> 
#include <functional> 
#include <cctype>
#include <locale>
#include <array>
#include <string>
#include <ctype.h>
#include <string.h>
#include <hdf5.h>
#include <ProcIO.hh>

using namespace std;
typedef boost::tokenizer<boost::char_separator<char> > tokenizer;

struct IdentifiedFilesystems;



struct ProcessSummary {
    public:

    ProcessSummary();
    ProcessSummary(const string& _hostname, const string& _identifier, const string& _subidentifier, time_t _startTime, int _pid) {
        strncpy(host, _hostname.c_str(), EXEBUFFER_SIZE);
        strncpy(identifier, _identifier.c_str(), IDENTIFIER_SIZE);
        strncpy(subidentifier, _subidentifier.c_str(), IDENTIFIER_SIZE);
        startTime = _startTime;
        pid = _pid;
    }

    char identifier[IDENTIFIER_SIZE];
    char subidentifier[IDENTIFIER_SIZE];
    unsigned long recTime;
    unsigned long recTimeUSec;
    unsigned long startTime;
    unsigned long startTimeUSec;
    int pid;

    vector<IdentifiedFilesystems> identifiedFilesystems;

    /* from procdata */
    char execName[EXEBUFFER_SIZE];
    unsigned long cmdArgBytes;
    char cmdArgs[BUFFER_SIZE];
    char exePath[BUFFER_SIZE];
    char cwdPath[BUFFER_SIZE];
    unsigned int ppid;

    /* from procstat */
    char state;
    int pgrp;
    int session;
    int tty;
    int tpgid;
    unsigned long realUid;
    unsigned long effUid;
    unsigned long realGid;
    unsigned long effGid;
    unsigned int flags;
    unsigned long utime;
    unsigned long stime;
    long priority;
    long nice;
    long numThreads;
    unsigned long vsize; /* virtual mem in bytes */
    unsigned long rss;   /* number of pages in physical memory */
    unsigned long rsslim;/* limit of rss bytes */
    unsigned long signal;
    unsigned long blocked;
    unsigned long sigignore;
    unsigned long sigcatch;
    unsigned int rtPriority;
    unsigned int policy;
    unsigned long long delayacctBlkIOTicks;
    unsigned long guestTime;

    /* fields from /proc/[pid]/status */
    unsigned long vmpeak;  /* kB */
    unsigned long rsspeak; /* kB */
    int cpusAllowed;

    /* fields from /proc/[pid]/io */
    unsigned long long io_rchar;
    unsigned long long io_wchar;
    unsigned long long io_syscr;
    unsigned long long io_syscw;
    unsigned long long io_readBytes;
    unsigned long long io_writeBytes;
    unsigned long long io_cancelledWriteBytes;

    /* fields from /proc/[pid]/statm */
    unsigned long m_size;
    unsigned long m_resident;
    unsigned long m_share;
    unsigned long m_text;
    unsigned long m_data;

    /* derived fields from summary calculation */
    int isParent;
    char host[EXEBUFFER_SIZE];
    char command[BUFFER_SIZE];
    char execCommand[BUFFER_SIZE];
    char script[BUFFER_SIZE];
    double derived_startTime;
    double derived_recTime;
    double baseline_startTime;
    double orig_startTime;
    unsigned int nObservations;
    size_t nRecords;
    double volatilityScore;
    double cpuTime;
    double duration;
    double cpuTime_net;
    unsigned long utime_net;
    unsigned long stime_net;
    unsigned long long io_rchar_net;
    unsigned long long io_wchar_net;
    double cpuRateMax;
    double iorRateMax;
    double iowRateMax;
    double msizeRateMax;
    double mresidentRateMax;
    double cov_cpuXiow;
    double cov_cpuXior;
    double cov_cpuXmsize;
    double cov_cpuXmresident;
    double cov_iowXior;
    double cov_iowXmsize;
    double cov_iowXmresident;
    double cov_iorXmsize;
    double cov_iorXmresident;
    double cov_msizeXmresident;
};

struct IdentifiedFilesystems: public procobs {
    IdentifiedFilesystems(ProcessSummary& other) {
        strncpy(host, other.host, EXEBUFFER_SIZE);
        strncpy(identifier, other.identifier, IDENTIFIER_SIZE);
        strncpy(subidentifier, other.subidentifier, IDENTIFIER_SIZE);
        startTime = other.startTime;
        pid = other.pid;
    }

    char host[EXEBUFFER_SIZE];
    char filesystem[BUFFER_SIZE];
    int read;
    int write;
};

class ProcessSummaryIO {
    hid_t strType_exeBuffer;
    hid_t strType_buffer;
    hid_t strType_idBuffer;
    hid_t type_procSummary;

    public:
    ProcessSummaryIO() {
        /* setup data structure types */
        strType_exeBuffer = H5Tcopy(H5T_C_S1);
        int status = H5Tset_size(strType_exeBuffer, EXEBUFFER_SIZE);
        if (status < 0) {
            throw ProcIOException("Failed to set strType_exeBuffer size");
        }

        strType_buffer = H5Tcopy(H5T_C_S1);
        status = H5Tset_size(strType_buffer, BUFFER_SIZE);
        if (status < 0) {
            throw ProcIOException("Failed to set strType_buffer size");
        }

        strType_idBuffer = H5Tcopy(H5T_C_S1);
        status = H5Tset_size(strType_idBuffer, IDENTIFIER_SIZE);
        if (status < 0) {
            throw ProcIOException("Failed to set strType_idBuffer size");
        }

        hid_t h5type = H5Tcreate(H5T_COMPOUND, sizeof(ProcessSummary));
        H5Tinsert(h5type, "identifier",    HOFFSET(ProcessSummary, identifier),    strType_idBuffer);
        H5Tinsert(h5type, "subidentifier", HOFFSET(ProcessSummary, subidentifier), strType_idBuffer);
        H5Tinsert(h5type, "startTime",     HOFFSET(ProcessSummary, startTime),     H5T_NATIVE_ULONG);
        H5Tinsert(h5type, "startTimeUSec", HOFFSET(ProcessSummary, startTimeUSec), H5T_NATIVE_ULONG);
        H5Tinsert(h5type, "recTime", HOFFSET(ProcessSummary, recTime), H5T_NATIVE_ULONG);
        H5Tinsert(h5type, "recTimeUSec", HOFFSET(ProcessSummary, recTimeUSec), H5T_NATIVE_ULONG);
        H5Tinsert(h5type, "pid",           HOFFSET(ProcessSummary, pid),           H5T_NATIVE_UINT);
        H5Tinsert(h5type, "hostname", HOFFSET(ProcessSummary, host), strType_exeBuffer);

        H5Tinsert(h5type, "execName", HOFFSET(ProcessSummary, execName), strType_exeBuffer);
        H5Tinsert(h5type, "cmdArgBytes", HOFFSET(ProcessSummary, cmdArgBytes), H5T_NATIVE_ULONG);
        H5Tinsert(h5type, "cmdArgs", HOFFSET(ProcessSummary, cmdArgs), strType_buffer);
        H5Tinsert(h5type, "exePath", HOFFSET(ProcessSummary, exePath), strType_buffer);
        H5Tinsert(h5type, "cwdPath", HOFFSET(ProcessSummary, cwdPath), strType_buffer);
        H5Tinsert(h5type, "ppid", HOFFSET(ProcessSummary, ppid), H5T_NATIVE_UINT);
        H5Tinsert(h5type, "startTime", HOFFSET(ProcessSummary, startTime), H5T_NATIVE_ULONG);
        H5Tinsert(h5type, "startTimeUSec", HOFFSET(ProcessSummary, startTimeUSec), H5T_NATIVE_ULONG);
        H5Tinsert(h5type, "state", HOFFSET(ProcessSummary, state), H5T_NATIVE_CHAR);
        H5Tinsert(h5type, "ppid", HOFFSET(ProcessSummary, ppid), H5T_NATIVE_INT);
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
        H5Tinsert(h5type, "command", HOFFSET(ProcessSummary, command), strType_buffer);
        H5Tinsert(h5type, "execCommand", HOFFSET(ProcessSummary, execCommand), strType_buffer);
        H5Tinsert(h5type, "script", HOFFSET(ProcessSummary, script), strType_buffer);
        H5Tinsert(h5type, "derived_startTime", HOFFSET(ProcessSummary, derived_startTime), H5T_NATIVE_DOUBLE);
        H5Tinsert(h5type, "derived_recTime", HOFFSET(ProcessSummary, derived_recTime), H5T_NATIVE_DOUBLE);
        H5Tinsert(h5type, "baseline_startTime", HOFFSET(ProcessSummary, baseline_startTime), H5T_NATIVE_DOUBLE);
        H5Tinsert(h5type, "orig_startTime", HOFFSET(ProcessSummary, orig_startTime), H5T_NATIVE_DOUBLE);
        H5Tinsert(h5type, "derived_recTime", HOFFSET(ProcessSummary, derived_recTime), H5T_NATIVE_DOUBLE);
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

        type_procSummary = h5type;
    }
    ~ProcessSummaryIO() {
        H5Tclose(type_procSummary);
        H5Tclose(strType_idBuffer);
        H5Tclose(strType_buffer);
        H5Tclose(strType_exeBuffer);
    }
};

static inline std::string &trim(std::string &s) {
    s.erase(s.begin(), find_if(s.begin(), s.end(), [](const char c){return isspace(c) == 0;}));
    s.erase(find_if(s.rbegin(), s.rend(), [](const char c){return isspace(c) == 0;}).base(), s.end());
    return s;
}

class Scriptable {
    protected:
    Scriptable(const char *_exePath, const char *_cmdArgs):
        exePath(_exePath)
    {
        string cmdArgsStr(_cmdArgs);
        boost::char_separator<char> sep("|");
        tokenizer tokens(cmdArgsStr, sep);
        for (auto token: tokens) {
            trim(token);
            cmdArgs.push_back(token);
        }
    }

    string exePath;
    vector<string> cmdArgs;

    public:
    static Scriptable *getScriptable(const char *exePath, const char *cmdArgs);
    virtual const string operator()() = 0;
};

class PerlScriptable : public Scriptable {
    public:
    PerlScriptable(const char *exePath, const char *cmdArgs):
        Scriptable(exePath, cmdArgs)
    {}

    virtual const string operator()() {
        for (size_t idx = 1; idx < cmdArgs.size(); ++idx) {
            if (cmdArgs[idx][0] == '-' && (cmdArgs[idx].find("e") != string::npos || cmdArgs[idx].find("E") != string::npos)) {
                return "COMMAND";
            } else if (cmdArgs[idx][0] == '-') {
                continue;
            }
            if (cmdArgs[idx].length() > 0) {
                return cmdArgs[idx];
            }
            return "";
        }
        return "";
    }
};

class JavaScriptable : public Scriptable {
    public:
    JavaScriptable(const char *exePath, const char *cmdArgs):
        Scriptable(exePath, cmdArgs)
    {}

    virtual const string operator()() {
        for (size_t idx = 1; idx < cmdArgs.size(); ++idx) {
            if (cmdArgs[idx] == "-cp" || cmdArgs[idx] == "-classpath") {
                // skip next arg
                idx += 1;
                continue;
            } else if (cmdArgs[idx][0] == '-') {
                // skip this arg
                continue;
            }
            // this arg must be the class name or jar file
            if (cmdArgs[idx].length() > 0) {
                return cmdArgs[idx];
            } else {
                return "";
            }
        }
        return "";
    }
};

class PythonScriptable : public Scriptable {
    public:
    PythonScriptable(const char *exePath, const char *cmdArgs):
        Scriptable(exePath, cmdArgs)
    {}

    virtual const string operator()() {
        array<string,3> skipArgs = {"-m", "-Q", "-W"};

        for (size_t idx = 1; idx < cmdArgs.size(); ++idx) {
            if (cmdArgs[idx][0] == '-' && cmdArgs[idx].find("c") != string::npos) {
                return "COMMAND";
            } else if (any_of(skipArgs.begin(), skipArgs.end(), [&](const string& arg){return (arg==cmdArgs[idx]);})) {
                idx++;
                continue;
            } else if (cmdArgs[idx][0] == '-') {
                continue;
            }
            if (cmdArgs[idx].length() > 0) {
                return cmdArgs[idx];
            }
            return "";
        }
        return "";
    }
};

class RubyScriptable : public Scriptable {
    public:
    RubyScriptable(const char *exePath, const char *cmdArgs):
        Scriptable(exePath, cmdArgs)
    {}

    virtual const string operator()() {
        array<string,5> skipArgs = {"-C","-F","-I","-K","-r"};

        for (size_t idx = 1; idx < cmdArgs.size(); ++idx) {
            if (cmdArgs[idx][0] == '-' && cmdArgs[idx].find("e") != string::npos) {
                return "COMMAND";
            } else if (any_of(skipArgs.begin(), skipArgs.end(), [&](const string& arg){return (arg==cmdArgs[idx]);})) {
                idx++;
                continue;
            } else if (cmdArgs[idx][0] == '-') {
                continue;
            }
            if (cmdArgs[idx].length() > 0) {
                return cmdArgs[idx];
            }
            return "";
        }
        return "";
    }
};

class BashScriptable : public Scriptable {
    public:
    BashScriptable(const char *exePath, const char *cmdArgs):
        Scriptable(exePath, cmdArgs)
    {}

    virtual const string operator()() {
        array<string,4> skipArgs = {"--rcfile","--init-file", "-O", "+O"};

        for (size_t idx = 1; idx < cmdArgs.size(); ++idx) {
            if (cmdArgs[idx][0] == '-' && cmdArgs[idx].find("c") != string::npos) {
                return "COMMAND";
            } else if (any_of(skipArgs.begin(), skipArgs.end(), [&](const string& arg){return (arg==cmdArgs[idx]);})) {
                idx++;
                continue;
            } else if (cmdArgs[idx][0] == '-') {
                continue;
            }
            if (cmdArgs[idx].length() > 0) {
                return cmdArgs[idx];
            }
            return "";
        }
        return "";
    }
};

class CshScriptable : public Scriptable {
    public:
    CshScriptable(const char *exePath, const char *cmdArgs):
        Scriptable(exePath, cmdArgs)
    {}

    virtual const string operator()() {
        for (size_t idx = 1; idx < cmdArgs.size(); ++idx) {
            if (cmdArgs[idx][0] == '-' && cmdArgs[idx].find("c") != string::npos) {
                return "COMMAND";
            } else if (cmdArgs[idx][0] == '-' && cmdArgs[idx].find("b") != string::npos) {
                idx += 1;
                if (idx < cmdArgs.size() && cmdArgs[idx].length() > 0) {
                    return cmdArgs[idx];
                }
            } else if (cmdArgs[idx][0] == '-') {
                continue;
            }
            if (cmdArgs[idx].length() > 0) {
                return cmdArgs[idx];
            }
            return "";
        }
        return "";
    }
};

#endif
