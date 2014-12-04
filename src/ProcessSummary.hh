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
#include <ProcIO2.hh>

using namespace std;
typedef boost::tokenizer<boost::char_separator<char> > tokenizer;

class ProcessSummary {
    public:
    ProcessSummary();
    ProcessSummary(const string& _hostname, const string& _identifier, const string& _subidentifier, unsigned long _start, int _pid);

    char identifier[IDENTIFIER_SIZE];
    char subidentifier[IDENTIFIER_SIZE];
    unsigned long recTime;
    unsigned long recTimeUSec;

    unsigned long startTime;
    unsigned long startTimeUSec;
    int pid;

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
    char user[EXEBUFFER_SIZE];
    char project[EXEBUFFER_SIZE];
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

class IdentifiedFilesystem {
    public:
    IdentifiedFilesystem(const ProcessSummary &other, const string &_filesystem, const int _read, const int _write) {
        snprintf(host, EXEBUFFER_SIZE, "%s", other.host);
        snprintf(identifier, IDENTIFIER_SIZE, "%s", other.identifier);
        snprintf(subidentifier, IDENTIFIER_SIZE, "%s", other.subidentifier);
        snprintf(command, BUFFER_SIZE, "%s", other.command);
        snprintf(user, EXEBUFFER_SIZE, "%s", other.user);
        snprintf(project, EXEBUFFER_SIZE, "%s", other.project);

        startTime = other.startTime;
        startTimeUSec = other.startTimeUSec;
        pid = other.pid;
        snprintf(filesystem, IDENTIFIER_SIZE, "%s", _filesystem.c_str());
        read = _read;
        write = _write;
    }

    char identifier[IDENTIFIER_SIZE];
    char subidentifier[IDENTIFIER_SIZE];
    unsigned long startTime;
    unsigned long startTimeUSec;
    int pid;
    char host[EXEBUFFER_SIZE];
    char filesystem[BUFFER_SIZE];
    char command[BUFFER_SIZE];
    char user[EXEBUFFER_SIZE];
    char project[EXEBUFFER_SIZE];
    int read;
    int write;
};

class IdentifiedNetworkConnection {
    public:
    IdentifiedNetworkConnection(const ProcessSummary &other, const string &net) {
        snprintf(host, EXEBUFFER_SIZE, "%s", other.host);
        snprintf(identifier, IDENTIFIER_SIZE, "%s", other.identifier);
        snprintf(subidentifier, IDENTIFIER_SIZE, "%s", other.subidentifier);
        snprintf(command, BUFFER_SIZE, "%s", other.command);
        snprintf(user, EXEBUFFER_SIZE, "%s", other.user);
        snprintf(project, EXEBUFFER_SIZE, "%s", other.project);
        startTime = other.startTime;
        startTimeUSec = other.startTimeUSec;
        pid = other.pid;

        int count = 0;
        size_t searchPos = 0;
        for (int count = 0; count < 5; ++count) {
            size_t endPos = net.find(':', searchPos);
            string component = endPos == string::npos ? net.substr(searchPos) : net.substr(searchPos, endPos - searchPos);
            switch (count) {
                case 0: strncpy(protocol, component.c_str(), IDENTIFIER_SIZE); break;
                case 1:
                        snprintf(localAddress, EXEBUFFER_SIZE, "%s", component.c_str());
                        localAddress[endPos-searchPos] = 0;
                        break;
                case 2: localPort = atoi(component.c_str()); break;
                case 3:
                        snprintf(remoteAddress, EXEBUFFER_SIZE, "%s", component.c_str());
                        remoteAddress[endPos-searchPos] = 0;
                        break;
                case 4: remotePort = atoi(component.c_str()); break;
            }
            if (endPos == string::npos) {
                break;
            }
            searchPos = endPos + 1;
        }
    }

    char identifier[IDENTIFIER_SIZE];
    char subidentifier[IDENTIFIER_SIZE];
    char host[EXEBUFFER_SIZE];
    unsigned long startTime;
    unsigned long startTimeUSec;
    int pid;
    char command[BUFFER_SIZE];
    char protocol[IDENTIFIER_SIZE];
    char remoteAddress[EXEBUFFER_SIZE];
    int remotePort;
    char localAddress[EXEBUFFER_SIZE];
    int localPort;
    char user[EXEBUFFER_SIZE];
    char project[EXEBUFFER_SIZE];
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
