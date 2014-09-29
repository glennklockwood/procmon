#ifndef __PROCESSSUMMARY_HH
#define __PROCESSSUMMARY_HH

#include "ProcData.hh"
#include <boost/tokenizer.hpp>
#include <algorithm> 
#include <functional> 
#include <cctype>
#include <locale>
#include <array>

typedef boost::tokenizer<boost::char_separator<char> > tokenizer;

struct ProcessSummary: public procobs {
    public:

    ProcessSummary();
    void update(const procstat *);
    update(const procdata *);
    update(const procfd *);
    normalize(ProcessSummary *, time_t baseline);



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
    char command[EXEBUFFER_SIZE];
    char execCommand[EXEBUFFER_SIZE];
    char script[BUFFER_SIZE];
    double derived_startTime;
    double derived_recTime;
    double baseline_startTime;
    double orig_startTime;
    unsigned int nObservations;
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
    double cor_cpuXiow;
    double cor_cpuXior;
    double cor_cpuXmresident;
    double cor_iowXior;
    double cor_iowXmsize;
    double cor_iowXmresident;
    double cor_iorXmsize;
    double cor_iorXmresident;
    double cor_msizeXmresident;
};

static inline std::string &trim(std::string &s) {
    s.erase(s.begin(), find_if(s.begin(), s.end(), not1(isspace)));
    s.erase(find_if(s.rbegin(), s.rend(), not1(isspace)).base(), s.end());
    return s;
}

class JavaScriptable;
class PythonScriptable;
class PerlScriptable;
class RubyScriptable;
class BashScriptable;
class CshScriptable;

class Scriptable {
    protected:
    Scriptable(const char *_exePath, const char *_cmdArgs):
        exePath(_exePath)
    {
        boost::char_separator<char> sep("|");
        tokenizer tokens(_cmdArgs, sep);
        for (auto token: tokens) {
            cmdArgs.insert(trim(token));
        }
    }

    string exePath;
    vector<string> cmdArgs;

    public:
    static Scriptable *getScriptable(const char *exePath, const char *cmdArgs) {
        const char *execName = exePath;
        const char *last_slash = strrchr(exePath, '/');
        if (last_slash != NULL) execName = last_slash + 1;
        if (strncmp(execName, "java") == 0) {
            return new JavaScriptble(exePath, cmdArgs);
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
    }
    virtual const string& operator()() = 0;
};

class PerlScriptable : public Scriptable {
    public:
    PerlScriptable(const char *exePath, const char *cmdArgs):
        Scriptable(exePath, cmdArgs)
    {}

    virtual const string& operator()() {
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

    virtual const string& operator()() {
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

    virtual const string& operator()() {
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

    virtual const string& operator()() {
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

    virtual const string& operator()() {
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

    virtual const string& operator()() {
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
