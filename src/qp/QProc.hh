#ifndef __QPROC_HH_
#define __QPROC_HH_

#include <string>
#include <vector>
#include <map>
#include <algorithm>
#include <iterator>
#include <stdlib.h>
#include <iostream>

#include "ProcData.hh"
#include "ProcCache.hh"

#include <boost/program_options.hpp>
#include <boost/filesystem.hpp>
#include <boost/regex.hpp>

#define T_INTEGER 0
#define T_DOUBLE 1
#define T_STRING 2
#define T_CHAR 3
#define T_BOOL 4
#define T_MEMORY 5
#define T_USER 6
#define T_GROUP 7
#define T_TICKS 8
#define T_UNDEF 9

typedef long intType;

union _DataRep {
    double d;
    intType i;
    string s;
    char c;
    bool b;
} DataRep;

class Data {
    unsigned char type;
    DataRep data;
    bool set;

    public:
    Data() {
        set = false;
    }

    void setValue(intType value, unsigned char t_type=T_INTEGER) {
        data.i = value;
        set = true;
        type = t_type;
    }
    void setValue(double value, unsigned char t_type=T_DOUBLE) {
        data.d = value;
        set = true;
        type = t_type;
    }

    void setValue(const char* value, unsigned char t_type=T_STRING) {
        data.s = value;
        set = true;
        type = t_type;
    }
    void setValue(std::string& value, unsigned char t_type=T_STRING) {
        data.s = value;
        set = true;
        type = t_type;
    }
    void setValue(bool value, unsigned char t_type=T_BOOL) {
        b_value = value;
        set = true;
        type = t_type;
    }
};

struct ProcessData {
    procstat *ps;
    procdata *pd;
    JobDelta<procstat> *delta_ps;
    const char *hostname;

    ProcessData(procstat *_ps, procdata *_pd, JobDelta<procstat> *_delta_ps, const char *_hostname):
        ps(_ps), pd(_pd), delta_ps(_delta_ps), hostname(_hostname)
    {
    }

    static inline void setIdentifier(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->identifier);
    }
    static inline void setSubidentifier(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->subidentifier);
    }
    static inline void setCmdArgs(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->subidentifier);
    }
    static inline void setHostname(ProcessData *p, Data *data) {
        data->setValue(hostname);
    }
    static inline void setExecName(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->pd == NULL) return;
        data->setValue(p->pd->execName);
    }
    static inline void setCmdArgs(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->pd == NULL) return;
        data->setValue(p->pd->cmdArgs);
    }
    static inline intType setCmdArgBytes(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->pd == NULL) return;
        data->setValue(p->ps->cmdArgBytes);
    }
    static inline void setExePath(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->pd == NULL) return;
        data->setValue(p->pd->exePath);
    }
    static inline void setCwdPath(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->pd == NULL) return;
        data->setValue(p->pd->cwdPath);
    }
    static inline void setObsTime(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->recTime + p->ps->recTimeUSec*1e-6);
    }
    static inline void setStartTime(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->startTime + p->ps->startTimeUSec*1e-6);
    }
    static inline void setPid(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->pid);
    }
    static inline void setPpid(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->ppid);
    }
    static inline void setPgrp(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->pgrp);
    }
    static inline void setSession(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->session);
    }
    static inline void setTty(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->tty);
    }
    static inline void setTpgid(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->tpgid);
    }
    static inline void setRealUid(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->realUid);
    }
    static inline void setEffUid(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->effUid);
    }
    static inline void setRealGid(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->realGid);
    }
    static inline void setEffGid(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->effGid);
    }
    static inline void setFlags(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->flags);
    }
    static inline void setUtime(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->utime);
    }
    static inline void setStime(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->stime);
    }
    static inline void setPriority(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->priority);
    }
    static inline void setNice(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->nice);
    }
    static inline void setNumThreads(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->numThreads);
    }
    static inline void setVsize(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->vsize);
    }
    static inline void setRss(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->rss);
    }
    static inline void setRsslim(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->rsslim);
    }
    static inline void setSignal(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->signal);
    }
    static inline void setBlocked(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->blocked);
    }
    static inline void setSigignore(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->sigignore);
    }
    static inline void setSigcatch(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->sigcatch);
    }
    static inline void setRtPriority(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->rtPriority);
    }
    static inline void setPolicy(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->policy);
    }
    static inline void setDelayacctBlkIOTicks(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->delayacctBlkIOTicks);
    }
    static inline void setDelayacctBlkIOTicks(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->delayacctBlkIOTicks);
    }
    static inline void setGuestTime(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->guestTime);
    }
    static inline void setVmpeak(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->vmpeak);
    }
    static inline void setRsspeak(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->rsspeak);
    }
    static inline void setCpusAllowed(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->cpusAllowed);
    }
    static inline void setIORchar(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->io_rchar);
    }
    static inline void setIOWchar(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->io_wchar);
    }
    static inline void setIOSyscr(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->io_syscr);
    }
    static inline void setIOSyscw(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->io_syscw);
    }
    static inline void setIOReadBytes(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->io_readBytes);
    }
    static inline void setIOWriteBytes(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->io_writeBytes);
    }
    static inline void setIOCancelledWriteBytes(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->io_cancelledWriteBytes);
    }
    static inline void setMSize(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->m_size);
    }
    static inline void setMResident(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->m_resident);
    }
    static inline void setMShare(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->m_share);
    }
    static inline void setMText(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->m_text);
    }
    static inline void setMData(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->m_data);
    }
    static inline void setStateSince(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->delta_ps == NULL || delta_ps->stateSince == 0) return;
        data->setValue(p->delta_ps->stateSince);
    }
    static inline void setStateAge(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->delta_ps == NULL || delta_ps->stateSince == 0) return;
        data->setValue( time(NULL) - p->delta_ps->stateSince);
    }
    static inline void setDeltaStime(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->delta_ps == NULL) return;
        data->setValue(p->delta_ps->delta_stime);
    }
    static inline void setDeltaUtime(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->delta_ps == NULL) return;
        data->setValue(p->delta_ps->delta_utime);
    }
    static inline void setDeltaIoread(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->delta_ps == NULL) return;
        data->setValue(p->delta_ps->delta_ioread);
    }
    static inline void setDeltaIowrite(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->delta_ps == NULL) return;
        data->setValue(p->delta_ps->delta_iowrite);
    }

    static inline void setCputicks(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->utime + ps->stime);
    }

    static inline void setDeltaCputicks(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->delta_ps == NULL) return;
        data->setValue(p->delta_ps->delta_stime + p->delta_ps->delta_utime);
    }

    static inline void setIo(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->io_wchar + p->ps->io-rchar);
    }

    static inline void setDeltaIo(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->delta_ps == NULL) return;
        data->setValue(p->delta_ps->io_read + p->delta_ps->io_write);
    }

    static inline void setAge(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(time(NULL) - p->ps->recTime);
    }

    static inline void setState(ProcessData *p, Data *data) {
        data->setValue(p->ps->state);
    }
};

typedef struct _VarDescriptor {
    char name[30];
    unsigned char type;
    unsigned char outputType;
    void (*evalFxn)(ProcessData *, Data *);
} VarDescriptor;

/*
qacctVariablesBasic describes all the variables that are defined
either in the accounting logs or are parsed from the categories/resources
field of the GE logs
*/
static const unsigned int maxVarSize = 64;
static const VarDescriptor qVariablesBasic[maxVarSize] = {
    { "identifier",            T_STRING,     T_STRING,      ProcessData::setIdentifier}, // 0
    { "subidentifier",         T_STRING,     T_STRING,      ProcessData::setSubidentifier }, // 1
    { "hostname",              T_STRING,     T_STRING,      ProcessData::setHostname }, // 2
    { "execName",              T_STRING,     T_STRING,      ProcessData::setExecName }, // 3
    { "cmdArgs",               T_STRING,     T_STRING,      ProcessData::setCmdArgs }, // 4
    { "cmdArgBytes",           T_MEMORY,     T_INTEGER,     ProcessData::setCmdArgBytes }, // 5
    { "exePath",               T_STRING,     T_STRING,      ProcessData::setExePath }, // 6
    { "cwdPath",               T_STRING,     T_STRING,      ProcessData::setCwdPath }, // 7
    { "obsTime",               T_INTEGER,    T_INTEGER,     ProcessData::setObsTime }, // 8
    { "startTime",             T_INTEGER,    T_INTEGER,     ProcessData::setStartTime }, // 9
    { "pid",                   T_INTEGER,    T_INTEGER,     ProcessData::setPid }, // 10
    { "ppid",                  T_INTEGER,    T_INTEGER,     ProcessData::setPpid }, // 11
    { "pgrp",                  T_INTEGER,    T_INTEGER,     ProcessData::setPgrp }, // 12
    { "session",               T_INTEGER,    T_INTEGER,     ProcessData::setSession }, // 13
    { "tty",                   T_INTEGER,    T_INTEGER,     ProcessData::setTty }, // 14
    { "tpgid",                 T_INTEGER,    T_INTEGER,     ProcessData::setTpgid }, // 15
    { "realUid",               T_USER,       T_INTEGER,     ProcessData::setRealUid }, // 16
    { "effUid",                T_USER,       T_INTEGER,     ProcessData::setEffUid }, // 17
    { "realGid",               T_GROUP,      T_INTEGER,     ProcessData::setRealGid }, // 18
    { "effGid",                T_GROUP,      T_INTEGER,     ProcessData::setEffGid }, // 19
    { "flags",                 T_INTEGER,    T_INTEGER,     ProcessData::setFlags }, // 20
    { "utime",                 T_TICKS,      T_INTEGER,     ProcessData::setUtime }, // 21
    { "stime",                 T_TICKS,      T_INTEGER,     ProcessData::setStime }, // 22
    { "priority",              T_INTEGER,    T_INTEGER,     ProcessData::setPriority }, // 23
    { "nice",                  T_INTEGER,    T_INTEGER,     ProcessData::setNice }, //24
    { "numThreads",            T_INTEGER,    T_INTEGER,     ProcessData::setNumThreads }, // 25
    { "vsize",                 T_INTEGER,    T_INTEGER,     ProcessData::setVsize }, // 26
    { "rss",                   T_MEMORY,     T_INTEGER,     ProcessData::setRss }, // 27
    { "rsslim",                T_MEMORY,     T_INTEGER,     ProcessData::setRsslim }, // 28
    { "signal",                T_INTEGER,    T_INTEGER,     ProcessData::setSignal }, // 29
    { "blocked",               T_INTEGER,    T_INTEGER,     ProcessData::setBlocked }, // 30
    { "sigignore",             T_INTEGER,    T_INTEGER,     ProcessData::setSigignore }, //31
    { "sigcatch",              T_INTEGER,    T_INTEGER,     ProcessData::setSigcatch }, //32
    { "rtPriority",            T_INTEGER,    T_INTEGER,     ProcessData::setRtPriority }, //33
    { "policy",                T_INTEGER,    T_INTEGER,     ProcessData::setPolicy }, // 34
    { "delayacctBlkIOTicks",   T_INTEGER,    T_INTEGER,     ProcessData::setDelayacctBlk }, // 35
    { "guestTime",             T_INTEGER,    T_INTEGER }, //36
    { "vmpeak",                T_MEMORY,     T_INTEGER }, //37
    { "rsspeak",               T_MEMORY,     T_INTEGER }, //38
    { "cpusAllowed",           T_INTEGER,    T_INTEGER}, //39
    { "io_rchar",              T_MEMORY,     T_INTEGER}, //40
    { "io_wchar",              T_MEMORY,     T_INTEGER}, //41
    { "io_syscr",              T_INTEGER,    T_INTEGER}, //42
    { "io_syscw",              T_INTEGER,    T_INTEGER}, //43
    { "io_readBytes",          T_MEMORY,     T_INTEGER}, //44
    { "io_writeBytes",         T_MEMORY,     T_INTEGER}, //45
    { "io_cancelledWriteBytes",T_MEMORY,     T_INTEGER}, //46
    { "m_size",                T_MEMORY,     T_INTEGER}, //47
    { "m_resident",            T_MEMORY,     T_INTEGER}, //48
    { "m_share",               T_MEMORY,     T_INTEGER}, //49
    { "m_text",                T_MEMORY,     T_INTEGER}, //50
    { "m_data",                T_MEMORY,     T_INTEGER}, //51
    { "stateSince",            T_INTEGER,    T_INTEGER}, //52
    { "stateAge",              T_INTEGER,    T_INTEGER}, //53
    { "delta_stime",           T_TICKS,      T_INTEGER}, //54
    { "delta_utime",           T_TICKS,      T_INTEGER}, //55
    { "delta_ioread",          T_MEMORY,     T_INTEGER}, //56
    { "delta_iowrite",         T_MEMORY,     T_INTEGER}, //57
    { "cputicks",              T_TICKS,      T_INTEGER}, //58
    { "delta_cputicks",        T_TICKS,      T_INTEGER}, //59
    { "io",                    T_MEMORY,     T_INTEGER}, //60
    { "delta_io",              T_MEMORY,     T_INTEGER}, //61
    { "age",                   T_INTEGER,    T_INTEGER}, //62
    { "state",                 T_CHAR,       T_CHAR}, //63
};

namespace po = boost::program_options;
namespace fs = boost::filesystem;


std::ostream& operator<<(std::ostream &out, Data& data);


class QProcConfiguration {
public:
    std::vector<std::string> declarations;
    std::vector<std::string> queries;
    std::vector<std::string> outputFilenames;
    std::vector<std::string> outputColumns;
    std::string header;

    bool verbose;
    std::string configFile;
    char delimiter;

    std::vector<VarDescriptor> qVariables;
    std::map<std::string,int> qVariableMap;

    QProcConfiguration(int argc, char** argv);
};

#endif
