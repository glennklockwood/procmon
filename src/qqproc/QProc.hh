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

typedef struct _VarDescriptor {
    char name[30];
    unsigned char type;
    unsigned char outputType;
} VarDescriptor;

/*
qacctVariablesBasic describes all the variables that are defined
either in the accounting logs or are parsed from the categories/resources
field of the GE logs
*/
static const unsigned int maxVarSize = 67;
static const VarDescriptor qVariablesBasic[maxVarSize] = {
    { "identifier",            T_STRING,     T_STRING }, // 0
    { "subidentifier",         T_STRING,     T_STRING }, // 1
    { "hostname",              T_STRING,     T_STRING }, // 2
    { "execName",              T_STRING,     T_STRING }, // 3
    { "cmdArgs",               T_STRING,     T_STRING }, // 4
    { "cmdArgBytes",           T_MEMORY,     T_INTEGER }, // 5
    { "exePath",               T_STRING,     T_STRING }, // 6
    { "cwdPath",               T_STRING,     T_STRING }, // 7
    { "obs_time",              T_INTEGER,    T_INTEGER }, // 8
    { "start_time",            T_INTEGER,    T_INTEGER }, // 9
    { "pid",                   T_INTEGER,    T_INTEGER }, // 10
    { "ppid",                  T_INTEGER,    T_INTEGER }, // 11
    { "pgrp",                  T_INTEGER,    T_INTEGER }, // 12
    { "session",               T_INTEGER,    T_INTEGER }, // 13
    { "tty",                   T_INTEGER,    T_INTEGER }, // 14
    { "tpgid",                 T_INTEGER,    T_INTEGER }, // 15
    { "realUid",               T_USER,       T_INTEGER }, // 16
    { "effUid",                T_USER,       T_INTEGER }, // 17
    { "realGid",               T_GROUP,      T_INTEGER }, // 18
    { "effGid",                T_GROUP,      T_INTEGER }, // 19
    { "flags",                 T_INTEGER,    T_INTEGER }, // 20
    { "utime",                 T_DOUBLE,     T_TICKS }, // 21
    { "stime",                 T_DOUBLE,     T_TICKS }, // 22
    { "priority",              T_INTEGER,    T_INTEGER }, // 23
    { "nice",                  T_INTEGER,    T_INTEGER }, //24
    { "numThreads",            T_INTEGER,    T_INTEGER }, // 25
    { "vsize",                 T_INTEGER,    T_INTEGER }, // 26
    { "rss",                   T_MEMORY,     T_INTEGER }, // 27
    { "rsslim",                T_MEMORY,     T_INTEGER }, // 28
    { "signal",                T_INTEGER,    T_INTEGER }, // 29
    { "blocked",               T_INTEGER,    T_INTEGER }, // 30
    { "sigignore",             T_INTEGER,    T_INTEGER }, //31
    { "sigcatch",              T_INTEGER,    T_INTEGER }, //32
    { "rtPriority",            T_INTEGER,    T_INTEGER }, //33
    { "policy",                T_INTEGER,    T_INTEGER }, // 34
    { "delayacctBlkIOTicks",   T_INTEGER,    T_INTEGER }, // 35
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
    { "fd",                    T_INTEGER,    T_INTEGER}, //52
    { "mode",                  T_INTEGER,    T_INTEGER}, //53
    { "path",                  T_STRING,     T_STRING}, //54
    { "stateSince",            T_INTEGER,    T_INTEGER}, //55
    { "stateAge",              T_INTEGER,    T_INTEGER}, //56
    { "delta_stime",           T_DOUBLE,     T_TICKS}, //57
    { "delta_utime",           T_DOUBLE,     T_TICKS}, //58
    { "delta_ioread",          T_MEMORY,     T_INTEGER}, //59
    { "delta_iowrite",         T_MEMORY,     T_INTEGER}, //60
    { "cputime",               T_DOUBLE,     T_TICKS}, //61
    { "delta_cputime",         T_DOUBLE,     T_TICKS}, //62
    { "io",                    T_MEMORY,     T_INTEGER}, //63
    { "delta_io",              T_MEMORY,     T_INTEGER}, //64
    { "age",                   T_INTEGER,    T_INTEGER}, //65
    { "state",                 T_CHAR,       T_CHAR}, //66
};

namespace po = boost::program_options;
namespace fs = boost::filesystem;

struct ProcessData {
    procstat *ps;
    procdata *pd;
    procfd  **fd;
    int nfd;
    JobDelta<procstat> *delta_ps;
};

class Data {
public:
    Data() {
        set = false;
    }

    void setValue(intType value, unsigned char t_type=T_INTEGER) {
        i_value = value;
        set = true;
        type = t_type;
    }
    void setValue(double value, unsigned char t_type=T_DOUBLE) {
        d_value = value;
        set = true;
        type = t_type;
    }

    void setValue(const char* value, unsigned char t_type=T_STRING) {
        s_value = value;
        set = true;
        type = t_type;
    }
    void setValue(std::string& value, unsigned char t_type=T_STRING) {
        str_value = value;
        s_value = str_value.c_str();
        set = true;
        type = t_type;
    }
    void setValue(bool value, unsigned char t_type=T_BOOL) {
        b_value = value;
        set = true;
        type = t_type;
    }
    unsigned char type;
    intType i_value;
    double d_value;
    bool b_value;
    const char* s_value;
    std::string str_value;
    bool set;

};

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
