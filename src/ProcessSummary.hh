#ifndef __PROCESSSUMMARY_HH
#define __PROCESSSUMMARY_HH

#include "ProcData.hh"

class ProcessSummary: public procobs {
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


    

#endif
