#ifndef __PROCDATA_CONFIG_HH_
#define __PROCDATA_CONFIG_HH_

#define OUTPUT_TYPE_TEXT 0x1
#define OUTPUT_TYPE_HDF5 0x2
#define OUTPUT_TYPE_AMQP 0x4
#define OUTPUT_TYPE_NONE 0x8

#define BUFFER_SIZE 1024
#define LBUFFER_SIZE 8192
#define EXEBUFFER_SIZE 256
#define IDENTIFIER_SIZE 24

typedef struct _procdata {
	char identifier[IDENTIFIER_SIZE];
	char subidentifier[IDENTIFIER_SIZE];
    char execName[EXEBUFFER_SIZE];
    unsigned long cmdArgBytes;
    char cmdArgs[BUFFER_SIZE];
    char exePath[BUFFER_SIZE];
    char cwdPath[BUFFER_SIZE];
    unsigned long recTime;
    unsigned long recTimeUSec;
    unsigned long startTime;
    unsigned long startTimeUSec;
    unsigned int pid;
    unsigned int ppid;
    bool equivRecord(const struct _procdata &other) const {
        return pid == other.pid && startTime == other.startTime;
    }
} procdata;

typedef struct _procstat {
	char identifier[IDENTIFIER_SIZE];
	char subidentifier[IDENTIFIER_SIZE];
    unsigned int pid;
    unsigned long recTime;
    unsigned long recTimeUSec;
    unsigned long startTime;
    unsigned long startTimeUSec;
    char state;
    unsigned int ppid;
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
    bool equivRecord(const struct _procstat &other) const {
        return pid == other.pid && startTime == other.startTime;
    }
} procstat;

typedef struct _procfd {
	char identifier[IDENTIFIER_SIZE];
	char subidentifier[IDENTIFIER_SIZE];
	unsigned int pid;
	unsigned int ppid;
    unsigned long recTime;
    unsigned long recTimeUSec;
    unsigned long startTime;
    unsigned long startTimeUSec;
    char path[BUFFER_SIZE];
	int fd;
	unsigned int mode;
    bool equivRecord(const struct _procfd &other) const {
        return pid == other.pid && startTime == other.startTime && fd == other.fd;
    }
} procfd;

typedef struct _procobs {
    char identifier[IDENTIFIER_SIZE];
    char subidentifier[IDENTIFIER_SIZE];
    unsigned int pid;
    unsigned long recTime;
    unsigned long recTimeUSec;
    unsigned long startTime;
    unsigned long startTimeUSec;
    bool equivRecord(const struct _procstat &other) const {
        return pid == other.pid && startTime == other.startTime;
    }
} procobs;

class ProcmonDataset {
    public:
    virtual bool equivRecord(const ProcmonDataset &other) const = 0;
    virtual bool cmpRecord(const ProcomnDataset &other) const = 0;

    char identifier[IDENTIFIER_SIZE];
    char subidentifier[IDENTIFIER_SIZE];
    unsigned long recTime;
    unsigned long recTimeUSec;
};

#endif
