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

struct procdata {
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
    inline bool equivRecord(const procdata &other) const {
        return pid == other.pid && startTime == other.startTime;
    }
    /*
    const string& serialize(bool includeIdent=true, const string host="") const {
        ostringstream oss;
        if (includeIdent) {
            oss << "procdata," << host << "," << identifier << "," << subidentifier << ",";
        }
        oss << pid << "," << ppid << "," << recTime << "," << recTimeUSec
            << "," << startTime << "," << startTimeUSec << ","
            << strlen(execName) << "," << execName << ","
            << cmdArgBytes << "," << cmdArgs << ","
            << strlen(exePath) << "," << exePath << ","
            << strlen(cwdPath) << "," << cwdPath << endl;
        return oss.str();
    }
    procdata(const string& data, bool includesIdent=true) {
        char *ptr = strdup(data.c_str());
        char *s_ptr = ptr;
        int idx = 0;
        int len = 0;
        if (! includesIdent) idx = 3;
        for ( ; *ptr != 0; ++ptr) {
            if (*ptr == ",") {
                *ptr = 0;
                switch (idx) {
                    case 0:
                    case 1:
                        break;
                    case 2:
                        snprintf(identifier, IDENTIFIER_SIZE, "%s", s_ptr);
                        break;
                    case 3:
                        snprintf(subidentifier, IDENTIFIER_SIZE, "%s", s_ptr);
                        break;
                    case 4:
                        pid = atoi(s_ptr); break;
                    case 5:
                        ppid = atoi(s_ptr); break;
                    case 6:
                        recTime = strtoul(s_ptr, 10, NULL); break;
                        break;
                    case 7:
                        recTimeUSec = strtoul(s_ptr, 10, NULL); break;
                        break;
                    case 8:
                        recTime = strtoul(s_ptr, 10, NULL); break;
                        break;
                    case 9:
                        recTimeUSec = strtoul(s_ptr, 10, NULL); break;
                        break;
                    case 10:
                        len = atoi(s_ptr);
                        s_ptr = ptr + 1;
                        ptr += len;
                        *ptr = 0;
                        snprintf(execName, EXEBUFFER_SIZE, "%s", s_ptr);
                        break;
                    case 11:
                        len = atoi(s_ptr);
                        s_ptr = ptr + 1;
                        ptr += len;
                        *ptr = 0;
                        snprintf(cmdArgs, BUFFER_SIZE, "%s", s_ptr);
                        break;
                    case 12:
                        len = atoi(s_ptr);
                        s_ptr = ptr + 1;
                        ptr += len;
                        *ptr = 0;
                        snprintf(exePath, BUFFER_SIZE, "%s", s_ptr);
                        break;
                    case 13:
                        len = atoi(s_ptr);
                        s_ptr = ptr + 1;
                        ptr += len;
                        *ptr = 0;
                        snprintf(cwdPath, BUFFER_SIZE, "%s", s_ptr);
                        break;
                }
                idx++;
                s_ptr = ptr + 1;
            }
        }
    }
    */
};

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
    inline bool equivRecord(const struct _procstat &other) const {
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
    inline bool equivRecord(const struct _procfd &other) const {
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
    inline bool equivRecord(const struct _procstat &other) const {
        return pid == other.pid && startTime == other.startTime;
    }
} procobs;

struct netstat {
    char identifier[IDENTIFIER_SIZE];
    char subidentifier[IDENTIFIER_SIZE];
    unsigned long recTime;
    unsigned long recTimeUSec;
    unsigned long local_address;
    unsigned int local_port;
    unsigned long remote_address;
    unsigned int remote_port;
    unsigned short state;
    unsigned long tx_queue;
    unsigned long rx_queue;
    unsigned short tr;
    unsigned long ticks_expire;
    unsigned long retransmit;
    unsigned long uid;
    unsigned long timeout;
    unsigned long inode;
    unsigned int refCount;
    int type; // 0 = tcp, 1 = udp
    inline bool equivRecord(const netstat& other) const {
        return inode == other.inode && local_address == other.local_address && remote_address == other.remote_address && local_port == other.local_port && remote_port == other.remote_port;
    }
};

class ProcmonDataset {
    public:
    char identifier[IDENTIFIER_SIZE];
    char subidentifier[IDENTIFIER_SIZE];
    unsigned long recTime;
    unsigned long recTimeUSec;

    virtual bool equivRecord(const ProcmonDataset &other) const = 0;
    virtual bool cmpRecord(const ProcmonDataset &other) const = 0;

};

#endif
