/*
 * procfmt.h
 *
 * Author: Douglas Jacobsen <dmjacobsen@lbl.gov>, NERSC User Services Group
 * 2013/02/17
 * Copyright (C) 2012, The Regents of the University of California
 *
 * The purpose of the procmon is to read data from /proc for an entire process tree
 * and save that data at intervals longitudinally
 */

#include <stdio.h>
#include <time.h>
#include <unistd.h>
#include <exception>
#include "hdf5.h"

#ifndef PROCFMT_H_
#define PROCFMT_H_

#define BUFFER_SIZE 1024
#define LBUFFER_SIZE 8192
#define EXEBUFFER_SIZE 256

typedef struct _procdata {
    char execName[EXEBUFFER_SIZE];
    short cmdArgBytes;
    char cmdArgs[BUFFER_SIZE];
    char exePath[BUFFER_SIZE];
    char cwdPath[BUFFER_SIZE];
    time_t recTime;
    unsigned long recTimeUSec;
    time_t startTime;
    unsigned long startTimeUSec;
    pid_t pid;
    pid_t ppid;
} procdata;

typedef struct _procstat {
	int pid;
    time_t recTime;
    unsigned long recTimeUSec;
	char state;
	int ppid;
	int pgrp;
	int session;
	int tty;
	int tpgid;
	unsigned long realUid;
	unsigned long effUid;
	unsigned long realGid;
	unsigned long effGid;
	unsigned int flags;
	unsigned long minorFaults;
	unsigned long cminorFaults;
	unsigned long majorFaults;
	unsigned long cmajorFaults;
	unsigned long utime;
	unsigned long stime;
	long cutime;
	long cstime;
	long priority;
	long nice;
	long numThreads;
	long itrealvalue; /* likely zero in all modern kernels */
	unsigned long long starttime;
	unsigned long vsize; /* virtual mem in bytes */
	unsigned long rss;   /* number of pages in physical memory */
	unsigned long rsslim;/* limit of rss bytes */
	unsigned long startcode;
	unsigned long endcode;
	unsigned long startstack;
	unsigned long kstkesp;
	unsigned long kstkeip;
	unsigned long signal;
	unsigned long blocked;
	unsigned long sigignore;
	unsigned long sigcatch;
	unsigned long wchan;
	unsigned long nswap;
	unsigned long cnswap;
	int exitSignal;
	int processor;
	unsigned int rtPriority;
	unsigned int policy;
	unsigned long long delayacctBlkIOTicks;
	unsigned long guestTime;
	unsigned long cguestTime;

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
} procstat;

//char* parseProcStatRecord(char* buffer, char* endPtr, procstat* procData, recordTime* RecordTime, char* pathBuffer, char* cwdBuffer);
//int writeProcStatRecord(FILE*, procstat*, long clockTicksPerSec);
int compareProcData(procdata*, procdata*);

class ProcFile {
public:
    ProcFile(const char* filename, const char* hostname, const char* identifier);
    bool write_procdata(procdata* start_ptr, int count);
    bool write_procstat(procstat* start_ptr, int count);
    ~ProcFile();
private:
    bool write_dataset(const char* dsName, hid_t type, void* start_pointer, int count, int chunkSize);
    hid_t file;
    hid_t hostGroup;
    hid_t idGroup;

    /* identifiers for string types */
    hid_t strType_exeBuffer;
    hid_t strType_buffer;

    /* identifiers for complex types */
    hid_t type_procdata;
    hid_t type_procstat;
};

class ProcFileException : public std::exception {
public:
    ProcFileException(const char* t_error) {
        error = t_error;
    }

    virtual const char* what() const throw() {
        return error;
    }
private:
    const char *error;
};

#endif /* PROCFMT_H_ */
