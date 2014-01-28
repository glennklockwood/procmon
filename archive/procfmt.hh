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
#include <string>

#include "hdf5.h"

#ifndef PROCFMT_H_
#define PROCFMT_H_

#define BUFFER_SIZE 1024
#define LBUFFER_SIZE 8192
#define EXEBUFFER_SIZE 256

typedef struct _procdata {
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
    unsigned int nextRec;
    unsigned int prevRec;
} procdata;

typedef struct _procstat {
	unsigned int pid;
    unsigned int nextRec;
    unsigned int prevRec;
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
} procstat;

typedef enum _ProcFileFormat {
    FILE_FORMAT_TEXT = 0,
    FILE_FORMAT_HDF5 = 1,
    FILE_FORMAT_INVALID = 2,
} ProcFileFormat;

typedef enum _ProcFileMode {
    FILE_MODE_READ = 0,
    FILE_MODE_WRITE = 1,
    FILE_MODE_INVALID = 2,
} ProcFileMode;

typedef enum _ProcFileRecordType {
    TYPE_PROCDATA = 0,
    TYPE_PROCSTAT = 1,
    TYPE_INVALID = 2,
} ProcFileRecordType;

//char* parseProcStatRecord(char* buffer, char* endPtr, procstat* procData, recordTime* RecordTime, char* pathBuffer, char* cwdBuffer);
//int writeProcStatRecord(FILE*, procstat*, long clockTicksPerSec);
int compareProcData(procdata*, procdata*);

class ProcFile {
public:
    ProcFile(const char* filename, const char* hostname, const char* identifier, ProcFileFormat format, ProcFileMode mode);
    ~ProcFile();

    bool write_procdata(procdata* start_ptr, int count);
    bool write_procstat(procstat* start_ptr, int count);

    unsigned int read_procdata(procdata* procData, unsigned int id);
    unsigned int read_procstat(procstat* procStat, unsigned int id);
    unsigned int read_procdata(procdata* start_ptr, unsigned int start_id, unsigned int count);
    unsigned int read_procstat(procstat* start_ptr, unsigned int start_id, unsigned int count);
    
    ProcFileRecordType read_stream_record(procdata* procData, procstat* procStat);

private:
    std::string filename;
    std::string hostname;
    std::string identifier;
    ProcFileFormat format;
    ProcFileMode mode;

    /* Text-specific private values and methods */
    FILE* filePtr;
    char buffer[LBUFFER_SIZE];
    char *ptr;
    char *sPtr;
    char* ePtr;
    void text_open_read();
    void text_open_write();
    bool text_read_procdata(procdata* procData);
    bool text_read_procstat(procstat* procStat);
    int text_write_procstat(procstat* procStat);
    int text_write_procdata(procdata* procData);
    ProcFileRecordType text_read_record(procdata* procData, procstat* procStat);
    int text_fill_buffer();

    /* HDF5-specfic private values and methods */
    void hdf5_open_read();
    void hdf5_open_write();
    void hdf5_initialize_types();
    unsigned int hdf5_write_dataset(const char* dsName, hid_t type, void* start_pointer, int count, int chunkSize);
    unsigned int hdf5_read_dataset(const char* dsName, hid_t type, void* start_pointer, unsigned int start_id, unsigned int count);
    unsigned int hdf5_write_procdata(procdata* start_ptr, unsigned int count);
    unsigned int hdf5_write_procstat(procstat* start_ptr, unsigned int count);
    unsigned int hdf5_read_procdata(procdata* start_ptr, unsigned int start_id, unsigned int count);
    unsigned int hdf5_read_procstat(procstat* start_ptr, unsigned int start_id, unsigned int count);
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
