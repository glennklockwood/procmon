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

#ifndef __PROCIO_HH_
#define __PROCIO_HH_

#include <stdio.h>
#include <time.h>
#include <unistd.h>
#include <exception>
#include <string>
#include "hdf5.h"

typedef enum _ProcFileFormat {
    FILE_FORMAT_INVALID = 0,
    FILE_FORMAT_TEXT = 1,
    FILE_FORMAT_HDF5 = 2,
    FILE_FORMAT_AMQP = 4,
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

class ProcIO {
public:
    virtual bool set_context(std::string& hostname, std::string& identifier, std::string& subidentifier);
    virtual bool write_procdata(procdata* start_ptr, int count);
    virtual bool write_procstat(procstat* start_ptr, int count);
protected:
    bool format_data(char* buffer, int bufferLen, procdata* start_ptr, int count, int headerLevel);
    bool format_data(char* buffer, int bufferLen, procstat* start_ptr, int count, int headerLevel);

    std::string identifier;
    std::string subidentifier;
    std::string hostname;
};

class ProcTextIO : protected ProcIO {
public:
    ProcTextIO(std::string& filename, ProcFileMode mode);
    ~ProcTestIO();
    virtual bool set_context(std::string& hostname, std::string& identifier, std::string& subidentifier);
    virtual bool write_procdata(procdata* start_ptr, int count);
    virtual bool write_procstat(procstat* start_ptr, int count);
    ProcFileRecordType read_stream_record(procdata* procData, procstat* procStat);
private:
    std::string filename;
    ProcFileMode mode;
};

class ProcHDF5IO : protected ProcIO {
public:
    ProcHDF5IO(std::string& filename, ProcFileMode mode);
    ~ProcHDF5IO();
    virtual bool set_context(std::string& hostname, std::string& identifier, std::string& subidentifier);
    virtual bool write_procdata(procdata* start_ptr, int count);
    virtual bool write_procstat(procstat* start_ptr, int count);
    unsigned int read_procdata(procdata* procData, unsigned int id);
    unsigned int read_procstat(procstat* procStat, unsigned int id);
    unsigned int read_procdata(procdata* start_ptr, unsigned int start_id, unsigned int count);
    unsigned int read_procstat(procstat* start_ptr, unsigned int start_id, unsigned int count);
private:
    unsigned int read_dataset(const char* dsName, hid_t type, void* start_pointer, unsigned int start_id, unsigned int count);
    unsigned int write_dataset(const char* dsName, hid_t type, void* start_pointer, int count, int chunkSize);
    void open_read();
    void open_write();
    void initialize_types();

    std::string filename;
    ProcFileMode mode;
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

class ProcAMQPIO : protected ProcIO {
public:
    ProcAMQPIO(std::string& mqHost, int port, std::string& mqHVost, std::string& exchangeName, int frameSize, ProcFileMode mode);
    ~ProcAMQPIO();
    virtual bool set_context(std::string& hostname, std::string& identifier, std::string& subidentifier);
    virtual bool write_procdata(procdata* start_ptr, int count);
    virtual bool write_procstat(procstat* start_ptr, int count);
    ProcFileRecordType read_stream_record(procdata* procData, procstat* procStat, int& nRec);
private:
    std::string mqServer;
    int port;
    std::string mqVHost;
    std::string exchangeName;
    int frameSize;
    ProcFileMode mode;
};

#endif /* PROCFMT_H_ */
