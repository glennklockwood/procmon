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

/*
 * procfmt.h
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
#include <map>
#include <vector>

#include "config.h"

#ifdef USE_HDF5
#include "hdf5.h"
#endif /* USE_HDF5 */

#ifdef USE_AMQP
#include <amqp_tcp_socket.h>
#include <amqp_framing.h>
#endif

#include "ProcData.hh"

#define AMQP_BUFFER_SIZE 3145728 /* 3 MB */
#define TEXT_BUFFER_SIZE 8192    /* 8 kB */

typedef enum _ProcIOFormat {
    IO_FORMAT_INVALID = 0,
    IO_FORMAT_TEXT = 1,
    IO_FORMAT_HDF5 = 2,
    IO_FORMAT_AMQP = 4,
} ProcIOFormat;

typedef enum _ProcIOFileMode {
    FILE_MODE_READ = 0,
    FILE_MODE_WRITE = 1,
    FILE_MODE_INVALID = 2,
} ProcIOFileMode;

typedef enum _ProcRecordType {
    TYPE_PROCDATA = 0,
    TYPE_PROCSTAT = 1,
    TYPE_PROCFD = 2,
    TYPE_PROCOBS = 3,
    TYPE_INVALID = 4,
} ProcRecordType;

using namespace std;

class ProcIO {
public:
	ProcIO();
	virtual ~ProcIO();
    virtual bool set_context(const string& hostname, const string& identifier, const string& subidentifier) = 0;
    virtual unsigned int write_procdata(procdata* start_ptr, int count) = 0;
    virtual unsigned int write_procstat(procstat* start_ptr, int count) = 0;
    virtual unsigned int write_procfd(procfd* start_ptr, int count) = 0;
protected:
	bool contextSet;
    string identifier;
    string subidentifier;
    string hostname;
};

class ProcTextIO : public ProcIO {
public:
    ProcTextIO(const string& _filename, ProcIOFileMode _mode);
    virtual ~ProcTextIO();
    virtual bool set_context(const string& hostname, const string& identifier, const string& subidentifier);
    virtual unsigned int write_procdata(procdata* start_ptr, int count);
    virtual unsigned int write_procstat(procstat* start_ptr, int count);
    virtual unsigned int write_procfd(procfd* start_ptr, int count);
    ProcRecordType read_stream_record(procdata* procData, procstat* procStat, procfd* procFD);
private:
	int fill_buffer();
	bool read_procstat(procstat*);
	bool read_procdata(procdata*);
    bool read_procfd(procfd*);

    string filename;
    ProcIOFileMode mode;
	FILE *filePtr;
	char *sPtr, *ePtr, *ptr;
	char buffer[TEXT_BUFFER_SIZE];
};

#ifdef USE_HDF5
class hdf5Ref {
	friend class ProcHDF5IO;

public:
	hdf5Ref(hid_t file, hid_t type_procstat, hid_t type_procdata, hid_t type_procfd, hid_t type_procobs, const std::string& hostname, ProcIOFileMode mode, unsigned int statBlockSize, unsigned int dataBlockSize, unsigned int fdBlockSize, unsigned int obBlockSize);
	~hdf5Ref();

private:
	unsigned int open_dataset(const char* dsName, hid_t type, int chunkSize, hid_t *dataset, hid_t *attribute, unsigned int zip_level);
	hid_t group;
	hid_t procstatDS;
	hid_t procdataDS;
    hid_t procfdDS;
    hid_t procobsDS;
	hid_t procstatSizeID;
	hid_t procdataSizeID;
    hid_t procfdSizeID;
    hid_t procobsSizeID;
	unsigned int procstatSize;
	unsigned int procdataSize;
    unsigned int procfdSize;
    unsigned int procobsSize;
	time_t lastUpdate;
};

class ProcHDF5IO : public ProcIO {
public:
    ProcHDF5IO(const string& filename, ProcIOFileMode mode, unsigned int statBlockSize=DEFAULT_STAT_BLOCK_SIZE, unsigned int dataBlockSize=DEFAULT_DATA_BLOCK_SIZE, unsigned int fdBlockSize=DEFAULT_FD_BLOCK_SIZE, unsigned int obsBlockSize=DEFAULT_OBS_BLOCK_SIZE);
    virtual ~ProcHDF5IO();
    virtual bool set_context(const string& hostname, const string& identifier, const string& subidentifier);
    virtual unsigned int write_procdata(procdata* start_ptr, int count);
    virtual unsigned int write_procstat(procstat* start_ptr, int count);
    virtual unsigned int write_procfd(procfd* start_ptr, int count);
    virtual unsigned int write_procdata(procdata* start_ptr, unsigned int start_id, int count);
    virtual unsigned int write_procstat(procstat* start_ptr, unsigned int start_id, int count);
    virtual unsigned int write_procfd(procfd* start_ptr, unsigned int start_id, int count);
    virtual unsigned int write_procobs(procobs* start_ptr, unsigned int start_id, int count);
    unsigned int read_procdata(procdata* procData, unsigned int id);
    unsigned int read_procstat(procstat* procStat, unsigned int id);
    unsigned int read_procfd(procfd* procFD, unsigned int id);
    unsigned int read_procobs(procobs* procObs, unsigned int id);
    unsigned int read_procdata(procdata* start_ptr, unsigned int start_id, unsigned int count);
    unsigned int read_procstat(procstat* start_ptr, unsigned int start_id, unsigned int count);
    unsigned int read_procfd(procfd* start_ptr, unsigned int start_id, unsigned int count);
    unsigned int read_procobs(procobs* start_ptr, unsigned int start_id, unsigned int count);
	unsigned int get_nprocdata();
	unsigned int get_nprocstat();
	unsigned int get_nprocfd();
	unsigned int get_nprocobs();

    bool metadata_set_string(const char*, const char*);
    bool metadata_set_uint(const char*, unsigned long);
    bool metadata_get_string(const char*, char**);
    bool metadata_get_uint(const char*, unsigned long*);

    void set_override_context(bool);
    bool get_hosts(vector<string>& hosts);
	void flush();
	void trim_segments(time_t cutoff);
private:
    unsigned int read_dataset(ProcRecordType recordType, hid_t type, void* start_pointer, unsigned int start_id, unsigned int count);
    unsigned int write_dataset(ProcRecordType recordType, hid_t type, void* start_pointer, unsigned int start_id, int count, int chunkSize);
    void initialize_types();

    string filename;
    ProcIOFileMode mode;
    hid_t file;
    hid_t root;
	map<string,hdf5Ref*> openRefs;
	hdf5Ref* hdf5Segment;

    /* identifiers for string types */
    hid_t strType_exeBuffer;
    hid_t strType_buffer;
	hid_t strType_idBuffer;
    hid_t strType_variable;

    /* identifiers for complex types */
    hid_t type_procdata;
    hid_t type_procstat;
    hid_t type_procfd;
    hid_t type_procobs;

	unsigned int dataBlockSize;
	unsigned int statBlockSize;
	unsigned int fdBlockSize;
	unsigned int obsBlockSize;
    bool override_context;
};
#endif

#ifdef USE_AMQP
class ProcAMQPIO : public ProcIO {
public:
    ProcAMQPIO(const string& _mqServer, int _port, const string& _mqVHost, const string& _username, const string& _password, const string& _exchangeName, const int _frameSize, const ProcIOFileMode _mode);
    virtual ~ProcAMQPIO();
    virtual bool set_context(const string& hostname, const string& identifier, const string& subidentifier);
    virtual unsigned int write_procdata(procdata* start_ptr, int count);
    virtual unsigned int write_procstat(procstat* start_ptr, int count);
    virtual unsigned int write_procfd(procfd* start_ptr, int count);
    bool set_queue_name(const string& queue_name);
    ProcRecordType read_stream_record(void **data, size_t *pool_size, int *nRec, long usecTimeout=0);
	bool get_frame_context(string& _hostname, string& _identifier, string& _subidentifier);
private:
    bool _amqp_open();
    bool _amqp_close(bool);
    bool _amqp_bind_context();
	bool _amqp_eval_status(amqp_rpc_reply_t _status);

	bool _read_procstat(procstat *startPtr, int nRecords, const char* buffer, int nBytes);
	bool _read_procdata(procdata *startPtr, int nRecords, const char* buffer, int nBytes);
	bool _read_procfd(procfd *startPtr, int nRecords, const char* buffer, int nBytes);
	bool _set_frame_context(const string& routingKey);

	bool _read_procstat(procstat*, int, char*, int);
	bool _read_procdata(procdata*, int, char*, int);
	bool _read_procfd(procfd*, int, char*, int);

    bool _send_message(const char *tag, amqp_bytes_t& message);

    string mqServer;
    int port;
    string mqVHost;
	string username;
	string password;
    string exchangeName;
    int frameSize;
    ProcIOFileMode mode;

	bool connected;
	amqp_connection_state_t conn;
	amqp_socket_t* socket;
	amqp_rpc_reply_t status;
	bool queueConnected;
    string queue_name;

	string frameHostname;
	string frameIdentifier;
	string frameSubidentifier;
	string frameMessageType;

	bool amqpError;
	string amqpErrorMessage;

	char buffer[AMQP_BUFFER_SIZE];
};
#endif

class ProcIOException : public exception {
public:
	ProcIOException(const string& err): error(err) {
	}

	virtual const char* what() const throw() {
		return error.c_str();
	}

	~ProcIOException() throw() { }
private:
    string error;
};

#endif /* PROCFMT_H_ */

