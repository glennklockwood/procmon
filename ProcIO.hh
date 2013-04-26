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

#include <amqp_tcp_socket.h>
#include <amqp_framing.h>

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
    TYPE_INVALID = 2,
} ProcRecordType;

class ProcIO {
public:
	ProcIO() { contextSet = false; }
    virtual bool set_context(const std::string& hostname, const std::string& identifier, const std::string& subidentifier);
    virtual int write_procdata(procdata* start_ptr, int count);
    virtual int write_procstat(procstat* start_ptr, int count);
protected:
	bool contextSet;
    std::string identifier;
    std::string subidentifier;
    std::string hostname;
};

class ProcTextIO : protected ProcIO {
public:
    ProcTextIO(const std::string& filename, ProcIOFileMode mode);
    ~ProcTextIO();
    virtual bool set_context(const std::string& hostname, const std::string& identifier, const std::string& subidentifier);
    virtual int write_procdata(procdata* start_ptr, int count);
    virtual int write_procstat(procstat* start_ptr, int count);
    ProcRecordType read_stream_record(procdata* procData, procstat* procStat);
private:
	int fill_buffer();
	bool read_procstat(procstat*);
	bool read_procdata(procdata*);

    std::string filename;
    ProcIOFileMode mode;
	FILE *filePtr;
	char *sPtr, *ePtr, *ptr;
	char buffer[TEXT_BUFFER_SIZE];
};

class ProcHDF5IO : protected ProcIO {
public:
    ProcHDF5IO(const std::string& filename, ProcIOFileMode mode);
    ~ProcHDF5IO();
    virtual bool set_context(const std::string& hostname, const std::string& identifier, const std::string& subidentifier);
    virtual int write_procdata(procdata* start_ptr, int count);
    virtual int write_procstat(procstat* start_ptr, int count);
    unsigned int read_procdata(procdata* procData, unsigned int id);
    unsigned int read_procstat(procstat* procStat, unsigned int id);
    unsigned int read_procdata(procdata* start_ptr, unsigned int start_id, unsigned int count);
    unsigned int read_procstat(procstat* start_ptr, unsigned int start_id, unsigned int count);
private:
    unsigned int read_dataset(const char* dsName, hid_t type, void* start_pointer, unsigned int start_id, unsigned int count);
    unsigned int write_dataset(const char* dsName, hid_t type, void* start_pointer, int count, int chunkSize);
    void initialize_types();

    std::string filename;
	std::string combinedId;
    ProcIOFileMode mode;
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
    ProcAMQPIO(const std::string& _mqServer, int _port, const std::string& _mqVHost, const std::string& _username, const std::string& _password, const std::string& _exchangeName, const int _frameSize, const ProcIOFileMode _mode);
    ~ProcAMQPIO();
    virtual bool set_context(std::string& hostname, std::string& identifier, std::string& subidentifier);
    virtual int write_procdata(procdata* start_ptr, int count);
    virtual int write_procstat(procstat* start_ptr, int count);
    //ProcFileRecordType read_stream_record(procdata* procData, procstat* procStat, int& nRec);
private:
	bool _amqp_open();
	bool _amqp_eval_status(amqp_rpc_reply_t _status);

    std::string mqServer;
    int port;
    std::string mqVHost;
	std::string username;
	std::string password;
    std::string exchangeName;
    int frameSize;
    ProcIOFileMode mode;

	bool connected;
	amqp_connection_state_t conn;
	amqp_socket_t* socket;
	amqp_rpc_reply_t status;

	bool amqpError;
	std::string amqpErrorMessage;

	char sendBuffer[AMQP_BUFFER_SIZE];
};

class ProcIOException : public std::exception {
public:
	ProcIOException(const std::string& err): error(err) {
	}

	virtual const char* what() const throw() {
		return error.c_str();
	}

	~ProcIOException() throw() { }
private:
    std::string error;
};

#endif /* PROCFMT_H_ */
