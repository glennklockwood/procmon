/*
 * ProcIO2.hh
 *
 * Author: Douglas Jacobsen <dmjacobsen@lbl.gov>, NERSC User Services Group
 * 2013/02/17
 * Copyright (C) 2012, The Regents of the University of California
 *
 * The purpose of the procmon is to read data from /proc for an entire process tree
 * and save that data at intervals longitudinally
 */

#ifndef __PROCIO2_HH_
#define __PROCIO2_HH_

#include <stdio.h>
#include <time.h>
#include <unistd.h>
#include <exception>
#include <string>
#include <map>
#include <vector>
#include <memory>

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

using namespace std;

namespace pmio2 {

enum IoMode {
    MODE_READ = 0,
    MODE_WRITE = 1,
    MODE_INVALID = 2,
};

struct Context {
    string system;
    string hostname;
    string identifier;
    string subidentifier;
    string contextString;

    Context(const string &_system, const string &_hostname,
            const string &_identifier, const string &_subidentifier):
        system(_system), hostname(_hostname), identifier(_identifier),
        subidentifier(_subidentifier)
    {
        contextString = system + "." + hostname + "." + identifier + "." + subidentifier;
    };
};

}

namespace std {
    template <>
    struct hash<pmio2::Context> {
        size_t operator()(const pmio2::Context &k) const {
            return hash<string>()(k.contextString);
        }
    };
}

namespace pmio2 {

class Dataset;

class IoMethod {
    public:
	IoMethod() {
        contextSet = false;
        contextOverride = false;
    }
    virtual bool setContext(const string& _system, const string& _hostname, const string& _identifier, const string& _subidentifier) {
        Context context(_system, _hostname, _identifier, _subidentifier);
        setContext(context);
    }
    virtual bool setContext(const Context &context) {
        this.context = context;
        return true;
    }
    virtual bool setContextOverride(bool _override) {
       contextOverride = _override;
    } 

    virtual bool addDataset(shared_ptr<Dataset> ptr, const Context &context, const string &dataset) {
        pair<Context, string> key(context, dataset);
        dsMap[key] = ptr;
        auto ds = find(registeredDatasets.begin(), registeredDatasets.end(), dsName);
        if (ds == registeredDatasets.end()) {
            registeredDatasets.push_back(dsName);
        }
        return true;
    }

    protected:
	bool contextSet;
    bool contextOverride;
    Context context;
    unordered_map<pair<Context, string>, shared_ptr<Dataset> > dsMap;
    vector<shared_ptr<Dataset> > currentDatasets;
    vector<string> registeredDatasets;
};

class Dataset : public enable_shared_from_this<Dataset> {
    public:
    Dataset(shared_ptr<IoMethod> &_ioMethod, const Context &context, const string &dsName) {
        ioMethod = _ioMethod;
        ioMethod->addDataset(shared_from_this(), dsName);
    }

    protected:
    shared_ptr<IoMethod> ioMethod;
};

class TextIO : public IoMethod {
public:
    TextIO(const string& _filename, IoMode _mode);
    ~TextIO();
private:
	int fill_buffer();

    string filename;
    IoMode mode;
	FILE *filePtr;
	char *sPtr, *ePtr, *ptr;
	char buffer[TEXT_BUFFER_SIZE];
};

#ifdef USE_HDF5
class Hdf5Type {
    friend class Hdf5DatasetBase;
    public:

    protected:
    hid_t type;
};

class Hdf5DatasetBase : public Dataset {
    public:
    Hdf5DatasetBase(
            shared_ptr<Hdf5File> &_ioMethod,
            shared_ptr<Hdf5Type> &_h5type,
            unsigned int &_blockSize,
            const Context &_context,
            const string &_dsName
    ):
        Dataset(_ioMethod, _context, _dsName)
    {
        type = _h5type;
        blockSize = _blockSize;
    }

    protected:
    shared_ptr<Hdf5Type> type;
    hid_t group;
    hid_t dataset;
    hid_t size_id;
    unsigned int blockSize;
    unsigned int size;
    time_t lastUpdate;
};

class Hdf5Io : public IoMethod {
    public:
    Hdf5Io(const string& filename, IoMode mode);
    ~Hdf5Io();
    bool metadata_set_string(const char*, const char*);
    bool metadata_set_uint(const char*, unsigned long);
    bool metadata_get_string(const char*, char**);
    bool metadata_get_uint(const char*, unsigned long*);

    bool get_hosts(vector<string>& hosts);
	void flush();
	void trim_segments(time_t cutoff);

    protected:
    unsigned int read_dataset(ProcRecordType recordType, hid_t type, void* start_pointer, unsigned int start_id, unsigned int count);
    unsigned int write_dataset(ProcRecordType recordType, hid_t type, void* start_pointer, unsigned int start_id, int count, int chunkSize);
    void initialize_types();

    string filename;
    ProcIOFileMode mode;
    hid_t file;
    hid_t root;

    /* identifiers for string types */
    hid_t strType_exeBuffer;
    hid_t strType_buffer;
	hid_t strType_idBuffer;
    hid_t strType_variable;
};
#endif

#ifdef USE_AMQP

template <typename pmType>
class AmqpDataset : public Dataset {
    public:
    AmqpDataset(shared_ptr<AmqpIo> _amqp, const Context &context, const string &dsName):
        Dataset(_amqp, context)
    {
        amqp = _amqp;
    }

    unique_ptr<ProcmonDataset> read(size_t &nElements, void *buffer, size_t nBytes);

    template <typename Iterator>
    size_t write(Iterator begin, Iterator end);


class AmqpProcstat : public AmqpData {
    public:
    AmqpProcstat(shared_ptr<AmqpIo> amqp, const Context &context);

    template <typename Iterator>
    virtual size_t write(Iterator begin, Iterator end);

    private:
    shared_ptr<AmqpIo> amqp;

};

class AmqpIo : public IoMethod {
    public:
    AmqpIo(const string &_mqServer, int _port, const string &_mqVHost, const string &_username, const string &_password, const string &_exchangeName, const int _frameSize, IoMode mode);
    ~AmqpIo();
    bool set_queue_name(const string& queue_name);
    ProcRecordType read_stream_record(void **data, size_t *pool_size, int *nRec);
	bool get_frame_context(string& _hostname, string& _identifier, string& _subidentifier);

    private:
    bool _amqp_open();
    bool _amqp_close(bool);
    bool _amqp_bind_context();
	bool _amqp_eval_status(amqp_rpc_reply_t _status);

	bool _set_frame_context(const string& routingKey);

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

class IoException : public exception {
public:
	IoException(const string& err): error(err) {
	}

	virtual const char* what() const throw() {
		return error.c_str();
	}

	~IoException() throw() { }
private:
    string error;
};

}

#endif /* PROCFMT_H_ */

