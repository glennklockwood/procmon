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
#include <algorithm>
#include <exception>
#include <string>
#include <map>
#include <unordered_map>
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

    Context() {
    }

    Context(const string &_system, const string &_hostname,
            const string &_identifier, const string &_subidentifier)
    {
        size_t endPos = _system.find('.');
        endPos = endPos == string::npos ? _system.size() : endPos;
        system = string(_system, 0, endPos);

        endPos = _hostname.find('.');
        endPos = endPos == string::npos ? _hostname.size() : endPos;
        hostname = string(_hostname, 0, endPos);

        endPos = _identifier.find('.');
        endPos = endPos == string::npos ? _identifier.size() : endPos;
        identifier = string(_identifier, 0, endPos);

        endPos = _subidentifier.find('.');
        endPos = endPos == string::npos ? _subidentifier.size() : endPos;
        subidentifier = string(_subidentifier, 0, endPos);

        contextString = system + "." + hostname + "." + identifier + "." + subidentifier;
    }

    bool operator==(const Context &other) const {
        return contextString == other.contextString;
    }
};

struct DatasetContext {
    Context context;
    string datasetName;

    DatasetContext(const Context& _context, const string& _datasetName):
        context(_context), datasetName(_datasetName)
    {
    }

    bool operator==(const DatasetContext& other) const {
        return context == other.context && datasetName == other.datasetName;
    }
};
};

namespace std {
    template <>
    struct hash<pmio2::Context> {
        size_t operator()(const pmio2::Context &k) const {
            return hash<string>()(k.contextString);
        }
    };
    template <>
    struct hash<pmio2::DatasetContext> {
        size_t operator()(const pmio2::DatasetContext &k) const {
            return (hash<pmio2::Context>()(k.context) ^ (hash<string>()(k.datasetName) << 1)) >> 1;
        }
    };
}

namespace pmio2 {

class Dataset;
class DatasetFactory {
    public:
    virtual shared_ptr<Dataset> operator()() = 0;
    virtual ~DatasetFactory() = 0;
};

class IoMethod {
    public:
	IoMethod() {
        contextSet = false;
        contextOverride = false;
    }
    virtual ~IoMethod() { }
    virtual bool setContext(const string& _system, const string& _hostname, const string& _identifier, const string& _subidentifier) {
        Context context(_system, _hostname, _identifier, _subidentifier);
        setContext(context);
    }
    virtual bool setContext(const Context &context) {
        this->context = context;
        return true;
    }
    virtual bool setContextOverride(bool _override) {
       contextOverride = _override;
    } 

    virtual bool addDataset(shared_ptr<Dataset> ptr, const string &dsName, shared_ptr<DatasetFactory> &dsGen) {
        auto ds = find_if(
                registeredDatasets.begin(),
                registeredDatasets.end(),
                [dsName](pair<string,shared_ptr<DatasetFactory> >& e) { return dsName == e.first; }
        );
        if (ds == registeredDatasets.end()) {
            registeredDatasets.emplace_back(dsName, dsGen);
            return true;
        }
        return false;
    }
    virtual bool addDatasetContext(shared_ptr<Dataset> ptr, const Context &context, const string &dsetName) {
        DatasetContext key(context, dsetName);
        dsMap[key] = ptr;
        return true;
    }

    protected:
	bool contextSet;
    bool contextOverride;
    Context context;
    unordered_map<DatasetContext, shared_ptr<Dataset> > dsMap;
    unordered_map<string, shared_ptr<Dataset> > currentDatasets;
    vector<pair<string,shared_ptr<DatasetFactory> > > registeredDatasets;
};

class Dataset : public enable_shared_from_this<Dataset> {
    public:
    Dataset(shared_ptr<IoMethod> &_ioMethod, const string &dsName) {
        ioMethod = _ioMethod;
    }
    virtual ~Dataset() { }

    protected:
    shared_ptr<IoMethod> ioMethod;
};


/*
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
*/

#ifdef USE_HDF5

class Hdf5Io;

template <class pmType>
class Hdf5Type {
    public:
    Hdf5Type(shared_ptr<Hdf5Io> &io) {
        initializeType(io);
    }
    ~Hdf5Type() {
        if (set) {
            H5Tclose(type);
            set = false;
        }
    }

    protected:
    bool set;
    hid_t type;

    void initializeType(shared_ptr<Hdf5Io> &io) {
        // no-op for default, must be specialized
        set = false;
    }
};

class Hdf5Group {
    public:
    Hdf5Group(shared_ptr<Hdf5Io> &hdf5File, const string &groupName);
    ~Hdf5Group() {
        H5Gclose(group);
    }
    private:
    hid_t group;
    bool set;
};

template <class pmType>
class Hdf5Dataset: public Dataset {
    public:
    Hdf5Dataset(
            shared_ptr<Hdf5Io> &_ioMethod,
            shared_ptr<Hdf5Type<pmType> > &_h5type,
            unsigned int &_blockSize,
            unsigned int &_zipLevel,
            const string &_dsName
    );
    ~Hdf5Dataset() {
        if (size_id >= 0) {
            H5Aclose(size_id);
        }
        if (dataset >= 0) {
            H5Dclose(dataset);
        }
    }

    size_t write(pmType *start, pmType *end);
    size_t read(pmType *start, size_t maxRead);
    inline const size_t howmany() const { return size; }

    protected:
    shared_ptr<Hdf5Type<pmType> > type;
    shared_ptr<Hdf5Group> group;
    string dsName;
    int zipLevel;
    hid_t dataset;
    hid_t size_id;
    unsigned int blockSize;
    size_t size;
    time_t lastUpdate;

    void initializeDataset();
};

template <class pmType>
class Hdf5DatasetFactory : public DatasetFactory {
    public:
    Hdf5DatasetFactory(
        shared_ptr<Hdf5Io> &_ioMethod,
        shared_ptr<Hdf5Type<pmType> > &_h5type,
        unsigned int &_blockSize,
        unsigned int &_zipLevel,
        const string &_dsName
    ):
        ioMethod(_ioMethod), h5type(_h5type), blockSize(_blockSize),
        zipLevel(_zipLevel), dsName(_dsName)
    { }

    shared_ptr<Dataset> operator()() {
        shared_ptr<Dataset> ptr(new Hdf5Dataset<pmType>(
            ioMethod, h5type, blockSize, zipLevel, dsName
        ));
        return ptr;
    }

    private:
    shared_ptr<Hdf5Io> ioMethod;
    shared_ptr<Hdf5Type<pmType> > h5type;
    unsigned int blockSize;
    unsigned int zipLevel;
    string dsName;
};


class Hdf5Io : public IoMethod {
    friend class Hdf5Type<class T>;
    friend class Hdf5Group;
    public:
    Hdf5Io(const string& filename, IoMode mode);
    ~Hdf5Io();
    bool metadataSetString(const char*, const char*);
    bool metadataSetUint(const char*, unsigned long);
    bool metadataGetString(const char*, char**);
    bool metadataGetUint(const char*, unsigned long*);

    template <class pmType>
    size_t write(const string &dsName, pmType *start, pmType *end);

    template <class pmType>
    size_t read(const string &dsName, pmType *start, size_t count);

    size_t howmany(const string &dsName);



    const vector<string>& getGroups();
    const shared_ptr<Hdf5Group> getCurrentGroup();
	void flush();
	void trimDatasets(time_t cutoff);

    protected:
    void initializeTypes();

    string filename;
    IoMode mode;
    hid_t file;
    hid_t root;
    unordered_map<Context,shared_ptr<Hdf5Group> > groups;

    public:
    /* identifiers for string types */
    hid_t strType_exeBuffer;
    hid_t strType_buffer;
	hid_t strType_idBuffer;
    hid_t strType_variable;
};

template <class pmType>
Hdf5Dataset<pmType>::Hdf5Dataset(
            shared_ptr<Hdf5Io> &_ioMethod,
            shared_ptr<Hdf5Type<pmType> > &_h5type,
            unsigned int &_blockSize,
            unsigned int &_zipLevel,
            const string &_dsName
):
    Dataset(_ioMethod, _dsName), dsName(_dsName)
{
    type = _h5type;
    blockSize = _blockSize;
    zipLevel = _zipLevel;
    lastUpdate = 0;
    group = _ioMethod->getCurrentGroup();
    initializeDataset();

}


#endif

#ifdef USE_AMQP

/*
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
    IoMode mode;

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
*/
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

