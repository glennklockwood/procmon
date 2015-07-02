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

#include <iostream>

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

//! Flag values to determine IO access mode
enum IoMode {
    MODE_READ = 0,    //!< Read-only access
    MODE_WRITE = 1,   //!< Write access
    MODE_INVALID = 2, //!< Invalid mode
};

//! The metadata context of collected/monitored data
struct Context {
    string system;          //!< Overall system where data were collected
    string hostname;        //!< Hostname where data were collected
    string identifier;      //!< Primary tag
    string subidentifier;   //!< Secondary tag
    string contextString;   //!< Automatically generated summary string

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
    virtual ~DatasetFactory() { }
};

class IoMethod {
    public:
	IoMethod() {
        contextSet = false;
        contextOverride = false;
    }
    virtual ~IoMethod() { }
    bool setContext(const string& _system, const string& _hostname, const string& _identifier, const string& _subidentifier) {
        Context context(_system, _hostname, _identifier, _subidentifier);
        setContext(context);
    }
    virtual bool setContext(const Context &context) {
        this->context = context;
        return true;
    }
    virtual const Context &getContext() {
        return context;
    }

    virtual bool setContextOverride(bool _override) {
       contextOverride = _override;
    } 
    virtual const bool getContextOverride() const {
        return contextOverride;
    }

    virtual bool addDataset(const string &dsName, shared_ptr<DatasetFactory> dsGen) {
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

    virtual const bool writable() const  = 0;

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
    Dataset(shared_ptr<IoMethod> _ioMethod, const string &dsName) {
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
struct Hdf5TypeFactory {
    hid_t operator()(shared_ptr<Hdf5Io> io) {
        return -1;
    }
};

template <class pmType>
class Hdf5Type {
    public:
    Hdf5Type(shared_ptr<Hdf5Io> io) {
        type = initializeType(io);
    }
    Hdf5Type(shared_ptr<Hdf5Io> io, Hdf5TypeFactory<pmType>& factory) {
        type = factory(io);
    }
    ~Hdf5Type() {
        if (set) {
            H5Tclose(type);
            set = false;
        }
    }
    const hid_t getType() {
        return type;
    }

    protected:
    bool set;
    hid_t type;

    hid_t initializeType(shared_ptr<Hdf5Io> io);
};

class Hdf5Group {
    public:
    Hdf5Group(Hdf5Io &hdf5File, const string &groupName);
    ~Hdf5Group() {
        H5Gclose(group);
    }
    const hid_t getGroup() {
        return group;
    }
    private:
    hid_t group;
    bool set;
};

template <class pmType>
class Hdf5Dataset: public Dataset {
    public:
    Hdf5Dataset(
            shared_ptr<Hdf5Io> _ioMethod,
            shared_ptr<Hdf5Type<pmType> > _h5type,
            unsigned int _maxSize, // 0 for unlimited
            unsigned int _blockSize, // 0 for non-chunked data
            unsigned int _zipLevel,
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

    size_t write(pmType *start, pmType *end, size_t start_id = 0, bool append = true);
    size_t read(pmType *start, size_t maxRead, size_t start_id = 0);
    inline const size_t howmany() const { return size; }

    protected:
    shared_ptr<Hdf5Type<pmType> > type;
    shared_ptr<Hdf5Group> group;
    string dsName;
    int zipLevel;
    hid_t dataset;
    hid_t size_id;
    unsigned int blockSize;
    unsigned int maxSize;
    size_t size;
    time_t lastUpdate;

    size_t initializeDataset();
};


template <class pmType>
class Hdf5DatasetFactory : public DatasetFactory {
    public:
    Hdf5DatasetFactory(
        shared_ptr<Hdf5Io> _ioMethod,
        shared_ptr<Hdf5Type<pmType> > _h5type,
        unsigned int _maxSize,
        unsigned int _blockSize,
        unsigned int _zipLevel,
        const string &_dsName
    ):
        ioMethod(_ioMethod), h5type(_h5type), maxSize(_maxSize),
        blockSize(_blockSize), zipLevel(_zipLevel), dsName(_dsName)
    { }

    shared_ptr<Dataset> operator()() {
        shared_ptr<Dataset> ptr(new Hdf5Dataset<pmType>(
            ioMethod, h5type, maxSize, blockSize, zipLevel, dsName
        ));
        return ptr;
    }

    private:
    shared_ptr<Hdf5Io> ioMethod;
    shared_ptr<Hdf5Type<pmType> > h5type;
    unsigned int maxSize;
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
    virtual bool setContext(const Context &context);
    bool metadataSetString(const char*, const char*);
    bool metadataSetUint(const char*, unsigned long);
    bool metadataGetString(const char*, char**);
    bool metadataGetUint(const char*, unsigned long*);

    template <class pmType>
    size_t write(const string &dsName, pmType *start, pmType *end, size_t start_id = 0, bool append = true);

    template <class pmType>
    size_t read(const string &dsName, pmType *start, size_t count, size_t start_id = 0);

    template <class pmType>
    size_t read(const string &dsName, pmType *start, size_t count) {
        return read(dsName, start, count, 0);
    }

    template <class pmType>
    size_t howmany(const string &dsName);

    virtual inline const bool writable() const {
        if (mode == IoMode::MODE_WRITE) {
            return true;
        }
    }

    const vector<string>& getGroups();
    const shared_ptr<Hdf5Group> getCurrentGroup() {
        return group;
    }
	void flush();
	void trimDatasets(time_t cutoff);

    protected:
    void initializeTypes();

    string filename;
    IoMode mode;
    hid_t file;
    hid_t root;
    unordered_map<Context,shared_ptr<Hdf5Group> > groups;
    shared_ptr<Hdf5Group> group;

    public:
    /* identifiers for string types */
    hid_t strType_exeBuffer;
    hid_t strType_buffer;
	hid_t strType_idBuffer;
    hid_t strType_variable;
};



#endif

#ifdef USE_AMQP_NEW

template <typename pmType> bool amqpEncode(const pmType *, const pmType *, string &);
template <typename pmType> size_t amqpDecode(const string &, pmType **);

template <typename pmType>
class AmqpDataset : public Dataset {
    public:
    AmqpDataset(
            shared_ptr<AmqpIo> _amqp,
            const Context &context,
            const string &dsName
    );
    AmqpDataset(
            shared_ptr<AmqpIo> _amqp,
            const Context &context,
            const string &dsName,
            function<int(pmType *, size_t n) streamRead_cb
    );

    virtual ~AmqpDataset();
    size_t write(pmType *start, pmType *end);

    private:
};



class AmqpIo : public IoMethod {
    public:
    AmqpIo(
            const string &_mqServer, int _port, const string &_mqVHost,
            const string &_username, const string &_password,
            const string &_exchangeName, const int _frameSize, IoMode mode
    );
    ~AmqpIo();

    bool setQueueName(const string& _queueName);

    virtual inline const bool writable() const {
        if (mode == IoMode::MODE_WRITE) {
            return true;
        }
    }

    template <class pmType>
    size_t write(const string &dsName, pmType *start, pmType *end, size_t start_id = 0);

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
    string queueName;

	bool amqpError;
	string amqpErrorMessage;
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

template <class pmType>
Hdf5Dataset<pmType>::Hdf5Dataset(
            shared_ptr<Hdf5Io> _ioMethod,
            shared_ptr<Hdf5Type<pmType> > _h5type,
            unsigned int _maxSize,
            unsigned int _blockSize,
            unsigned int _zipLevel,
            const string &_dsName
):
    Dataset(_ioMethod, _dsName), dsName(_dsName)
{
    type = _h5type;
    blockSize = _blockSize;
    maxSize = _maxSize;
    zipLevel = _zipLevel;
    lastUpdate = 0;
    size = 0;
    dataset = 0;
    size_id = 0;
    group = _ioMethod->getCurrentGroup();
    initializeDataset();

}

template <class pmType>
size_t Hdf5Dataset<pmType>::initializeDataset() {
    size = 0;
    const hid_t group_id = group->getGroup();
    if (group_id < 0) {
        IoException e("Called initializeDataset before group was opened!");
        throw &e;
    }
    if (H5Lexists(group_id, dsName.c_str(), H5P_DEFAULT) == 1) {
        dataset = H5Dopen2(group_id, dsName.c_str(), H5P_DEFAULT);
		size_id = H5Aopen(dataset, "nRecords", H5P_DEFAULT);
        hid_t attr_type = H5Aget_type(size_id);
		H5Aread(size_id, attr_type, &size);
        H5Tclose(attr_type);
    } else if (ioMethod->writable()) {
        hid_t param;
		hsize_t rank = 1;
        hsize_t initial_dims = 0;
        hsize_t maximal_dims = maxSize;
        if (maxSize == 0) {
            maximal_dims = H5S_UNLIMITED;
        }
        hid_t dataspace = H5Screate_simple(rank, &initial_dims, &maximal_dims);

        param = H5Pcreate(H5P_DATASET_CREATE);
        if (zipLevel > 0) {
            H5Pset_deflate(param, zipLevel);
        }
        if (blockSize > 0) {
            H5Pset_layout(param, H5D_CHUNKED);
    	    hsize_t chunk_dims = blockSize;
            H5Pset_chunk(param, rank, &chunk_dims);
        }
        dataset = H5Dcreate(group_id, dsName.c_str(), type->getType(), dataspace, H5P_DEFAULT, param, H5P_DEFAULT);
        H5Pclose(param);
        H5Sclose(dataspace);

        hid_t a_id = H5Screate(H5S_SCALAR);
		size_id = H5Acreate2(dataset, "nRecords", H5T_NATIVE_UINT, a_id, H5P_DEFAULT, H5P_DEFAULT);
		H5Awrite(size_id, H5T_NATIVE_UINT, &size);
        H5Sclose(a_id);
    } else {
        IoException e(string("Dataset ") + dsName + " doesn't exist and cannot be created (h5 file not writable");
        throw &e;
    }
	return size;
}

template <class pmType>
size_t Hdf5Dataset<pmType>::read(pmType *start_pointer, size_t count, size_t start_id) {
    lastUpdate = time(NULL);

    hsize_t targetRecords = 0;
    hsize_t localRecords = count;
    hsize_t remoteStart = start_id > 0 ? start_id : 0;
    hsize_t localStart = 0;
    hsize_t nRecords = 0;

    hid_t dataspace = H5Dget_space(dataset);
    hid_t memspace = H5Screate_simple(1, &localRecords, NULL);
    herr_t status = 0;

    //int rank = H5Sget_simple_extent_ndims(dataspace);
    status = H5Sget_simple_extent_dims(dataspace, &nRecords, NULL);
    if (remoteStart < nRecords) {
        targetRecords = count < (nRecords - remoteStart) ? count : (nRecords - remoteStart);

        status = H5Sselect_hyperslab(dataspace, H5S_SELECT_SET, &remoteStart, H5P_DEFAULT, &targetRecords, H5P_DEFAULT);
        status = H5Sselect_hyperslab(memspace, H5S_SELECT_SET, &localStart, H5P_DEFAULT, &localRecords, H5P_DEFAULT);
        status = H5Dread(dataset, type->getType(), memspace, dataspace, H5P_DEFAULT, start_pointer);
    }

    H5Sclose(dataspace);
    H5Sclose(memspace);

    return (size_t) count;
}

template <class pmType>
size_t Hdf5Dataset<pmType>::write(pmType *start, pmType *end, size_t start_id, bool append) {
    if (!ioMethod->getContextOverride()) {
        const Context &context = ioMethod->getContext();
        for (pmType *ptr = start; ptr != end; ++ptr) {
            snprintf(ptr->identifier, IDENTIFIER_SIZE, "%s", context.identifier.c_str());
            snprintf(ptr->subidentifier, IDENTIFIER_SIZE, "%s", context.subidentifier.c_str());
        }
    }

    size_t count = end - start;
    hsize_t rank = 1;
    hsize_t maxRecords = 0;
	hsize_t startRecord = 0;
    hsize_t targetRecords = 0;
    hsize_t newRecords = end - start;
	unsigned int old_nRecords = 0;

    if (append && start_id != 0) {
        IoException e("write start_id must be 0 when appending to a dataset");
        throw &e;
    }
	startRecord = start_id > 0 ? start_id : 0;

    size_t *nRecords = &size;

    hid_t filespace;
    herr_t status;

	lastUpdate = time(NULL);

    hid_t dataspace = H5Dget_space(dataset);
    status = H5Sget_simple_extent_dims(dataspace, &maxRecords, NULL);
    H5Sclose(dataspace);

	if (append) startRecord = *nRecords;
    if (startRecord + count > maxRecords) {
        targetRecords = startRecord + count;
    } else {
        targetRecords = maxRecords;
    }

    status = H5Dset_extent(dataset, &targetRecords);
    filespace = H5Dget_space(dataset);

    status = H5Sselect_hyperslab(filespace, H5S_SELECT_SET, &startRecord, NULL, &newRecords, NULL);
    dataspace = H5Screate_simple(rank, &newRecords, NULL);

    H5Dwrite(dataset, type->getType(), dataspace, filespace, H5P_DEFAULT, start);

	old_nRecords = *nRecords;
	*nRecords = startRecord + count > *nRecords ? startRecord + count : *nRecords;

	H5Awrite(size_id, H5T_NATIVE_UINT, nRecords);

    H5Sclose(filespace);
    H5Sclose(dataspace);
    return count;
}

template <class pmType>
size_t Hdf5Io::write(const string &dsName, pmType *start, pmType *end, size_t start_id, bool append) {
    auto it = currentDatasets.find(dsName);
    if (it == currentDatasets.end()) {
        return 0;
    }
    shared_ptr<Dataset> baseDs = it->second;
    shared_ptr<Hdf5Dataset<pmType> > dataset = dynamic_pointer_cast<Hdf5Dataset<pmType> >(baseDs);
    return dataset->write(start, end, start_id, append);
}

template <class pmType>
size_t Hdf5Io::read(const string &dsName, pmType *start, size_t count, size_t start_id) {
    auto it = currentDatasets.find(dsName);
    if (it == currentDatasets.end()) {
        return 0;
    }
    shared_ptr<Dataset> baseDs = it->second;
    shared_ptr<Hdf5Dataset<pmType> > dataset = dynamic_pointer_cast<Hdf5Dataset<pmType> >(baseDs);
    return dataset->read(start, count, start_id);
}

template <class pmType>
size_t Hdf5Io::howmany(const string &dsName) {
    auto it = currentDatasets.find(dsName);
    if (it == currentDatasets.end()) {
        IoException e(string("No dataset named ") + dsName);
        throw &e;
    }
    shared_ptr<Dataset> baseDs = it->second;
    shared_ptr<Hdf5Dataset<pmType> > dataset = dynamic_pointer_cast<Hdf5Dataset<pmType> >(baseDs);
    return dataset->howmany();
}
 
}

#endif /* PROCFMT_H_ */

