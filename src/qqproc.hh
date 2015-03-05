#ifndef __QQPROC_HH_
#define __QQPROC_HH_

#include <string>
#include <vector>
#include <map>
#include <algorithm>
#include <iterator>
#include <stdlib.h>
#include <iostream>

#include <boost/program_options.hpp>
#include <boost/filesystem.hpp>
#include <boost/regex.hpp>

#include <nclq.hh>
#include "ProcessSummary.hh"

namespace po = boost::program_options;
namespace fs = boost::filesystem;
using namespace std;

class QQProcConfiguration;

class QQProcDataSource : public nclq::DataSource {
    protected:
    QQProcConfiguration *config;

    public:
    QQProcDataSource(QQProcConfiguration *);
    ~QQProcDataSource();

    virtual const vector<nclq::VarDescriptor> &getSymbols() const = 0;
    virtual bool getNext() = 0;
    virtual bool setValue(int idx, nclq::Data *data, nclq::SymbolType outputType) = 0;
    virtual int size() = 0;
    virtual void outputValue(ostream& out, int idx) = 0;
    virtual void prepareQueries(vector<nclq::Expression*>& declarations,
            vector<nclq::Expression*> &queries,
            vector<nclq::Expression*> &outputColumns) = 0;
};

class DataConfiguration {
    protected:
    string name;
    nclq::SymbolTable *symbolTable;

    public:
    DataConfiguration() {}
    
    virtual QQProcDataSource *createDataSource() = 0;
    virtual po::options_description *getOptions() = 0;
    virtual po::options_description *getPrivateOptions() = 0;
    virtual void setupOptions(const po::variables_map& vm) = 0;
    virtual nclq::SymbolTable *getSymbolTable() = 0;
    virtual vector<string> *getOutputColumnsDefault() = 0;
    virtual vector<string> *getSymbolMapList() = 0;
};

class QQProcConfiguration {
    protected:
    DataConfiguration *dataConfig;
    string header;
    string datafileType;
    string dataset;
    string datasetGroup;
	vector<string> declarations;
	vector<string> queries;
    vector<string> outputQueries;
	vector<string> outputFilenames;
	vector<string> outputColumns;
	int daysBack;
	bool verbose;
	time_t startDate;
	time_t endDate;
	string str_startDate;
	string str_endDate;
	string configFile;
    string queryConfigFile;
	char delimiter;
    string mode;

    public:
	QQProcConfiguration(int argc, char** argv);
    ~QQProcConfiguration();
    inline const bool isVerbose() const { return verbose; }
    inline const time_t getStartDate() const { return startDate; }
    inline const time_t getEndDate() const { return endDate; }
    inline const vector<string>& getDeclarations() const { return declarations; }
    inline const vector<string>& getQueries() const { return queries; }
    inline const vector<string>& getOutputQueries() const { return outputQueries; }
    inline const vector<string>& getOuputFilenames() const { return outputFilenames; }
    inline nclq::SymbolTable* getSymbolTable() const { return dataConfig != NULL ? dataConfig->getSymbolTable() : NULL; }
    inline const string& getHeader() const { return header; }
    inline const vector<string>& getOutputFilenames() const { return outputFilenames; }
    inline QQProcDataSource *createDataSource() { return dataConfig != NULL ? dataConfig->createDataSource() : NULL; }
    inline const char getDelimiter() const { return delimiter; }
    inline const string &getDatasetGroup() const { return datasetGroup; }
    inline const string &getDataset() const { return dataset; }
    inline const string &getMode() const { return mode; }
};

template <class pmType>
class SummaryDataSource;

class SummaryConfiguration : public DataConfiguration {
    QQProcConfiguration *config;
    string filePath;
    string h5pattern;
    string dateformat;
    vector<fs::path> files;
    vector<string> specificFiles;
    vector<string> outputColumnsDefault;
    vector<string> symbolMapList;

    string dataset;
    string datasetGroup;

    public:
    SummaryConfiguration(QQProcConfiguration *);
    ~SummaryConfiguration();

    virtual QQProcDataSource *createDataSource();
    virtual po::options_description *getOptions();
    virtual po::options_description *getPrivateOptions();
    virtual void setupOptions(const po::variables_map& vm);
    virtual nclq::SymbolTable *getSymbolTable();

    inline const string &getDataset() const { return dataset; }
    inline const string &getDatasetGroup() const { return datasetGroup; }
    inline const vector<fs::path> &getFiles() const { return files; }
    virtual inline vector<string> *getOutputColumnsDefault() { return &outputColumnsDefault; }
    virtual inline vector<string> *getSymbolMapList() { return &symbolMapList; }
};

template <class pmType>
class SummaryDataSource : public QQProcDataSource {
    friend class SummaryConfiguration;

    SummaryConfiguration *config;
    const static vector<nclq::VarDescriptor> symbols;
    vector<fs::path>::const_iterator fileIter;
    shared_ptr<pmio2::Hdf5Io> input;
    pmType *curr;
    pmType *buffer;

    size_t nObjects;
    size_t filePos;
    size_t buffPos;
    size_t buffCount;
    size_t nRead;
    bool fileEOF;

    void openFile();
    void fillBuffer();

    public:
    SummaryDataSource(QQProcConfiguration *_qConfig, SummaryConfiguration *_uConfig);
    ~SummaryDataSource();

    virtual const vector<nclq::VarDescriptor> &getSymbols() const;
    virtual bool getNext();
    virtual bool setValue(int idx, nclq::Data *data, nclq::SymbolType outputType);
    virtual int size();
    virtual void outputValue(ostream& out, int idx);
    virtual void prepareQueries(vector<nclq::Expression*>& declarations,
            vector<nclq::Expression*> &queries, 
            vector<nclq::Expression*> &outputColumns);
};

template <class pmType>
SummaryDataSource<pmType>::SummaryDataSource(QQProcConfiguration *_qConfig,
        SummaryConfiguration *_uConfig):
        QQProcDataSource(_qConfig), config(_uConfig)
{

    if (config == NULL) throw invalid_argument("Invalid configuration!");
    fileIter = config->getFiles().begin();
    if (fileIter == config->getFiles().end()) {
        throw invalid_argument("Zero UGE account files discovered!");
    }
    curr = NULL;
    nRead = 100000;
    buffPos = 0;
    buffCount = 0;
    filePos = 0;
    nObjects = 0;
    fileEOF = false;

    buffer = new pmType[nRead];
    openFile();
}

template <class pmType>
SummaryDataSource<pmType>::~SummaryDataSource() {
    delete[] buffer;
}

template <class pmType>
void SummaryDataSource<pmType>::openFile() {
    if (fileIter == config->getFiles().end()) {
        throw invalid_argument("No more accounting files.");
    }
    if (input != NULL) {
        input.reset();
    }
    input = make_shared<pmio2::Hdf5Io>(fileIter->c_str(), pmio2::IoMode::MODE_READ);
    input->addDataset("ProcessSummary",
        make_shared<pmio2::Hdf5DatasetFactory<ProcessSummary> >(
            input,
            make_shared<pmio2::Hdf5Type<ProcessSummary> >(input),
            0, 4096, 5, "ProcessSummary"
        )
    );
    input->addDataset("IdentifiedFilesystem",
        make_shared<pmio2::Hdf5DatasetFactory<IdentifiedFilesystem> >(
            input,
            make_shared<pmio2::Hdf5Type<IdentifiedFilesystem> >(input),
            0, 4096, 5, "IdentifiedFilesystem"
        )
    );
    input->addDataset("IdentifiedNetworkConnection",
        make_shared<pmio2::Hdf5DatasetFactory<IdentifiedNetworkConnection> >(
            input,
            make_shared<pmio2::Hdf5Type<IdentifiedNetworkConnection> >(input),
            0, 4096, 5, "IdentifiedNetworkConnection"
        )
    );
    pmio2::Context context("genepool", config->getDatasetGroup(), "*", "*");
    input->setContext(context);
    nObjects = input->howmany<pmType>(config->getDataset());
    fillBuffer();
    fileIter++;
}

template <class pmType>
void SummaryDataSource<pmType>::fillBuffer() {
    if (filePos >= nObjects) {
        fileEOF = true;
        return;
    }
    size_t l_nread = filePos + nRead > nObjects ? nObjects - filePos : nRead;
    size_t read = input->read(config->getDataset(), buffer, l_nread, filePos);
    filePos += l_nread;
    buffPos = 0;
    buffCount = l_nread;
}


template <class pmType>
const vector<nclq::VarDescriptor>& SummaryDataSource<pmType>::getSymbols() const {
    return symbols;
}

template <class pmType>
bool SummaryDataSource<pmType>::getNext() {
    if (buffPos >= buffCount) {
        fillBuffer();
    }
    if (buffPos >= buffCount || fileEOF) {
        if (fileIter == config->getFiles().end()) {
            return false;
        }
        openFile();
    }
    if (buffPos >= buffCount) return false;
    curr = &(buffer[buffPos++]);
    return true;
}

template <class pmType>
int SummaryDataSource<pmType>::size() {
    return symbols.size();
}

template <class pmType>
void SummaryDataSource<pmType>::outputValue(ostream &out, int idx) {
}

template <class pmType>
void SummaryDataSource<pmType>::prepareQueries(vector<nclq::Expression*>& declarations,
            vector<nclq::Expression*> &queries, 
            vector<nclq::Expression*> &outputColumns)
{
    /* do nothing */
}


#endif
