#include <string>
#include <vector>
#include <map>
#include <algorithm>
#include <iterator>
#include <fstream>
#include <sstream>
#include <stdlib.h>
#include <stdexcept>

#include <boost/program_options.hpp>
#include <boost/filesystem.hpp>
#include <boost/regex.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/split.hpp>

#include "ProcIO2.hh"
#include "qqproc.hh"
#include "ProcessSummary.hh"
#include "config.h"

namespace po = boost::program_options;
namespace fs = boost::filesystem;

using namespace std;
using namespace nclq;

void checkQueryExpression(const string&, const nclq::Expression*);
void checkDeclarationExpression(const string&, const nclq::Expression*);

int main(int argc, char** argv) {

	/* read command line arguments & determine which accounting
	files are to be parsed.  Also, initially prepare symbol table */
	QQProcConfiguration *config = NULL;
    QQProcDataSource *dataSource = NULL;
    
    try {
        config = new QQProcConfiguration(argc, argv);
    } catch (invalid_argument &e) {
        cerr << "Unable to determine configuration: " << e.what() << endl;
        exit(1);
    }
    try {
        dataSource = config->createDataSource();
    } catch (invalid_argument &e) {
        cerr << "Error discovering data: " << e.what() << endl;
        exit(1);
    }

	std::vector<nclq::Expression*> declarations;
	std::vector<nclq::Expression*> queries;
	std::vector<nclq::Expression*> outputColumns;
	std::vector<std::ostream*> outputs;

	if (config->isVerbose()) {
		std::cout << "Output Columns" << std::endl;
        for (auto it: config->getOutputQueries()) {
            cout << it << endl;
        }
	}


	if (config->getQueries().size() == 0) {
		std::cout << "At least one query must be specified with \"-q '<query>'\" or in a config file." << std::endl;
		return 1;
	}

    nclq::nclqParser *parser = new nclq::nclqParser(config->getSymbolTable());
    if (config->getDeclarations().size() > 0) {
        try {
            parser->parseExpression(config->getDeclarations(), declarations, &checkDeclarationExpression);
        } catch (invalid_argument &e) {
            cerr << "Failed: invalid declaration(s): " << e.what() << endl;
            exit(1);
        }
    }
    config->getSymbolTable()->setModifiable(false);

    try {
        parser->parseExpression(config->getQueries(), queries, &checkQueryExpression);
        parser->parseExpression(config->getOutputQueries(), outputColumns, NULL);
    } catch (invalid_argument &e) {
        cerr << "Failed: invalid query(ies): " << e.what() << endl;
        exit(1);
    }

    if (queries.size() > 1 && queries.size() != config->getOutputFilenames().size()) {
        cerr << "The number of queries must match the number of output files" << endl;
        exit(1);
    }

    bool usingFileOutput = false;
    for (auto it: config->getOutputFilenames()) {
        ofstream *out = new ofstream(it.c_str());
        outputs.push_back(out);
        usingFileOutput = true;
    }
    if (outputs.size() == 0) outputs.push_back(&cout);
    for (auto out: outputs) {
        if (config->getHeader().length() > 0) {
            *out << config->getHeader() << endl;
        }
    }

    // prepare queries for execution on the data
    dataSource->prepareQueries(declarations, queries, outputColumns);

    // parse accounting data
    int counter = 0;
    int foundCount = 0;
    nclq::SymbolTable *symbols = config->getSymbolTable();
    while (dataSource->getNext()) {
        nclq::Data *data = new nclq::Data[symbols->size()];

        // run any declarations, values stored in "data" 
        for (auto it: declarations) {
            nclq::Expression *result = it->evaluateExpression(dataSource, &data);
            delete result;
        }

        // run queries and potentially output rows 
        int qIdx = 0;
        for (auto it: queries) {
            nclq::Expression *result = it->evaluateExpression(dataSource, &data);
            assert(result != NULL);

            nclq::BoolExpression *boolExpr = static_cast<nclq::BoolExpression*>(result);
            if (boolExpr->getValue()) {
                ostream *out = outputs[qIdx];
                foundCount++;
                int oIdx = 0;
                for (auto outIt: outputColumns) {
                    nclq::Expression *outRes = outIt->evaluateExpression(dataSource, &data);
                    assert(outRes != NULL);
                    if (oIdx++ > 0) *out << config->getDelimiter();
                    outRes->output(*out, dataSource, &data);
                    delete outRes;
                }
                *out << endl;
            }
            delete result;
            qIdx++;
        }
        delete[] data;
        counter++;
    }

    if (config->isVerbose()) {
        cout << counter << " records examined; found=" << foundCount << endl;
    }

    if (usingFileOutput) {
        for (auto out: outputs) {
            delete out;
        }
    }

    delete config;
    delete parser;
    return 0;
}

void checkQueryExpression(const std::string& exprString, const nclq::Expression* expr) {
    if (expr == NULL) {
        throw invalid_argument("Expression cannot be NULL!");
    }
	if (expr == NULL || expr->getBaseType() != nclq::T_BOOL) {
        throw invalid_argument("Query expression must evaluate to a boolean!");
	}
	if (expr->hasType(nclq::EXPR_ASSIGN)) {
        throw invalid_argument("Assignments not allowed in query expressions!");
	}
}
void checkDeclarationExpression(const std::string& exprString, const nclq::Expression* expr) {
	if (expr == NULL || !expr->hasType(nclq::EXPR_ASSIGN)) {
        throw invalid_argument("Declaration expressions must be assignments");
	}
}

QQProcDataSource::QQProcDataSource(QQProcConfiguration *_config): config(_config)
{
}

QQProcDataSource::~QQProcDataSource() {
}

QQProcConfiguration::QQProcConfiguration(int argc, char** argv) {
	/* setup default values */
	verbose = false;
	configFile = "";
	daysBack = 0;
    dataConfig = NULL;
    datafileType = "unset";
    dataset = "unset";
    datasetGroup = "unset";

    string baseConfigFile = string(SYSTEM_CONFIG_DIR) + "/qqproc.conf";
    char *configEnv = NULL;
    if ((configEnv = getenv("QQPROC_CONFIG")) != NULL) {
        baseConfigFile = configEnv;
    }
    po::options_description dataOpts("Data Options");
    dataOpts.add_options()
        //("mode", po::value<string>(&mode)->default_value("procsummary"), "mode of operation") XXX TODO XXX
        ("data.fileType", po::value<string>(&datafileType)->default_value(""), "type of procmon file (summary, raw)")
        ("data.dataset", po::value<string>(&dataset)->default_value(""), "dataset")
        ("data.datasetGroup", po::value<string>(&datasetGroup)->default_value(""), "group containing dataset (like /processes)")
    ;
    po::options_description procsummaryOpts("procsummary Options");
    procsummaryOpts.add_options()
        ("procsummary.dataset", po::value<string>()->default_value("ProcessSummary"), "name of ProcessSummary dataset in summary h5 files")
        ("procsummary.datasetGroup", po::value<string>()->default_value("/processes"), "group path in which ProcessSummary dataset can be found in summary h5 files")
        ("procsummary.varmap", po::value<vector<string> >()->composing(), "variable maps for ProcessSummary")
        ("procsummary.column_default", po::value<vector<string> >()->composing(), "default output columns for ProcessSummary")
    ;

    po::options_description fssummaryOpts("fssummary Options");
    fssummaryOpts.add_options()
        ("fssummary.dataset", po::value<string>()->default_value("IdentifiedFilesystem"), "name of IdentifiedFilesystem dataset in summary h5 files")
        ("fssummary.datasetGroup", po::value<string>()->default_value("/processes"), "group path in which IdentifiedFilesystem dataset can be found in summary h5 files")
        ("fssummary.varmap", po::value<vector<string> >()->composing(), "variable maps for IdentifiedFilesystem")
        ("fssummary.column_default", po::value<vector<string> >()->composing(), "default output columns for IdentifiedFilesystem")
    ;

    po::options_description netsummaryOpts("netsummary Options");
    netsummaryOpts.add_options()
        ("netsummary.dataset", po::value<string>()->default_value("IdentifiedNetworkConnection"), "name of IdentifiedNetworkConnection dataset in summary h5 files")
        ("netsummary.datasetGroup", po::value<string>()->default_value("/processes"), "group path in which IdentifiedNetworkConnection dataset can be found in summary h5 files")
        ("netsummary.varmap", po::value<vector<string> >()->composing(), "variable maps for IdentifiedNetworkConnection")
        ("netsummary.column_default", po::value<vector<string> >()->composing(), "default output columns for IdentifiedNetworkConnection")
    ;

    // if possible, read the baseConfigFile to bootstrap the application
    if (fs::exists(baseConfigFile)) {
        ifstream input(baseConfigFile.c_str());
        po::variables_map vm;

		if (!input) {
			throw invalid_argument("Bootstrap config file doesn't exist: " + baseConfigFile);
		}
		po::store(po::parse_config_file(input, dataOpts, true), vm);
		po::notify(vm);

        input.close();
        if (vm.count("data.fileType") == 0) {
            throw invalid_argument("Couldn't determine bootstrap datafileType to use; none specified in config file");
        }
    }
    if (datafileType == "summary") {
        dataConfig = new SummaryConfiguration(this);
    //} else if (batchSystem == "SLURM") {
        // XXX need to write slurm add-on
    } else {
        invalid_argument e("Unknown datafile type: " + datafileType);
        throw &e;
    }

    if (dataConfig == NULL) {
        invalid_argument e("configuration for " + datafileType + " invalid");
        throw &e;
    }

	/* parse command line variables */
	po::options_description basic("Basic Options");
	basic.add_options()
		("version", "Print version information")
		("help,h", "Print help message")
		("verbose,v", "Print extra (debugging) information")
		("file,f", po::value<std::string>(&(this->configFile))->default_value(""), "Optional configuration file")
		;

	po::options_description config("Configuration Options");
	config.add_options()
		("declare,d",po::value<std::vector<std::string> >(&(this->declarations))->composing(), "Specify any declarations - expressions to evaluate prior to the query/queries")
		("query,q",po::value<std::vector<std::string> >(&(this->queries))->composing(), "Specify query or queries")
		("output,o",po::value<std::vector<std::string> >(&(this->outputFilenames))->composing(), "Specify output file(s); Optional, but if multiple queries specified then an equal number of output files must be specified.")
		("header,H",po::value<std::string>(&(this->header))->default_value(""), "Optional header string for output character delimited files")
		("delimiter,t", po::value<char>(&(this->delimiter))->default_value(','), "Set the character delimiter to be used in output, by default a comma is used")
		("column,c",po::value<std::vector<std::string> >(&(this->outputColumns))->composing(), "Specify output columns")
		("days,D", po::value<int>(&(this->daysBack)), "Specify number of previous days back to look at accounting logs - uses last modification date of accounting files, default: 1")
		("start,S",po::value<std::string>(&(this->str_startDate))->default_value("undefined"), "Specify earliest modification date of accounting files to consider, YYYY-MM-DD")
		("end,E",po::value<std::string>(&(this->str_endDate))->default_value("undefined"), "Specify lastest modification date of accounting files to consider, YYYY-MM-DD")
		;

    po::options_description hidden_options("Hidden Options");
    hidden_options.add_options()
        ("varmap", po::value<vector<string> >(&symbolMapList)->composing(), "Mapped/Aliased names for symbols")
        ("column_default", po::value<vector<string> >(&outputColumns_default)->composing(), "Default column values")
    ;

    po::options_description *dataPublicOptions = dataConfig->getOptions();
    po::options_description *dataPrivateOptions = dataConfig->getPrivateOptions();

    po::positional_options_description pos_opts;
    pos_opts.add("query", 1);

	po::options_description options;
	options.add(basic).add(config);
    if (dataPublicOptions != NULL) {
        options.add(*dataPublicOptions);
    }
    if (dataPrivateOptions != NULL) {
        options.add(*dataPrivateOptions);
    }
    options.add(dataOpts).add(hidden_options);

    po::options_description helpOptions;
    helpOptions.add(basic).add(config).add(*dataPublicOptions);


	po::variables_map vm;
	po::store(po::command_line_parser(argc, argv).options(options).positional(pos_opts).run(), vm);

	if (vm.count("verbose")) {
		verbose = true;
	}
	if (vm.count("file") && configFile.length() > 0) {
		std::ifstream ifs(configFile.c_str());
		if (!ifs) {
            invalid_argument e(string("Config file doesn't exist: ") + configFile);
            throw &e;
		}
		po::store(po::parse_config_file(ifs, config), vm);
        ifs.close();
	}

    // re-read global configuration file with full options
    if (fs::exists(baseConfigFile)) {
        ifstream input(baseConfigFile.c_str());

		if (!input) {
			throw invalid_argument("Bootstrap config file doesn't exist: " + baseConfigFile);
		}
		po::store(po::parse_config_file(input, options), vm);
        input.close();
    }
    po::notify(vm);

	// setup initial symbol table
    SymbolTable *table = dataConfig->getSymbolTable();
    for (auto mapItem: symbolMapList) {
        size_t found = mapItem.find(':');
        if (found == string::npos) continue;

        string key = mapItem.substr(0, found);
        string value = mapItem.substr(found+1);
        ssize_t idx = table->find(value);
        if (idx == -1) {
            invalid_argument e(string("Unknown symbol in varmap: ") + mapItem);
            throw &e;
        }
        table->mapSymbol(key, idx);
    }

    // setup output columns
    vector<string> *out = &outputColumns_default;
    if (outputColumns.size() > 0) out = &outputColumns;
    vector<string> outputNames;
    for (auto colMap: *out) {
        size_t loc = colMap.find(':');
        if (loc != string::npos) {
            outputNames.push_back(colMap.substr(0, loc));
            outputQueries.push_back(colMap.substr(loc+1));
        } else {
            outputQueries.push_back(colMap);
        }
    }

    // if a custom header wasn't specified AND the output columns were
    // labeled, then generate the header from the labels
    if (header.size() == 0 && outputNames.size() > 0) {
        ostringstream oss;
        int idx = 0;
        for (auto name: outputNames) {
            if (idx++ > 0) oss << delimiter;
            oss << name;
        }
        header = oss.str();
    }

	/* now that symbol table is setup, display help & exit if requested */
	if (vm.count("help")) {
		std::cout << helpOptions << std::endl;
		std::cout << std::endl << "Valid built-in identifiers for query/output expressions:" << std::endl;
		int lineLen = 0;
        vector<string> symbolKeys;
        for (auto it: table->getVariableMap()) {
            symbolKeys.push_back(it.first);
        }
        sort(symbolKeys.begin(), symbolKeys.end());
        int idx = 0;
        for (auto it: symbolKeys) {
            if (idx++ != 0) {
                cout << ", ";
                lineLen += 2;
            }
            if (lineLen > 80) {
                cout << endl;
                lineLen = 0;
            }
            cout << it;
            lineLen += it.length();
        }
		cout << endl << endl;
		exit(1);
	}

	if (str_startDate != "undefined") {
		if (vm.count("days") >  0) {
			std::cout << "Cannot specify both -d (days) and -s (start date)." << std::endl;
			exit(1);
		}
		daysBack = 0;
	} else if (vm.count("days") == 0) {
		daysBack = 1;
	}

	// deal with startDate
	struct tm tmpTime;
	memset(&tmpTime, 0, sizeof(struct tm));
	tmpTime.tm_isdst = -1; //auto-detect DST
	if (str_startDate != "undefined") {
		if (strptime(str_startDate.c_str(), "%Y-%m-%d", &tmpTime) == NULL) {
			std::cout << "Could not parse start date, use YYYY-MM-DD format: " << str_startDate << std::endl;
			exit(1);
		} else {
			startDate = mktime(&tmpTime);
		}
	} else if (daysBack > 0) {
		startDate = time(NULL);
		if (localtime_r(&startDate, &tmpTime)) {
			tmpTime.tm_hour = 0;
			tmpTime.tm_min = 0;
			tmpTime.tm_sec = 0;
			tmpTime.tm_mday -= daysBack; // mktime will sort out if this crosses month/year boundaries
			startDate = mktime(&tmpTime);
		} else {
			std::cout << "Failed to obtain current date! Exiting" << std::endl;
			exit(1);
		}
	} else {
		startDate = 0;
	}
	if (str_endDate != "undefined") {
		if (strptime(str_endDate.c_str(), "%Y-%m-%d", &tmpTime) == NULL) {
			std::cout << "Could not parse end date, use YYYY-MM-DD format: " << str_endDate << std::endl;
			exit(1);
		}
		tmpTime.tm_hour = 23;
		tmpTime.tm_min = 59;
		tmpTime.tm_sec = 59;
		endDate = mktime(&tmpTime);
	} else {
		endDate = time(NULL) + 100;
	}

	if (verbose) {
		std::cout << "start date: " << startDate << std::endl;
		std::cout << "end date: " << endDate << std::endl;
	}
    // setup dataConfg
    dataConfig->setupOptions(vm);

    if (dataPublicOptions != NULL) {
        delete dataPublicOptions;
    }
    if (dataPrivateOptions != NULL) {
        delete dataPrivateOptions;
    }
}

QQProcConfiguration::~QQProcConfiguration() {
    if (dataConfig != NULL) {
        delete dataConfig;
        dataConfig = NULL;
    }
}

template <>
const vector<nclq::VarDescriptor> SummaryDataSource<ProcessSummary>::symbols = {
    { "identifier",         nclq::T_STRING,     nclq::T_STRING }, // 0
    { "subidentifier",      nclq::T_STRING,     nclq::T_STRING }, // 1
    { "recTime",            nclq::T_INTEGER,    nclq::T_INTEGER }, // 2
    { "recTimeUSec",        nclq::T_INTEGER,    nclq::T_INTEGER }, // 3
    { "startTime",          nclq::T_INTEGER,    nclq::T_INTEGER }, // 4
    { "startTimeUSec",      nclq::T_INTEGER,    nclq::T_INTEGER }, // 5
    { "pid",                nclq::T_INTEGER,    nclq::T_INTEGER }, // 7
    { "execName",           nclq::T_STRING,     nclq::T_STRING },   // 8
    { "cmdArgBytes",        nclq::T_INTEGER,    nclq::T_INTEGER },  // 9
    { "cmdArgs",            nclq::T_STRING,     nclq::T_STRING },   //10
    { "exePath",            nclq::T_STRING,     nclq::T_STRING },   //11
    { "cwdPath",            nclq::T_STRING,     nclq::T_STRING },   //12
    { "ppid",               nclq::T_INTEGER,    nclq::T_INTEGER },  //13
    { "state",              nclq::T_STRING,     nclq::T_STRING },   //14
    { "pgrp",               nclq::T_INTEGER,    nclq::T_INTEGER },  //15
    { "session",            nclq::T_INTEGER,    nclq::T_INTEGER },  //16
    { "tty",                nclq::T_INTEGER,    nclq::T_INTEGER },  //17
    { "tpgid",              nclq::T_INTEGER,    nclq::T_INTEGER },  //18
    { "realUid",            nclq::T_INTEGER,    nclq::T_INTEGER },  //19
    { "effUid",             nclq::T_INTEGER,    nclq::T_INTEGER },  //20
    { "realGid",            nclq::T_INTEGER,    nclq::T_INTEGER },
    { "effGid",             nclq::T_INTEGER,    nclq::T_INTEGER },
    { "flags",              nclq::T_INTEGER,    nclq::T_INTEGER },
    { "utime",              nclq::T_INTEGER,    nclq::T_INTEGER },
    { "stime",              nclq::T_INTEGER,    nclq::T_INTEGER },
    { "priority",           nclq::T_INTEGER,    nclq::T_INTEGER },
    { "nice",               nclq::T_INTEGER,    nclq::T_INTEGER },
    { "numThreads",         nclq::T_INTEGER,    nclq::T_INTEGER },
    { "vsize",             nclq::T_INTEGER,    nclq::T_INTEGER },
    { "rss",                nclq::T_INTEGER,    nclq::T_INTEGER },
    { "rsslim",             nclq::T_INTEGER,    nclq::T_INTEGER },
    { "signal",             nclq::T_INTEGER,    nclq::T_INTEGER },
    { "blocked",            nclq::T_INTEGER,    nclq::T_INTEGER },
    { "sigignore",          nclq::T_INTEGER,    nclq::T_INTEGER },
    { "sigcatch",           nclq::T_INTEGER,    nclq::T_INTEGER },
    { "rtPriority",         nclq::T_INTEGER,    nclq::T_INTEGER },
    { "policy",             nclq::T_INTEGER,    nclq::T_INTEGER },
    { "delayacctBlkIOTicks",nclq::T_INTEGER,    nclq::T_INTEGER },
    { "guestTime",          nclq::T_INTEGER,    nclq::T_INTEGER },
    { "vmpeak",             nclq::T_INTEGER,    nclq::T_INTEGER },
    { "rsspeak",            nclq::T_INTEGER,    nclq::T_INTEGER },
    { "cpusAllowed",        nclq::T_INTEGER,    nclq::T_INTEGER },
    { "io_rchar",           nclq::T_INTEGER,    nclq::T_INTEGER },
    { "io_wchar",           nclq::T_INTEGER,    nclq::T_INTEGER },
    { "io_syscr",           nclq::T_INTEGER,    nclq::T_INTEGER },
    { "io_syscw",           nclq::T_INTEGER,    nclq::T_INTEGER },
    { "io_readBytes",       nclq::T_INTEGER,    nclq::T_INTEGER },
    { "io_writeBytes",      nclq::T_INTEGER,    nclq::T_INTEGER },
    { "io_cancelledWriteBytes",nclq::T_INTEGER, nclq::T_INTEGER },
    { "m_size",             nclq::T_INTEGER,    nclq::T_INTEGER },
    { "m_resident",         nclq::T_INTEGER,    nclq::T_INTEGER },
    { "m_share",            nclq::T_INTEGER,    nclq::T_INTEGER },
    { "m_text",             nclq::T_INTEGER,    nclq::T_INTEGER },
    { "m_data",             nclq::T_INTEGER,    nclq::T_INTEGER },
    { "isParent",           nclq::T_INTEGER,    nclq::T_INTEGER },
    { "host",               nclq::T_STRING,     nclq::T_STRING },
    { "command",            nclq::T_STRING,     nclq::T_STRING },
    { "execCommand",        nclq::T_STRING,     nclq::T_STRING },
    { "script",             nclq::T_STRING,     nclq::T_STRING },
    { "user",               nclq::T_STRING,     nclq::T_STRING },
    { "project",            nclq::T_STRING,     nclq::T_STRING },
    { "derived_startTime",  nclq::T_DOUBLE,     nclq::T_DOUBLE },
    { "derived_recTime",    nclq::T_DOUBLE,    nclq::T_DOUBLE },
    { "baseline_startTime", nclq::T_DOUBLE,    nclq::T_DOUBLE },
    { "orig_startTime",     nclq::T_DOUBLE,    nclq::T_DOUBLE },
    { "nObservations",      nclq::T_INTEGER,    nclq::T_INTEGER },
    { "nRecords",           nclq::T_INTEGER,    nclq::T_INTEGER },
    { "volatilityScore",    nclq::T_DOUBLE,    nclq::T_DOUBLE },
    { "cpuTime",            nclq::T_DOUBLE,    nclq::T_DOUBLE },
    { "duration",           nclq::T_DOUBLE,    nclq::T_DOUBLE },
    { "cpuTime_net",        nclq::T_DOUBLE,    nclq::T_DOUBLE },
    { "utime_net",          nclq::T_INTEGER,    nclq::T_INTEGER },
    { "stime_net",          nclq::T_INTEGER,    nclq::T_INTEGER },
    { "io_rchar_net",       nclq::T_INTEGER,    nclq::T_INTEGER },
    { "io_wchar_net",       nclq::T_INTEGER,    nclq::T_INTEGER },
    { "cpuRateMax",         nclq::T_DOUBLE,    nclq::T_DOUBLE },
    { "iorRateMax",         nclq::T_DOUBLE,    nclq::T_DOUBLE },
    { "iowRateMax",         nclq::T_DOUBLE,    nclq::T_DOUBLE },
    { "msizeRateMax",       nclq::T_DOUBLE,    nclq::T_DOUBLE },
    { "mresidentRateMax",   nclq::T_DOUBLE,    nclq::T_DOUBLE },
    { "cov_cpuXiow",        nclq::T_DOUBLE,    nclq::T_DOUBLE },
    { "cov_cpuXior",        nclq::T_DOUBLE,    nclq::T_DOUBLE },
    { "cov_cpuXmsize",      nclq::T_DOUBLE,    nclq::T_DOUBLE },
    { "cov_cpuXmresident",  nclq::T_DOUBLE,    nclq::T_DOUBLE },
    { "cov_iowXior",        nclq::T_DOUBLE,    nclq::T_DOUBLE },
    { "cov_iowXmsize",      nclq::T_DOUBLE,    nclq::T_DOUBLE },
    { "cov_iowXmresident",  nclq::T_DOUBLE,    nclq::T_DOUBLE },
    { "cov_iorXmsize",      nclq::T_DOUBLE,    nclq::T_DOUBLE },
    { "cov_iorXmresident",  nclq::T_DOUBLE,    nclq::T_DOUBLE },
    { "cov_msizeXmresident",nclq::T_DOUBLE,    nclq::T_DOUBLE },
};

template<>
const vector<nclq::VarDescriptor> SummaryDataSource<IdentifiedFilesystem>::symbols = {
    { "identifier",         nclq::T_STRING,     nclq::T_STRING }, // 0
    { "subidentifier",      nclq::T_STRING,     nclq::T_STRING }, // 1
    { "startTime",          nclq::T_INTEGER,    nclq::T_INTEGER }, // 4
    { "startTimeUSec",      nclq::T_INTEGER,    nclq::T_INTEGER }, // 5
    { "pid",                nclq::T_INTEGER,    nclq::T_INTEGER }, // 7
    { "host",               nclq::T_STRING,     nclq::T_STRING },
    { "filesystem",         nclq::T_STRING,     nclq::T_STRING },
    { "command",            nclq::T_STRING,     nclq::T_STRING },
    { "user",               nclq::T_STRING,     nclq::T_STRING },
    { "project",            nclq::T_STRING,     nclq::T_STRING },
    { "read",               nclq::T_INTEGER,    nclq::T_INTEGER },
    { "write",              nclq::T_INTEGER,    nclq::T_INTEGER },
};

template<>
const vector<nclq::VarDescriptor> SummaryDataSource<IdentifiedNetworkConnection>::symbols = {
    { "identifier",         nclq::T_STRING,     nclq::T_STRING }, // 0
    { "subidentifier",      nclq::T_STRING,     nclq::T_STRING }, // 1
    { "host",               nclq::T_STRING,     nclq::T_STRING },
    { "startTime",          nclq::T_INTEGER,    nclq::T_INTEGER }, // 4
    { "startTimeUSec",      nclq::T_INTEGER,    nclq::T_INTEGER }, // 5
    { "pid",                nclq::T_INTEGER,    nclq::T_INTEGER }, // 7
    { "command",            nclq::T_STRING,     nclq::T_STRING },
    { "protocol",           nclq::T_STRING,     nclq::T_STRING },
    { "remoteAddress",      nclq::T_STRING,     nclq::T_STRING },
    { "remotePort",         nclq::T_INTEGER,    nclq::T_INTEGER },
    { "localAddress",       nclq::T_STRING,     nclq::T_STRING },
    { "localPort",          nclq::T_INTEGER,    nclq::T_INTEGER },
    { "user",               nclq::T_STRING,     nclq::T_STRING },
    { "project",            nclq::T_STRING,     nclq::T_STRING },
};



template <>
bool SummaryDataSource<IdentifiedFilesystem>::setValue(int idx, nclq::Data *data, nclq::SymbolType outputType) {
    if (curr == NULL) return false;
    switch (idx) {
        case 0: data->setValue(curr->identifier); break;
        case 1: data->setValue(curr->subidentifier); break;
        case 2: data->setValue((intType)curr->startTime); break;
        case 3: data->setValue((intType)curr->startTimeUSec); break;
        case 4: data->setValue((intType)curr->pid); break;
        case 5: data->setValue(curr->host); break;
        case 6: data->setValue(curr->filesystem); break;
        case 7: data->setValue(curr->command); break;
        case 8: data->setValue(curr->user); break;
        case 9: data->setValue(curr->project); break;
        case 10: data->setValue((intType)curr->read); break;
        case 11: data->setValue((intType)curr->write); break;
        default: return false;
    }
    return true;
}

template <>
bool SummaryDataSource<IdentifiedNetworkConnection>::setValue(int idx, nclq::Data *data, nclq::SymbolType outputType) {
    if (curr == NULL) return false;
    switch (idx) {
        case 0: data->setValue(curr->identifier); break;
        case 1: data->setValue(curr->subidentifier); break;
        case 2: data->setValue(curr->host); break;
        case 3: data->setValue((intType)curr->startTime); break;
        case 4: data->setValue((intType)curr->startTimeUSec); break;
        case 5: data->setValue((intType)curr->pid); break;
        case 6: data->setValue(curr->command); break;
        case 7: data->setValue(curr->protocol); break;
        case 8: data->setValue(curr->remoteAddress); break;
        case 9: data->setValue((intType)curr->remotePort); break;
        case 10: data->setValue(curr->localAddress); break;
        case 11: data->setValue((intType)curr->localPort); break;
        case 12: data->setValue(curr->user); break;
        case 13: data->setValue(curr->project); break;
        default: return false;
    }
    return true;
}

template <>
bool SummaryDataSource<ProcessSummary>::setValue(int idx, nclq::Data *data,
        nclq::SymbolType outputType)
{
    if (curr == NULL) return false;
    switch (idx) {
        case 0: data->setValue(curr->identifier); break;
        case 1: data->setValue(curr->subidentifier); break;
        case 2: data->setValue((intType)curr->recTime, nclq::T_INTEGER); break;
        case 3: data->setValue((intType)curr->recTimeUSec, nclq::T_INTEGER); break;
        case 4: data->setValue((intType)curr->startTime, nclq::T_INTEGER); break;
        case 5: data->setValue((intType)curr->startTimeUSec, nclq::T_INTEGER); break;
        case 6: data->setValue((intType)curr->pid); break;
        case 7: data->setValue(curr->execName); break;
        case 8: data->setValue((intType)curr->cmdArgBytes); break;
        case 9: data->setValue(curr->cmdArgs); break;
        case 10: data->setValue(curr->exePath); break;
        case 11: data->setValue(curr->cwdPath); break;
        case 12: data->setValue((intType)curr->ppid); break;
        case 13: {
            char tmp[2] = { curr->state, 0 };
            data->setValue(tmp);
            break;
        }
        case 14: data->setValue((intType)curr->pgrp); break;
        case 15: data->setValue((intType)curr->session); break;
        case 16: data->setValue((intType)curr->tty); break;
        case 17: data->setValue((intType)curr->tpgid); break;
        case 18: data->setValue((intType)curr->realUid); break;
        case 19: data->setValue((intType)curr->effUid); break;
        case 20: data->setValue((intType)curr->realGid); break;
        case 21: data->setValue((intType)curr->effGid); break;
        case 22: data->setValue((intType)curr->flags); break;
        case 23: data->setValue((intType)curr->utime); break;
        case 24: data->setValue((intType)curr->stime); break;
        case 25: data->setValue((intType)curr->priority); break;
        case 26: data->setValue((intType)curr->nice); break;
        case 27: data->setValue((intType)curr->numThreads); break;
        case 28: data->setValue((intType)curr->vsize ); break;
        case 29: data->setValue((intType)curr->rss   ); break;
        case 30: data->setValue((intType)curr->rsslim); break;
        case 31: data->setValue((intType)curr->signal); break;
        case 32: data->setValue((intType)curr->blocked); break;
        case 33: data->setValue((intType)curr->sigignore); break;
        case 34: data->setValue((intType)curr->sigcatch); break;
        case 35: data->setValue((intType)curr->rtPriority); break;
        case 36: data->setValue((intType)curr->policy); break;
        case 37: data->setValue((intType)curr->delayacctBlkIOTicks); break;
        case 38: data->setValue((intType)curr->guestTime); break;
        case 39: data->setValue((intType)curr->vmpeak  ); break;
        case 40: data->setValue((intType)curr->rsspeak ); break;
        case 41: data->setValue((intType)curr->cpusAllowed); break;
        case 42: data->setValue((intType)curr->io_rchar); break;
        case 43: data->setValue((intType)curr->io_wchar); break;
        case 44: data->setValue((intType)curr->io_syscr); break;
        case 45: data->setValue((intType)curr->io_syscw); break;
        case 46: data->setValue((intType)curr->io_readBytes); break;
        case 47: data->setValue((intType)curr->io_writeBytes); break;
        case 48: data->setValue((intType)curr->io_cancelledWriteBytes); break;
        case 49: data->setValue((intType)curr->m_size); break;
        case 50: data->setValue((intType)curr->m_resident); break;
        case 51: data->setValue((intType)curr->m_share); break;
        case 52: data->setValue((intType)curr->m_text); break;
        case 53: data->setValue((intType)curr->m_data); break;
        case 54: data->setValue((intType)curr->isParent); break;
        case 55: data->setValue(curr->host); break;
        case 56: data->setValue(curr->command); break;
        case 57: data->setValue(curr->execCommand); break;
        case 58: data->setValue(curr->script); break;
        case 59: data->setValue(curr->user); break;
        case 60: data->setValue(curr->project); break;
        case 61: data->setValue((intType)curr->derived_startTime); break;
        case 62: data->setValue((intType)curr->derived_recTime); break;
        case 63: data->setValue((intType)curr->baseline_startTime); break;
        case 64: data->setValue((intType)curr->orig_startTime); break;
        case 65: data->setValue((intType)curr->nObservations); break;
        case 66: data->setValue((intType)curr->nRecords); break;
        case 67: data->setValue(curr->volatilityScore); break;
        case 68: data->setValue(curr->cpuTime); break;
        case 69: data->setValue(curr->duration); break;
        case 70: data->setValue(curr->cpuTime_net); break;
        case 71: data->setValue((intType)curr->utime_net); break;
        case 72: data->setValue((intType)curr->stime_net); break;
        case 73: data->setValue((intType)curr->io_rchar_net); break;
        case 74: data->setValue((intType)curr->io_wchar_net); break;
        case 75: data->setValue(curr->cpuRateMax); break;
        case 76: data->setValue(curr->iorRateMax); break;
        case 77: data->setValue(curr->iowRateMax); break;
        case 78: data->setValue(curr->msizeRateMax); break;
        case 79: data->setValue(curr->mresidentRateMax); break;
        case 80: data->setValue(curr->cov_cpuXiow); break;
        case 81: data->setValue(curr->cov_cpuXior); break;
        case 82: data->setValue(curr->cov_cpuXmsize); break;
        case 83: data->setValue(curr->cov_cpuXmresident); break;
        case 84: data->setValue(curr->cov_iowXior); break;
        case 85: data->setValue(curr->cov_iowXmsize); break;
        case 86: data->setValue(curr->cov_iowXmresident); break;
        case 87: data->setValue(curr->cov_iorXmsize); break;
        case 88: data->setValue(curr->cov_iorXmresident); break;
        case 89: data->setValue(curr->cov_msizeXmresident); break;
        default: return false;
    }
    return true;
}

SummaryConfiguration::SummaryConfiguration(QQProcConfiguration *_config):
    config(_config)
{
    outputColumnsDefault = {
        "job_number", "task_number", "hostname", "owner", "project",
        "job_name", "datetime(submission)", "datetime(start)",
        "datetime(end)", "failed", "exit_status", "h_rt", "wallclock",
        "memory(h_vmem)", "memory(maxvmem)",
    };

    dataset = config->getDataset();
    datasetGroup = config->getDatasetGroup();
    if (dataset == "ProcessSummary") {
        symbolTable = new nclq::SymbolTable(SummaryDataSource<ProcessSummary>::symbols);
    } else if (dataset == "IdentifiedFilesystem") {
        symbolTable = new nclq::SymbolTable(SummaryDataSource<IdentifiedFilesystem>::symbols);
    } else if (dataset == "IdentifiedNetworkConnection") {
        symbolTable = new nclq::SymbolTable(SummaryDataSource<IdentifiedNetworkConnection>::symbols);
    } else {
        throw invalid_argument(string("Unknown dataset: ") + dataset);
    }
    symbolTable->setModifiable(true);
}

SummaryConfiguration::~SummaryConfiguration() {
    delete symbolTable;
}

QQProcDataSource *SummaryConfiguration::createDataSource() {
    if (dataset == "ProcessSummary") {
        return new SummaryDataSource<ProcessSummary>(config, this);
    } else if (dataset == "IdentifiedFilesystem") {
        return new SummaryDataSource<IdentifiedFilesystem>(config, this);
    } else if (dataset == "IdentifiedNetworkConnection") {
        return new SummaryDataSource<IdentifiedNetworkConnection>(config, this);
    }
    throw invalid_argument(string("Unknown dataset: ") + dataset);
}

po::options_description *SummaryConfiguration::getOptions() {
    po::options_description *opts = new po::options_description("procmon Summary File Options");

    opts->add_options()
        ("summary.path,p", po::value<string>(&filePath)->default_value("NONE"), "Specify path for procmon summary files")
        ("summary.h5pattern,P", po::value<string>(&h5pattern)->default_value("procmon.(\\d+).summary.h5"), "Specify file naming pattern (using strftime options for time)")
        ("summary.dateformat", po::value<string>(&dateformat)->default_value("%Y%m%d"), "strftime options for time encoded in date match group of h5pattern")
        ("summary.file", po::value<vector<string> >(&specificFiles)->composing(), "Specific h5 file to be examined (disables time-based h5file discovery")
    ;
    return opts;
}

po::options_description *SummaryConfiguration::getPrivateOptions() {
    return nullptr;
}

void SummaryConfiguration::setupOptions(const po::variables_map& vm) {
    vector<string> paths;
    boost::algorithm::split(paths, filePath, boost::is_any_of(":"));
    for (vector<string>::iterator iter = paths.begin(), end=paths.end(); iter!=end; ++iter) {
        const string& currPath = *iter;

        boost::regex h5Regex(h5pattern);
        fs::path h5Path(currPath);
        struct tm datetime;
        if (specificFiles.size() == 0) {
            try {
                if (fs::exists(currPath) && fs::is_directory(currPath)) {
                    vector<fs::path> all_dirent;
                    copy(fs::directory_iterator(currPath), fs::directory_iterator(), back_inserter(all_dirent));
                    for (vector<fs::path>::const_iterator iter(all_dirent.begin()), end(all_dirent.end()); iter != end; ++iter) {
                        if (!is_regular_file(*iter)) continue;

                        boost::smatch matched;
                        if (boost::regex_match(iter->filename().native(), matched, h5Regex)) {
                            bzero(&datetime, sizeof(struct tm));
                            const char *datestr = &*(matched[1].first);
                            if (strptime(datestr, dateformat.c_str(), &datetime) == NULL) {
                                continue;
                            }
                            time_t timestamp = mktime(&datetime);
                            if (timestamp >= config->getStartDate() && timestamp <= config->getEndDate()) {
                                files.push_back(*iter);
                            }
                        }
                    }
                }
            } catch (const fs::filesystem_error &err) {
                cerr << "Encountered filesystem error: " << err.what() << endl;
                exit(1);
            }
        } else {
            vector<string>::iterator iter(specificFiles.begin());
            while (iter < specificFiles.end()) {
                fs::path currFile = currPath / fs::path(*iter);
                if (fs::exists(currFile) && is_regular_file(currFile)) {
                    specificFiles.erase(iter++);
                    files.push_back(currFile);
                } else {
                    ++iter;
                }
            }
        }
    }
    for (vector<string>::const_iterator iter(specificFiles.begin()), end(specificFiles.end()); iter != end; ++iter) {
        fs::path currFile = *iter;
        if (fs::exists(currFile) && is_regular_file(currFile)) {
            files.push_back(currFile);
        } else {
            cout << "Couldn't find file: " << currFile << endl;
            exit(2);
        }
    }

    sort(files.begin(), files.end());
    if (files.size() == 0) {
        cout << "No matching accounting files.  Exiting." << endl;
        exit(1);
    }
    if (config->isVerbose()) {
        for (auto it: files) {
            cout << "Will parse: " << it << endl;
        }
    }
}

nclq::SymbolTable *SummaryConfiguration::getSymbolTable() {
    return symbolTable;
}
