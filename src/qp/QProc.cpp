
#include <string>
#include <vector>
#include <map>
#include <algorithm>
#include <iterator>
#include <fstream>
#include <sstream>
#include <stdlib.h>

#include <boost/program_options.hpp>
#include <boost/filesystem.hpp>
#include <boost/regex.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/split.hpp>

#include "QProc.hh"

namespace po = boost::program_options;

const vector<nclq::VarDescriptor> qpDataSource::symbols = {
    { "identifier",            T_STRING,     T_STRING, },
    { "subidentifier",         T_STRING,     T_STRING, }, //1
    { "hostname",              T_STRING,     T_STRING, },
    { "obsTime",               T_INTEGER,    T_INTEGER,}, //3
    { "startTime",             T_INTEGER,    T_INTEGER,},
    { "pid",                   T_INTEGER,    T_INTEGER,}, //5
    { "execName",              T_STRING,     T_STRING, },
    { "cmdArgs",               T_STRING,     T_STRING, }, //7
    { "cmdArgBytes",           T_MEMORY,     T_INTEGER,},
    { "exePath",               T_STRING,     T_STRING, }, //9
    { "cwdPath",               T_STRING,     T_STRING, },
    { "ppid",                  T_INTEGER,    T_INTEGER,}, //11
    { "pgrp",                  T_INTEGER,    T_INTEGER,},
    { "session",               T_INTEGER,    T_INTEGER,}, //13
    { "tty",                   T_INTEGER,    T_INTEGER,},
    { "tpgid",                 T_INTEGER,    T_INTEGER,}, //15
    { "realUid",               T_USER,       T_INTEGER,},
    { "effUid",                T_USER,       T_INTEGER,}, //17
    { "realGid",               T_GROUP,      T_INTEGER,},
    { "effGid",                T_GROUP,      T_INTEGER,}, //19
    { "flags",                 T_INTEGER,    T_INTEGER,},
    { "utime",                 T_TICKS,      T_INTEGER,}, //21
    { "stime",                 T_TICKS,      T_INTEGER,},
    { "priority",              T_INTEGER,    T_INTEGER,}, //23
    { "nice",                  T_INTEGER,    T_INTEGER,},
    { "numThreads",            T_INTEGER,    T_INTEGER,}, //25
    { "vsize",                 T_INTEGER,    T_INTEGER,},
    { "rss",                   T_MEMORY,     T_INTEGER,}, //27
    { "rsslim",                T_MEMORY,     T_INTEGER,},
    { "signal",                T_INTEGER,    T_INTEGER,}, //29
    { "blocked",               T_INTEGER,    T_INTEGER,},
    { "sigignore",             T_INTEGER,    T_INTEGER,}, //31
    { "sigcatch",              T_INTEGER,    T_INTEGER,},
    { "rtPriority",            T_INTEGER,    T_INTEGER,}, //33
    { "policy",                T_INTEGER,    T_INTEGER,},
    { "delayacctBlkIOTicks",   T_INTEGER,    T_INTEGER,}, //35
    { "guestTime",             T_INTEGER,    T_INTEGER }, //36
    { "vmpeak",                T_MEMORY,     T_INTEGER }, //37
    { "rsspeak",               T_MEMORY,     T_INTEGER }, //38
    { "cpusAllowed",           T_INTEGER,    T_INTEGER}, //39
    { "io_rchar",              T_MEMORY,     T_INTEGER}, //40
    { "io_wchar",              T_MEMORY,     T_INTEGER}, //41
    { "io_syscr",              T_INTEGER,    T_INTEGER}, //42
    { "io_syscw",              T_INTEGER,    T_INTEGER}, //43
    { "io_readBytes",          T_MEMORY,     T_INTEGER}, //44
    { "io_writeBytes",         T_MEMORY,     T_INTEGER}, //45
    { "io_cancelledWriteBytes",T_MEMORY,     T_INTEGER}, //46
    { "m_size",                T_MEMORY,     T_INTEGER}, //47
    { "m_resident",            T_MEMORY,     T_INTEGER}, //48
    { "m_share",               T_MEMORY,     T_INTEGER}, //49
    { "m_text",                T_MEMORY,     T_INTEGER}, //50
    { "m_data",                T_MEMORY,     T_INTEGER}, //51
    { "stateSince",            T_INTEGER,    T_INTEGER}, //52
    { "stateAge",              T_INTEGER,    T_INTEGER}, //53
    { "delta_stime",           T_TICKS,      T_INTEGER}, //54
    { "delta_utime",           T_TICKS,      T_INTEGER}, //55
    { "delta_ioread",          T_MEMORY,     T_INTEGER}, //56
    { "delta_iowrite",         T_MEMORY,     T_INTEGER}, //57
    { "cputicks",              T_TICKS,      T_INTEGER}, //58
    { "delta_cputicks",        T_TICKS,      T_INTEGER}, //59
    { "io",                    T_MEMORY,     T_INTEGER}, //60
    { "delta_io",              T_MEMORY,     T_INTEGER}, //61
    { "age",                   T_INTEGER,    T_INTEGER}, //62
    { "state",                 T_CHAR,       T_CHAR}, //63
    { "fdpath",                T_STRING,     T_STRING},
    { "fdmode",                T_INTEGER,    T_INTEGER},
};

qpDataSource::qpDataSource(Cache<procdata> *_pd, Cache<procstat> *_ps, Cache<procfd> *_fd):
    pd(_pd), ps(_ps), fd(_fd)
{
    iterateOverPS = false;
    iterateOverPD = false;
    iterateOverFD = false;
    haveNameExpr  = false;
}

qpDataSource::~qpDataSource() {
}

void qpDataSource::prepareQuery(vector<nclq::Expression *>& declarations,
            vector<nclq::Expression *>& queries, 
            vector<nclq::Expression *>& output)
{
    for (auto it: declarations) prepareQuery(it);
    for (auto it: queries) prepareQuery(it);
    for (auto it: output) prepareQuery(it);

    /* check if user just requesting data from data structure headers */
    if (haveNameExpr && !iterateOverPS && !iterateOverPD && !iterateOverFD) {
        iterateOverPD = true;
    }
}

void qpDataSource::prepareQuery(nclq::Expression *expr) {
    /* walk expression tree and discover any NameExpression(s)
       and try to bind the complexity of the query matching */

    // check to see if we're already doing maximum work
    if (iterateOverPS && iterateOverPD && iterateOverFD) return;

    stack<nclq::Expression *> exprStack;
    exprStack.push(expr);
    while (exprStack.size() > 0) {
        nclq::Expression *ex = exprStack.pop();
        if (ex == NULL) continue;

        switch (ex->getExpressionType()) {
            case nclq::EXPR_VARIABLE:
            case nclq::EXPR_NAME:
                {
                    NameExpression *namex = dynamic_cast<NameExpression*>(ex);
                    if (namex != NULL) {
                        haveNameExpr = true;
                        const size_t varIdx = namex->getVariableIndex();
                        if (varIdx > 5 && varIdx <= 10) iterateOverPD = true;
                        else if (varIdx > 10 && varIdx <= 63) iterateOverPS = true;
                        else if (varIdx > 63 && varIdx <= 65) iterateOverFD = true;
                    }
                }
                break;
            case nclq::EXPR_PLUS:
            case nclq::EXPR_MINUS:
            case nclq::EXPR_TIMES:
            case nclq::EXPR_DIVIDE:
            case nclq::EXPR_MODULUS:
            case nclq::EXPR_EXPONENT:
            case nclq::EXPR_EQUAL:
            case nclq::EXPR_NEQUAL:
            case nclq::EXPR_LESS:
            case nclq::EXPR_GREATER:
            case nclq::EXPR_LESSEQUAL:
            case nclq::EXPR_GREATEREQUAL:
            case nclq::EXPR_AND:
            case nclq::EXPR_OR:
            case nclq::EXPR_ASSIGN:
                /* BinExpression: look left, look right */
                {
                    BinExpression *binex = dynamic_cast<BinExpression*>(ex);
                    if (binex != NULL) {
                        exprStack.push(binex->left);
                        exprStack.push(binex->right);
                    }
                }
                break;
            case nclq::EXPR_NOT:
            case nclq::EXPR_GROUP:
                /* MonExpression: expr */
                {
                    MonExpression *monex = dynamic_cast<MonExpression*>(ex);
                    if (monex != NULL) {
                        exprStack.push(monex->expr);
                    }
                }
                break;
            case nclq::EXPR_FUNCTION:
                /* FxnExpression: expr */
                {
                    FxnExpresion *fxnex = dynamic_cast<FxnExpression*>(ex);
                    if (fxnex != NULL) {
                        exprStack.push(fxnex->expr);
                    }
                }
                break;
            case nclq::EXPR_EXPRESSIONLIST:
                /* ExpressionList: vec<Exp*> expressions */
                {
                    ExpressionList *exlist = dynamic_cast<ExpressionList*>(ex);
                    if (exlist != NULL) {
                        for (auto it: exlist->expressions) {
                            exprStack.push(it);
                        }
                    }
                }
                break;
        }
    }
}

const vector<nclq::VarDescriptor>& qpDataSource::getSymbols() const {
    return symbols;
}

const bool qpDataSource::getNext() {
    if (iterateOverFD) {

    return false;
}

const bool qpDataSource::setValue(int idx, nclq::Data *data, nclq::SymbolType outputType) {
    return false;
}

int qpDataSource::size() {
    return symbols.size();
}

virtual void qpDataSource::outputValue(ostream& out, int idx) {
}

QProcConfiguration::QProcConfiguration(int argc, char** argv) {

	/* setup default values */
	verbose = false;
	configFile = "";
	header = "";

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
		;

    po::positional_options_description pos_opts;
    pos_opts.add("query", 1);

	po::options_description options;
	options.add(basic).add(config);

	po::variables_map vm;
	po::store(po::command_line_parser(argc, argv).options(options).positional(pos_opts).run(), vm);
	po::notify(vm);


	if (vm.count("verbose")) {
		verbose = true;
	}

	if (vm.count("file") && configFile.length() > 0) {
		std::ifstream ifs(configFile.c_str());
		if (!ifs) {
			std::cout << "Config file doesn't exist: " << std::endl;
			exit(1);
		}
		po::store(po::parse_config_file(ifs, config), vm);
		po::notify(vm);
	}

	// setup initial symbol table
	for (unsigned int i = 0; i < maxVarSize; i++) {
		qVariables.push_back(qVariablesBasic[i]);
		std::string id(qVariablesBasic[i].name);
		qVariableMap[id] = i;
	}

	/* add aliases */
	qVariableMap["u"] = qVariableMap["effUid"];
    qVariableMap["uid"] = qVariableMap["effUid"];
    qVariableMap["user"] = qVariableMap["effUid"];
	qVariableMap["job"] = qVariableMap["identifier"];
	qVariableMap["task"] = qVariableMap["subidentifier"];
    qVariableMap["vmem"] = qVariableMap["vsize"];


	/* now that symbol table is setup, display help & exit if requested */
	if (vm.count("help")) {
		std::cout << options << std::endl;
		std::cout << std::endl << "Valid built-in identifiers for query/output expressions:" << std::endl;
		int lineLen = 0;
		std::map<std::string,int>::const_iterator start = qVariableMap.begin();
		for (std::map<std::string,int>::const_iterator iter = start, end = qVariableMap.end(); iter != end; iter++) {
			if (iter != start) {
				std::cout << ", ";
				lineLen += 2;
			}
			if (lineLen > 80) {
				std::cout << std::endl;
				lineLen = 0;
			}
			std::cout << (*iter).first;
			lineLen += (*iter).first.length();
		}
		std::cout << std::endl << std::endl;
		
		exit(1);
	}


	// set default outputColumns if unspecified
	if (outputColumns.size() == 0) {
		std::ostringstream oss;
		oss << "job" << delimiter << "task" << delimiter << "hostname" << delimiter << "pid" << delimiter << "user" << delimiter;
		oss << "rss" << delimiter << "vmem" << delimiter << "start" << delimiter << "obs" << delimiter << "state";
		header = oss.str();
		//header="job_number,task_number,hostname,owner,project,job_name,submission,start,end,failed,exit_status,h_rt,wallclock,h_vmem,maxvmem";
		outputColumns.push_back("job");
		outputColumns.push_back("task");
		outputColumns.push_back("hostname");
		outputColumns.push_back("pid");
		outputColumns.push_back("getent_user(uid)");
		outputColumns.push_back("memory(rss)");
		outputColumns.push_back("memory(vmem)");
		outputColumns.push_back("datetime(start_time)");
		outputColumns.push_back("datetime(obs_time)");
		outputColumns.push_back("state");
	}


}

std::ostream& operator<<(std::ostream &out, Data& data) {
	switch (data.type) {
		case T_INTEGER:
			out << data.i_value;
			break;
		case T_DOUBLE:
			out << data.d_value;
			break;
		case T_STRING:
			out << data.s_value;
			break;
		case T_BOOL:
			out << data.b_value;
			break;
		case T_MEMORY:
			out << data.d_value / (1024*1024*1024) << "G";
			break;
	}
	return out;
}
