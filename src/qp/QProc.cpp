
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
namespace fs = boost::filesystem;

bool _sort_path_by_date(const fs::path& a, const fs::path& b) {
	return (fs::last_write_time(a) < fs::last_write_time(b));
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
