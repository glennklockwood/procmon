#include "config.h"
#include "ProcData.hh"
#include "ProcIO.hh"
#include <signal.h>
#include <string.h>
#include <iostream>
#include <vector>

#include <boost/program_options.hpp>
namespace po = boost::program_options;

/* ProcReducer
    * Takes in procmon data from all sources and only writes the minimal record
    * Writes an HDF5 file for all procdata foreach day
*/

/* global variables - these are global for signal handling */
int cleanUpExitFlag = 0;
int resetOutputFileFlag = 0;

void sig_handler(int signum) {
	/* if we receive any trapped signal, just set the cleanUpFlag
     * this will break the infinite loop and cause the message
     * buffer to get written out
     */
     cleanUpExitFlag = 1;
}

void sighup_handler(int signum) {
	/* if we receive any trapped signal, just set the cleanUpFlag
     * this will break the infinite loop and cause the message
     * buffer to get written out
     */
     resetOutputFileFlag = 1;
}

class proc_t {
public:
	proc_t(): statSet(false),dataSet(false) { }
	bool statSet;
	bool dataSet;
	procstat stat;
	procdata data;
	unsigned int statRecord;
	unsigned int dataRecord;
	int nData;
	int nStat;
};

int procdatacmp(const procdata& a, const procdata& b) {
	if (a.pid != b.pid) return 1;
	if (a.ppid != b.ppid) return 2;
	if (a.startTime != b.startTime) return 3;
	if (a.startTimeUSec != b.startTimeUSec) return 4;
	if (a.cmdArgBytes != b.cmdArgBytes) return 5;
	if (memcmp(a.cmdArgs, b.cmdArgs, a.cmdArgBytes) != 0) return 6;
	if (strcmp(a.exePath, b.exePath) != 0) return 7;
	if (strcmp(a.cwdPath, b.cwdPath) != 0) return 8;
	return 0;
}

int procstatcmp(const procstat& a, const procstat& b) {
	if (a.pid != b.pid) return 1;
	if (a.ppid != b.ppid) return 2;
	if (a.state != b.state) return 3;
	if (a.realUid != b.realUid) return 4;
	if (a.effUid != b.effUid) return 5;
	if (a.realGid != b.realGid) return 6;
	if (a.effGid != b.effGid) return 7;
	if (a.utime != b.utime) return 8;
	if (a.stime != b.stime) return 9;
	if (a.priority != b.priority) return 10;
	if (a.vsize != b.vsize) return 11;
	if (a.rss != b.rss) return 12;
	if (a.rsslim != b.rsslim) return 13;
	if (a.delayacctBlkIOTicks != b.delayacctBlkIOTicks) return 14;
	if (a.vmpeak != b.vmpeak) return 15;
	if (a.rsspeak != b.rsspeak) return 16;
	if (a.guestTime != b.guestTime) return 17;
	if (a.io_rchar != b.io_rchar) return 18;
	if (a.io_wchar != b.io_wchar) return 19;
	if (a.io_syscr != b.io_syscr) return 20;
	if (a.io_syscw != b.io_syscw) return 21;
	if (a.io_readBytes != b.io_readBytes) return 22;
	if (a.io_writeBytes != b.io_writeBytes) return 23;
	return 0;
}

class ProcReducerConfig {
public:
	ProcReducerConfig(int argc, char **argv) {
		/* Parse command line arguments */
		po::options_description basic("Basic Options");
		basic.add_options()
			("version", "Print version information")
			("help,h", "Print help message")
			("verbose,v", "Print extra (debugging) information")
		;
		po::options_description config("Configuration Options");
		config.add_options()
			("daemonize,d", "Daemonize the procmon process")
			("hostname,H",po::value<std::string>(&(this->hostname))->default_value(DEFAULT_REDUCER_HOSTNAME), "identifier for tagging data")
			("identifier,I",po::value<std::string>(&(this->identifier))->default_value(DEFAULT_REDUCER_IDENTIFIER), "identifier for tagging data")
			("subidentifier,S",po::value<std::string>(&(this->subidentifier))->default_value(DEFAULT_REDUCER_SUBIDENTIFIER), "secondary identifier for tagging data")
			("outputhdf5prefix,O",po::value<std::string>(&(this->outputHDF5FilenamePrefix))->default_value(DEFAULT_REDUCER_OUTPUT_PREFIX), "prefix for hdf5 output filename [final names will be prefix.YYYYMMDDhhmmss.h5 (required)")
			("cleanfreq,c",po::value<int>(&(this->cleanFreq))->default_value(DEFAULT_REDUCER_CLEAN_FREQUENCY), "time between process hash table cleaning runs")
			("maxage,m",po::value<int>(&(this->maxProcessAge))->default_value(DEFAULT_REDUCER_MAX_PROCESS_AGE), "timeout since last communication from a pid before allowing removal from hash table (cleaning)")
			("statblock",po::value<int>(&(this->statBlockSize))->default_value(DEFAULT_STAT_BLOCK_SIZE), "number of stat records per block in hdf5 file" )
			("datablock",po::value<int>(&(this->dataBlockSize))->default_value(DEFAULT_DATA_BLOCK_SIZE), "number of data records per block in hdf5 file" )
			("debug", "enter debugging mode")
		;
		po::options_description mqconfig("AMQP Configuration Options");
		mqconfig.add_options()
			("mqhostname,H", po::value<std::string>(&(this->mqServer))->default_value(DEFAULT_AMQP_HOST), "hostname for AMQP Server")
			("mqport,P",po::value<unsigned int>(&(this->mqPort))->default_value(DEFAULT_AMQP_PORT), "port for AMQP Server")
			("mqvhost,V",po::value<std::string>(&(this->mqVHost))->default_value(DEFAULT_AMQP_VHOST), "virtual-host for AMQP Server")
			("mqexchange,E",po::value<std::string>(&(this->mqExchangeName))->default_value("procmon"), "exchange name for AMQP Server")
			("mqUser",po::value<std::string>(&(this->mqUser))->default_value(DEFAULT_AMQP_USER), "virtual-host for AMQP Server")
			("mqPassword",po::value<std::string>(&(this->mqPassword)), "virtual-host for AMQP Server")
			("mqframe,F",po::value<unsigned int>(&(this->mqFrameSize))->default_value(DEFAULT_AMQP_FRAMESIZE), "maximum frame size for AMQP Messages (bytes)")
		;

		po::options_description options;
		options.add(basic).add(config).add(mqconfig);
		po::variables_map vm;
		try {
			po::store(po::command_line_parser(argc, argv).options(options).run(), vm);
			po::notify(vm);
			if (vm.count("help")) {
				std::cout << options << std::endl;
				exit(0);
			}
			if (vm.count("daemonize")) {
				daemonize = true;
			}
			if (vm.count("debug")) {
				debug = true;
			}
			if (vm.count("mqPassword") == 0) {
				mqPassword = DEFAULT_AMQP_PASSWORD;
			}
		} catch (std::exception &e) {
			std::cout << e.what() << std::endl;
			std::cout << options << std::endl;
			exit(1);
		}
	}
	string mqServer;
	string mqVHost;
	string mqExchangeName;
	string mqUser;
	string mqPassword;
	unsigned int mqPort;
	unsigned int mqFrameSize;
	bool debug;
	bool daemonize;

	string hostname;
	string identifier;
	string subidentifier;

	string outputHDF5FilenamePrefix;

	int cleanFreq;
	int maxProcessAge;
	int dataBlockSize;
	int statBlockSize;

};

int main(int argc, char **argv) {
	cleanUpExitFlag = 0;
	resetOutputFileFlag = 0;

    ProcReducerConfig config(argc, argv);
    ProcAMQPIO *conn = new ProcAMQPIO(config.mqServer, config.mqPort, config.mqVHost, config.mqUser, config.mqPassword, config.mqExchangeName, config.mqFrameSize, FILE_MODE_READ);
    //ProcAMQPIO conn(DEFAULT_AMQP_HOST, DEFAULT_AMQP_PORT, DEFAULT_AMQP_VHOST, DEFAULT_AMQP_USER, DEFAULT_AMQP_PASSWORD, DEFAULT_AMQP_EXCHANGE_NAME, DEFAULT_AMQP_FRAMESIZE, FILE_MODE_READ);
    conn->set_context(config.hostname, config.identifier, config.subidentifier);
    ProcHDF5IO* outputFile = NULL;

    time_t currTimestamp = time(NULL);
	time_t lastClean = 0;
    struct tm currTm;
    memset(&currTm, 0, sizeof(struct tm));
    currTm.tm_isdst = -1;
    localtime_r(&currTimestamp, &currTm);
    int last_day = currTm.tm_mday;
    string hostname, identifier, subidentifier;

	procstat* procstat_ptr = NULL;
	procdata* procdata_ptr = NULL;
	int saveCnt = 0;
	int nRecords = 0;
	string outputFilename;
	int count = 0;

	char buffer[1024];
	map<string,proc_t*> pidMap = new map<string,proc_t*>();

	signal(SIGINT, sig_handler);
	signal(SIGTERM, sig_handler);
	signal(SIGXCPU, sig_handler);
	signal(SIGUSR1, sig_handler);
	signal(SIGUSR2, sig_handler);
	signal(SIGHUP, sighup_handler);

    while (cleanUpExitFlag == 0) {
        currTimestamp = time(NULL);
        localtime_r(&currTimestamp, &currTm);

        if (currTm.tm_mday != last_day || outputFile == NULL || resetOutputFileFlag == 2) {
			/* flush the file buffers and clean up the old references to the old file */
            if (outputFile != NULL) {
				outputFile->flush();
                delete outputFile;
                outputFile = NULL;
            }

			/* dump the hash table - we need a fresh start for a fresh file */
			for (auto iter = pidMap->begin(), end = pidMap->end(); iter != end; ) {
				proc_t* record = iter->second;
				delete record;
			}
			delete pidMap;
			pidMap = new map<string,proc_t*>();

			/* use the current date and time as the suffix for this file */
			strftime(buffer, 1024, "%Y%m%d%H%M%S", &currTm);
			outputFilename = config.outputHDF5FilenamePrefix + "." + string(buffer) + ".h5";
            outputFile = new ProcHDF5IO(outputFilename, FILE_MODE_WRITE);
			resetOutputFileFlag = 0;
        }
        last_day = currTm.tm_mday;

		void* data = NULL;
        ProcRecordType recordType = conn->read_stream_record(&data, &nRecords);
		conn->get_frame_context(hostname, identifier, subidentifier);

		if (data == NULL) {
			continue;
		}
		if (recordType == TYPE_PROCDATA) {
			procdata_ptr = (procdata*) data;
		} else if (recordType == TYPE_PROCSTAT) {
			procstat_ptr = (procstat*) data;
		}

		outputFile->set_context(hostname, identifier, subidentifier);
        for (int i = 0; i < nRecords; i++) {
			int pid = 0;
			unsigned int recId = 0;
			bool saveNewRec = false;
			unsigned int recID = 0;
			proc_t* record = NULL;
			if (recordType == TYPE_PROCDATA) {
				pid = procdata_ptr[i].pid;
			} else if (recordType == TYPE_PROCSTAT) {
				pid = procstat_ptr[i].pid;
			}
			snprintf(buffer, 1024, "%s.%s.%s.%d", hostname.c_str(), identifier.c_str(), subidentifier.c_str(), pid);
			const char* key = buffer;
			map<string,proc_t*>::iterator pidRec_iter = pidMap->find(key);

			if (pidRec_iter == pidMap->end()) {
				/* new pid! */
				record = new proc_t;
				bzero(record, sizeof(proc_t));
				if (recordType == TYPE_PROCDATA) {
					memcpy(&(record->data), &(procdata_ptr[i]), sizeof(procdata));
					record->dataSet = true;
				} else if (recordType == TYPE_PROCSTAT) {
					memcpy(&(record->stat), &(procstat_ptr[i]), sizeof(procstat));
					record->statSet = true;
				}
				saveNewRec = true;
				cout << "new pid: " << key << endl;
				auto insertVal = pidMap->insert({string(key), record});
				if (insertVal.second) {
					pidRec_iter = insertVal.first;
				} else {
					/* this is bad, couldn't insert! */
					cout << "couldn't insert new process record" << endl;
					delete record;
					record = NULL;
				}
			} else {
				record = pidRec_iter->second;
				int cmpVal = 0;
				if (recordType == TYPE_PROCDATA) {
					recId = record->dataRecord;
					if (record->nData <= 1 || (cmpVal = procdatacmp(record->data, procdata_ptr[i])) != 0) {
						saveNewRec = true;
					}
					memcpy(&(record->data), &(procdata_ptr[i]), sizeof(procdata));
					record->nData++;
				} else if (recordType == TYPE_PROCSTAT) {
					recId = record->statRecord;
					if (record->nStat <= 1 || (cmpVal = procstatcmp(record->stat, procstat_ptr[i])) != 0) {
						saveNewRec = true;
					}
					memcpy(&(record->stat), (&procstat_ptr[i]), sizeof(procstat));
					record->nStat++;
				}
			}
			if (saveNewRec) { recId = 0; }
			if (record != NULL) {
				if (recordType == TYPE_PROCSTAT) {
					record->statRecord = outputFile->write_procstat(&(procstat_ptr[i]), recId, 1);
				} else if (recordType == TYPE_PROCDATA) {
					record->dataRecord = outputFile->write_procdata(&(procdata_ptr[i]), recId, 1);
				}
			}
		}
		free(data);

		if (config.debug) {
			outputFile->flush();
		}
		time_t currTime = time(NULL);
		if (currTime - lastClean > config.cleanFreq || cleanUpExitFlag != 0 || resetOutputFileFlag == 1) {
			cout << "Begin Clean: Presently tracking " << pidMap->size() << " processes" << std::endl;
			cout << "Flushing data to disk..." << endl;
			outputFile->flush();
			outputFile->trim_segments(currTime - config.maxProcessAge);
			cout << "Complete" << endl;
			cout << "Hash cleanup:" << endl;
			/* first trim out local hash */
			map<string,proc_t*> *t_pidMap = new map<string,proc_t*>();
			for (auto iter = pidMap->begin(), end = pidMap->end(); iter != end; ) {
				proc_t* record = iter->second;
				time_t maxTime = record->data.recTime > record->stat.recTime ? record->data.recTime : record->stat.recTime;
				if (currTime - maxTime > config.maxProcessAge) {
					delete record;
					pidMap->erase(iter++);
				} else {
					string key = iter->first;
					proc_t *record = iter->second;
					auto insertVal = t_pidMap->insert({key, record});
					++iter;
				}
			}
			delete pidMap;
			pidMap = t_pidMap;
			time_t nowTime = time(NULL);
			time_t deltaTime = nowTime - currTime;
			cout << "Cleaning finished in " << deltaTime << " seconds" << endl;
			cout << "End Clean: Presently tracking " << pidMap->size() << " processes" << std::endl;
			lastClean = currTime;
			if (resetOutputFileFlag == 1) {
				resetOutputFileFlag++;
			}
		}
    }

	/* clean up all the records */
	for (auto iter = pidMap->begin(), end = pidMap->end(); iter != end; ) {
		proc_t *record = iter->second;
		delete record;
		pidMap->erase(iter++);
	}
	delete pidMap;
	if (outputFile != NULL) {
		outputFile->flush();
		delete outputFile;
		outputFile = NULL;
	}
	if (conn != NULL) {
		delete conn;
		conn = NULL;
	}
    return 0;
}
