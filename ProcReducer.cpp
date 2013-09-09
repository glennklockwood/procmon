#include "config.h"
#include "ProcData.hh"
#include "ProcIO.hh"
#include <signal.h>
#include <string.h>
#include <iostream>
#include <unordered_map>
#include "ProcReducerData.hh"

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <boost/program_options.hpp>
#define PROCREDUCER_VERSION 2.3
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

void version() {
    cout << "ProcReducer " << PROCREDUCER_VERSION;
    cout << endl;
    exit(0);
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
            ("pid,q",po::value<string>(&(this->pidfile))->default_value(""), "pid file to write")
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
            if (vm.count("version")) {
                version();
                exit(0);
            }
            if (vm.count("daemonize")) {
                daemonize = true;
            } else {
                daemonize = false;
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

    string pidfile;

    string outputHDF5FilenamePrefix;

    int cleanFreq;
    int maxProcessAge;
    int dataBlockSize;
    int statBlockSize;
    int fdBlockSize;

};

unsigned int get_record_pid(ProcRecordType type, void *data, int n) {
    if (type == TYPE_PROCDATA) {
        procdata *ptr = (procdata*) data;
        return ptr[n].pid;
    } else if (type == TYPE_PROCSTAT) {
        procstat *ptr = (procstat*) data;
        return ptr[n].pid;
    } else if (type == TYPE_PROCFD) {
        procfd *ptr = (procfd*) data;
        return ptr[n].pid;
    }
    return 0;
}

unsigned int set_process_record(ProcRecordType recordType, void *data, int i, ProcessRecord *record, bool newRecord, ProcHDF5IO *outputFile) {
    if (recordType == TYPE_PROCDATA) {
        procdata *ptr = (procdata *) data;
        unsigned int recId = record->set_procdata(&(ptr[i]), newRecord);
        record->set_procdata_id(
            outputFile->write_procdata(&(ptr[i]), recId, 1)
        );
    } else if (recordType == TYPE_PROCSTAT) {
        procstat *ptr = (procstat *) data;
        unsigned int recId = record->set_procstat(&(ptr[i]), newRecord);
        record->set_procstat_id(
            outputFile->write_procstat(&(ptr[i]), recId, 1)
        );
    } else if (recordType == TYPE_PROCFD) {
        procfd *ptr = (procfd *) data;
        try {
            unsigned int recId = record->set_procfd(&(ptr[i]), newRecord);
            record->set_procfd_id(
                outputFile->write_procfd(&(ptr[i]), recId, 1),
                &(ptr[i])
            );
        } catch (ReducerInvalidFDException &e) {
            cerr << "Caught (and ignored) invalid FD exception: " << e.what() << endl;
        }
    }
    return 0;
}

static void daemonize() {
	pid_t pid, sid;

	if (getppid() == 1) {
		return; // already daemonized
	}
	pid = fork();
	if (pid < 0) {
		exit(1); // failed to fork
	}
	if (pid > 0) {
		exit(0); // this is the parent, so exit
	}
	umask(0);

	sid = setsid();
	if (sid < 0) {
		exit(1);
	}

	if ((chdir("/")) < 0) {
		exit(1);
	}

	freopen("/dev/null", "r", stdin);
	freopen("/dev/null", "w", stdout);
	freopen("/dev/null", "w", stderr);
}

/* pidfile start-up routine.
   Should be called after daemonizing, but before any privilege reduction.

   Will check if an existing pid file exists, if it does it will read that
   pid file, check to see if that pid is still running.

   If the pid isn't running, then the existing pidfile will be unlinked
  
   If the pid is running, then exit()

   Finally, the results of getpid() will be written into the pidfile. If the
   pid file fails to write, then exit()
*/
void pidfile(const string& pidfilename) {
    if (pidfilename.length() == 0) return;

    FILE *pidfile = NULL;
    char buffer[BUFFER_SIZE];
    pid_t my_pid = getpid();

    /* try to open existing pidfile */
    if ((pidfile = fopen(pidfilename.c_str(), "r")) != NULL) {

        /* try to read the pid */
        int readBytes = fread(buffer, sizeof(char), BUFFER_SIZE, pidfile);
        fclose(pidfile);

        if (readBytes > 0) {
            pid_t pid = atoi(buffer);

            if (pid != 0 && pid != my_pid) {
                struct stat st_stat;
                snprintf(buffer, BUFFER_SIZE, "/proc/%d/status", pid);
                if (stat(buffer, &st_stat) == 0) {
                    /* the process still exists! */
                    fprintf(stderr, "Process %d is still running, exiting.\n", pid);
                    exit(0);
                } else {
                    unlink(pidfilename.c_str());
                }
            }
        }
    }
    if ((pidfile = fopen(pidfilename.c_str(), "w")) != NULL) {
        fprintf(pidfile, "%d\n", my_pid);
        fclose(pidfile);
        return;
    }
    fprintf(stderr, "FAILED to write pidfile %s, exiting.", pidfilename.c_str());
    exit(1);
}

int main(int argc, char **argv) {
    cleanUpExitFlag = 0;
    resetOutputFileFlag = 0;

    ProcReducerConfig config(argc, argv);

	if (config.daemonize) {
		daemonize();
	}
    if (config.pidfile.length() > 0) {
        pidfile(config.pidfile);
    }

    ProcAMQPIO *conn = new ProcAMQPIO(config.mqServer, config.mqPort, config.mqVHost, config.mqUser, config.mqPassword, config.mqExchangeName, config.mqFrameSize, FILE_MODE_READ);
    conn->set_context(config.hostname, config.identifier, config.subidentifier);
    ProcHDF5IO* outputFile = NULL;

    time_t currTimestamp = time(NULL);
    time_t lastClean = 0;
    struct tm currTm;
    memset(&currTm, 0, sizeof(struct tm));
    currTm.tm_isdst = -1;
    localtime_r(&currTimestamp, &currTm);
    int last_hour = currTm.tm_hour;
    string hostname, identifier, subidentifier;

    int saveCnt = 0;
    int nRecords = 0;
    string outputFilename;
    int count = 0;
    void* data = NULL;
    size_t data_size = 0;

    char buffer[1024];
    unordered_map<std::string, ProcessList*> processLists;
    processLists.reserve(1000);
    ProcessList* spare_processList = new ProcessList(config.maxProcessAge);

    signal(SIGINT, sig_handler);
    signal(SIGTERM, sig_handler);
    signal(SIGXCPU, sig_handler);
    signal(SIGUSR1, sig_handler);
    signal(SIGUSR2, sig_handler);
    signal(SIGHUP, sighup_handler);

    while (cleanUpExitFlag == 0) {
        currTimestamp = time(NULL);
        localtime_r(&currTimestamp, &currTm);

        if (currTm.tm_hour !=  last_hour || outputFile == NULL || resetOutputFileFlag == 2) {
            /* flush the file buffers and clean up the old references to the old file */
            if (outputFile != NULL) {
                outputFile->flush();
                delete outputFile;
                outputFile = NULL;
            }

            /* dump the hash table - we need a fresh start for a fresh file */
            for (auto iter = processLists.begin(), end = processLists.end(); iter != end; ++iter) {
                ProcessList* list = iter->second;
                list->expire_all_processes();
            }

            /* use the current date and time as the suffix for this file */
            strftime(buffer, 1024, "%Y%m%d%H%M%S", &currTm);
            outputFilename = config.outputHDF5FilenamePrefix + "." + string(buffer) + ".h5";
            outputFile = new ProcHDF5IO(outputFilename, FILE_MODE_WRITE);
            resetOutputFileFlag = 0;
        }
        last_hour = currTm.tm_hour;

        ProcRecordType recordType = conn->read_stream_record(&data, &data_size, &nRecords);
        conn->get_frame_context(hostname, identifier, subidentifier);

        if (data == NULL) {
            continue;
        }
        ProcessList* currList = NULL;
        auto procList_iter = processLists.find(hostname);
        if (procList_iter == processLists.end()) {
            /* new host! */
            currList = new ProcessList(config.maxProcessAge);
            auto insertVal = processLists.insert({hostname, currList});
            if (insertVal.second) {
                procList_iter = insertVal.first;
            } else {
                cout << "couldn't insert new host processlist for: " << hostname << endl;
                delete currList;
                currList = NULL;
            }
        } else {
            currList = procList_iter->second;
        }

        outputFile->set_context(hostname, identifier, subidentifier);
        int last_pid = -1;
        ProcessRecord *last_record = NULL;
        for (int i = 0; i < nRecords; i++) {
            unsigned int recId = 0;
            bool saveNewRec = false;
            unsigned int recID = 0;
            ProcessRecord* record = NULL;

            int pid = get_record_pid(recordType, data, i);
            if (currList != NULL) {
                bool newRecord = false;
                record = currList->find_process_record(pid);
                if (record == NULL) {
                    record = currList->new_process_record(spare_processList);
                    newRecord = true;
                } 
                if (record == NULL) {
                    cout << "couldn't get a new record!" << endl;
                } else {
                    set_process_record(recordType, data, i, record, newRecord, outputFile);
                }
            }
        }

        time_t currTime = time(NULL);
        if (currTime - lastClean > config.cleanFreq || cleanUpExitFlag != 0 || resetOutputFileFlag == 1) {
            cout << "Begin Clean; Flushing data to disk..." << endl;
            outputFile->flush();
            outputFile->trim_segments(currTime - config.maxProcessAge);
            cout << "Flush Complete" << endl;

            int before_processes = 0;
            int before_process_capacity = 0;
            int after_processes = 0;
            int after_process_capacity = 0;
            for (auto iter = processLists.begin(), end = processLists.end(); iter != end; ++iter) {
                ProcessList *list = iter->second;
                before_processes += list->get_process_count();
                before_process_capacity += list->get_process_capacity();
                list->find_expired_processes(spare_processList);
                after_processes += list->get_process_count();
                after_process_capacity += list->get_process_capacity();
            }
            time_t nowTime = time(NULL);
            time_t deltaTime = nowTime - currTime;
            cout << "Cleaning finished in " << deltaTime << " seconds" << endl;
            cout << "End Clean: Presently tracking " << after_processes << " processes (removed " << (before_processes - after_processes) << ")" << endl;
            cout << "Capacity for " << after_process_capacity << " processes." << endl;
            cout << "Reserve capacity: " << spare_processList->get_process_capacity() << endl;
            lastClean = currTime;
            if (resetOutputFileFlag == 1) {
                resetOutputFileFlag++;
            }
        }
    }
    free(data);

    /* clean up all the records */
    for (auto iter = processLists.begin(), end = processLists.end(); iter != end; ) {
        ProcessList *record = iter->second;
        delete record;
        processLists.erase(iter++);
    }
    if (outputFile != NULL) {
        outputFile->flush();
        delete outputFile;
        outputFile = NULL;
    }
    if (conn != NULL) {
        delete conn;
        conn = NULL;
    }
    if (spare_processList != NULL) {
        delete spare_processList;
    }
    return 0;
}
