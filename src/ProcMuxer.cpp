#include "config.h"
#include "ProcData.hh"
#include "ProcIO.hh"
#include <signal.h>
#include <string.h>
#include <iostream>

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <pthread.h>

#include <boost/program_options.hpp>
#define PROCMUXER_VERSION "2.4"
namespace po = boost::program_options;

/* ProcMuxer
    * Takes in procmon data from all sources and writes it all out to hdf5 without filtering
    * Writes an HDF5 file for all procdata foreach day
*/

/* global variables - these are global for signal handling */
int cleanUpExitFlag = 0;
int resetOutputFileFlag = 0;
string queue_name;

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
    cout << "ProcMuxer " << PROCMUXER_VERSION;
    cout << endl;
    exit(0);
}


class ProcMuxerConfig {
public:
    ProcMuxerConfig(int argc, char **argv) {
        string tempMem;
        char buffer[BUFFER_SIZE];
        bzero(buffer, BUFFER_SIZE);
        gethostname(buffer, BUFFER_SIZE);
        buffer[BUFFER_SIZE-1] = 0;
        group = string(buffer);
        my_id = getpid();
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
            ("pidfile,p",po::value<std::string>(&(this->pidfile))->default_value(""), "pid file")
            ("cleanfreq,c",po::value<int>(&(this->cleanFreq))->default_value(DEFAULT_REDUCER_CLEAN_FREQUENCY), "time between process hash table cleaning runs")
            ("maxage,m",po::value<int>(&(this->maxProcessAge))->default_value(DEFAULT_REDUCER_MAX_PROCESS_AGE), "timeout since last communication from a pid before allowing removal from hash table (cleaning)")
            ("maxwrites,w",po::value<unsigned long>(&(this->max_file_writes))->default_value(DEFAULT_REDUCER_MAX_FILE_WRITES), "maximum writes for a single file before resetting")
            ("totalwrites,t",po::value<unsigned long>(&(this->max_total_writes))->default_value(DEFAULT_REDUCER_MAX_TOTAL_WRITES), "total writes before exiting")
            //("maxmem,M",po::value<string>(tempMem)->default_vaulue(DEFAULT_REDUCER_MAX_MEMORY), "maximum vmem consumption before exiting")
            ("statblock",po::value<int>(&(this->statBlockSize))->default_value(DEFAULT_STAT_BLOCK_SIZE), "number of stat records per block in hdf5 file" )
            ("datablock",po::value<int>(&(this->dataBlockSize))->default_value(DEFAULT_DATA_BLOCK_SIZE), "number of data records per block in hdf5 file" )
            ("fdblock",po::value<int>(&(this->dataBlockSize))->default_value(DEFAULT_FD_BLOCK_SIZE), "number of data records per block in hdf5 file" )
            ("group,g",po::value<string>(&(this->group)), "ProcMuxer group (defaults to local hostname)")
            ("id,i",po::value<int>(&(this->my_id)), "ProcMuxer ID (defaults to pid)")
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
    unsigned long max_file_writes;
    unsigned long max_total_writes;
    unsigned long maxMem;
    bool debug;
    bool daemonize;
    string group;
    int my_id;

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
ProcMuxerConfig *config = NULL;

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
	umask(077);

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
    char buffer[BUFFER_SIZE];

    config = new ProcMuxerConfig(argc, argv);

	if (config->daemonize) {
		daemonize();
	}
    if (config->pidfile.size() > 0) {
        pidfile(config->pidfile);
    }

    queue_name = string("ProcMuxer_") + config->group;


    ProcAMQPIO *conn = new ProcAMQPIO(config->mqServer, config->mqPort, config->mqVHost, config->mqUser, config->mqPassword, config->mqExchangeName, config->mqFrameSize, FILE_MODE_READ);
    conn->set_queue_name(queue_name);
    conn->set_context(config->hostname, config->identifier, config->subidentifier);
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
    unsigned long n_writes = 0;
    unsigned long file_n_writes = 0;

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
                outputFile->metadata_set_uint("recording_stop", currTimestamp);
                outputFile->metadata_set_uint("n_writes", file_n_writes);
                outputFile->flush();
                delete outputFile;
                outputFile = NULL;
            }
            n_writes += file_n_writes;
            file_n_writes = 0;

            /* use the current date and time as the suffix for this file */
            strftime(buffer, BUFFER_SIZE, "%Y%m%d%H%M%S", &currTm);
            outputFilename = config->outputHDF5FilenamePrefix + "." + string(buffer) + ".h5";
            outputFile = new ProcHDF5IO(outputFilename, FILE_MODE_WRITE);
            outputFile->metadata_set_string("writer", "ProcMuxer");
            outputFile->metadata_set_string("writer_version", PROCMUXER_VERSION);
            gethostname(buffer, BUFFER_SIZE);
            buffer[BUFFER_SIZE-1] = 0;
            outputFile->metadata_set_string("writer_host", buffer);
            snprintf(buffer, BUFFER_SIZE, "amqp://%s@%s:%d/%s", config->mqUser.c_str(), config->mqServer.c_str(), config->mqPort, config->mqVHost.c_str());
            outputFile->metadata_set_string("source", buffer);
            outputFile->metadata_set_uint("recording_start", currTimestamp);

            resetOutputFileFlag = 0;
        }
        last_hour = currTm.tm_hour;

        ProcRecordType recordType = conn->read_stream_record(&data, &data_size, &nRecords);
        conn->get_frame_context(hostname, identifier, subidentifier);

        if (data == NULL) {
            continue;
        }

        string message_type = "NA";
        outputFile->set_context(hostname, identifier, subidentifier);
        if (recordType == TYPE_PROCDATA) {
            procdata *ptr = (procdata *) data;
            outputFile->write_procdata(ptr, 0, nRecords);
            file_n_writes++;
            message_type = "procdata";
        } else if (recordType == TYPE_PROCSTAT) {
            procstat *ptr = (procstat *) data;
            outputFile->write_procstat(ptr, 0, nRecords);
            file_n_writes++;
            message_type = "procstat";
        } else if (recordType == TYPE_PROCFD) {
            procfd *ptr = (procfd *) data;
            outputFile->write_procfd(ptr, 0, nRecords);
            file_n_writes++;
            message_type = "procfd";
        } else if (recordType == TYPE_NETSTAT) {
            netstat *ptr = (netstat *) data;
            outputFile->write_netstat(ptr, 0, nRecords);
            file_n_writes++;
            message_type = "netstat";
        }
        cerr << "Received message: " << hostname << "." << identifier << "." << subidentifier << "." << message_type << endl;

        /* check if # of writes to this file has exceeded max,
           if so, flush changes to disk and start writing a new file */
        if (file_n_writes > config->max_file_writes) {
            resetOutputFileFlag = 1;
        }

        time_t currTime = time(NULL);
        if (currTime - lastClean > config->cleanFreq || cleanUpExitFlag != 0 || resetOutputFileFlag == 1) {
            cout << "Begin Clean; Flushing data to disk..." << endl;
            outputFile->flush();
            outputFile->trim_segments(currTime - config->maxProcessAge);
            cout << "Flush Complete" << endl;

            int before_processes = 0;
            int before_process_capacity = 0;
            int after_processes = 0;
            int after_process_capacity = 0;
            time_t nowTime = time(NULL);
            time_t deltaTime = nowTime - currTime;
            cout << "Cleaning finished in " << deltaTime << " seconds" << endl;
            cout << "Data pool size: " <<  data_size << endl;
            lastClean = currTime;
            if (resetOutputFileFlag == 1) {
                resetOutputFileFlag++;
            }
        }
        if (config->max_total_writes > 0 && n_writes + file_n_writes >= config->max_total_writes) {
            cleanUpExitFlag = 1;
        }
    }
    free(data);

    if (outputFile != NULL) {
        outputFile->metadata_set_uint("recording_stop", time(NULL));
        outputFile->metadata_set_uint("n_writes", file_n_writes);
        outputFile->flush();
        delete outputFile;
        outputFile = NULL;
    }
    if (conn != NULL) {
        delete conn;
        conn = NULL;
    }
    delete config;
    return 0;
}
