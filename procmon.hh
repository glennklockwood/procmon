#ifndef __PROCMON_CONFIG_HH_
#define __PROCMON_CONFIG_HH_

#include "ProcData.hh"
#include <unistd.h>
#include <getopt.h>
#include <stdlib.h>
#include <vector>

#include "config.h"

#define PROCMON_VERSION "2.1"

#ifndef DEFAULT_FREQUENCY
#define DEFAULT_FREQUENCY 60
#define DEFAULT_INITIAL_PHASE 0
#define DEFAULT_INITIAL_PHASE_FREQUENCY 1
#define DEFAULT_OUTPUT_FLAGS 0
#define DEFAULT_IDENTIFIER "proc"
#define DEFAULT_SUBIDENTIFIER "mon"
#define DEFAULT_AMQP_HOST "localhost"
#define DEFAULT_AMQP_PORT 5672
#define DEFAULT_AMQP_VHOST "/"
#define DEFAULT_AMQP_EXCHANGE_NAME "procmon"
#define DEFAULT_AMQP_USER "guest"
#define DEFAULT_AMQP_PASSWORD "guest"
#define DEFAULT_AMQP_FRAMESIZE 131072
#endif

time_t getBootTime();
int parseProcStatus(int pid, int tgtGid, procstat* statData, int* gidList, int gidListLimit);
int fileFillBuffer(FILE* fp, char* buffer, int buffSize, char** sptr, char** ptr, char** eptr);

class ProcmonException : public std::exception {
public:
    ProcmonException(const char* t_error) {
        error = t_error;
    }

    virtual const char* what() const throw() {
        return error;
    }
private:
    const char *error;
};

class ProcmonConfig {
public:
    /* Configurable monitoring options */
    int targetPPid;
    int frequency;
    int initialFrequency;
    int initialPhase;
    bool daemonize;
    bool verbose;
	int debug;
    int maxfd;
    std::string identifier;
    std::string subidentifier;

    /* Derived monitoring inputs */
    int tgtGid;
    long clockTicksPerSec;
    long pageSize;
    time_t boottime;
    std::string hostname;

    /* Output Options */
    unsigned int outputFlags;
    std::string outputTextFilename;
#ifdef __USE_HDF5
    std::string outputHDF5Filename;
#endif

    /* AMQP options */
    std::string mqServer;
    unsigned int mqPort;
	std::string mqVHost;
	std::string mqUser;
	std::string mqPassword;
    std::string mqExchangeName;
    unsigned int mqFrameSize;

    ProcmonConfig(int argc, char** argv) {
        int gid_range_min = -1;
        int gid_range_max = -1;
        /* Initialize defaults */
        targetPPid = 1;
        frequency = DEFAULT_FREQUENCY;
        initialFrequency = DEFAULT_INITIAL_PHASE_FREQUENCY;
        initialPhase = DEFAULT_INITIAL_PHASE;
        clockTicksPerSec = 0;
        pageSize = 0;
        daemonize = false;
        verbose = false;
		debug = 0;
        outputFlags = DEFAULT_OUTPUT_FLAGS;
        tgtGid = 0;
        maxfd = 0;

		mqServer = DEFAULT_AMQP_HOST;
		mqPort = DEFAULT_AMQP_PORT;
		mqUser = DEFAULT_AMQP_USER;
		mqExchangeName = DEFAULT_AMQP_EXCHANGE_NAME;
		mqFrameSize = DEFAULT_AMQP_FRAMESIZE;

		identifier = DEFAULT_IDENTIFIER;
		subidentifier = DEFAULT_SUBIDENTIFIER;

        /* Setup Context-derived values */
        char buffer[BUFFER_SIZE];
        if (gethostname(buffer, BUFFER_SIZE) != 0) {
            snprintf(buffer, BUFFER_SIZE, "Unknown");
        }
        hostname = buffer;

        clockTicksPerSec = sysconf(_SC_CLK_TCK);
        pageSize = sysconf(_SC_PAGESIZE);
        boottime = getBootTime();

        /* Parse command line arguments */
        struct option prg_options[] = {
            {"help",    no_argument, 0, 'h'},
            {"version", no_argument, 0, 'V'},
            {"verbose", no_argument, 0, 'v'},
            {"daemonize", no_argument, 0, 'd'},
            {"frequency", required_argument, 0, 'f'},
            {"initialphase", required_argument, 0, 'i'},
            {"initialfrequency", required_argument, 0, 'F'},
            {"ppid", required_argument, 0, 'p'},
            {"group_min", required_argument, 0, 'g'},
            {"group_max", required_argument, 0, 'G'},
            {"fd_max", required_argument, 0, 'W'},
            {"identifier", required_argument, 0, 'I'},
            {"subidentifier", required_argument, 0, 'S'},
            {"outputtext", required_argument, 0, 'o'},
            {"outputhdf5", required_argument, 0, 'O'},
            {"debug", required_argument, 0, 'D'},
            {"mqhostname", required_argument, 0, 'H'},
            {"mqport", required_argument, 0, 'P'},
            {"mqvhost", required_argument, 0, 'Q'},
            {"mqexchange", required_argument, 0, 'E'},
            {"mquser", required_argument, 0, 'U'},
            {"mqpassword", required_argument, 0, 'Y'},
            {"mqframe", required_argument, 0, 'R'},
            { 0, 0, 0, 0},
        };
        int c;

        for ( ; ; ) {
            int option_index = 0;
            c = getopt_long(argc, argv,
                    "hVvdf:i:F:p:W:g:G:I:S:o:O:D:H:P:E:Q:U:Y:R:",
                    prg_options, &option_index
            );
            if (c == -1) {
                break;
            }
            switch (c) {
                case 'h': usage(0); break;
                case 'V': version(); break; 
                case 'v': verbose = true; break;
                case 'd': daemonize = true; break;
                case 'f':
                    frequency = atoi(optarg);
                    if (frequency <= 0) {
                        cerr << "Frequency must be at least 1 second!" << endl;
                        usage(1);
                    }
                    break;
                case 'i':
                    initialPhase = atoi(optarg);
                    if (initialPhase <= 0) {
                        cerr << "Initial-phase, if specified,  must be at least 1 second!" << endl;
                        usage(1);
                    }
                    break;
                case 'F':
                    initialFrequency = atoi(optarg);
                    if (initialFrequency <= 0) {
                        cerr << "Initial-frequency, if specified,  must be at least 1 second!" << endl;
                        usage(1);
                    }
                    break;
                case 'p':
                    targetPPid = atoi(optarg);
                    if (targetPPid <= 0) {
                        cerr << "ppid must be at least 1 (should be 1 to track all user-space processes)" << endl;
                        usage(1);
                    }
                    break;
                case 'W':
                    maxfd = atoi(optarg);
                    if (maxfd <= 2) {
                        cerr << "fd_max, if specified, must be at least 3 (fd 0, 1, 2 are never read)" << endl;
                        usage(1);
                    }
                    break;
                case 'g':
                    gid_range_min = atoi(optarg);
                    if (gid_range_min <= 0) {
                        cerr << "gid range minimum, if specified,  must be at least 1!" << endl;
                        usage(1);
                    }
                    break;
                case 'G':
                    gid_range_max = atoi(optarg);
                    if (gid_range_max <= 1) {
                        cerr << "gid range maximum, if specified,  must be at least 2!" << endl;
                        usage(1);
                    }
                    break;
                case 'I': identifier = string(optarg); break;
                case 'S': subidentifier = string(optarg); break;
                case 'o': outputTextFilename = string(optarg); break;
                case 'O':
#ifdef __USE_HDF5
                    outputHDF5Filename = string(optarg);
#else
                    cerr << "HDF5 output is not supported by this build of procmon." << endl;
                    exit(1);
#endif                   
                    break;
                case 'D':
                    debug = atoi(optarg);
                    if (debug < 0) {
                        cerr << "debug must be a non-negative integer" << endl;
                        usage(1);
                    }
                    break;
                case 'H': mqServer = string(optarg); break;
                case 'P':
                    mqPort = (unsigned int) atoi(optarg);
                    break;
                case 'E': mqVHost = string(optarg); break;
                case 'Q': mqExchangeName = string(optarg); break;
                case 'U': mqUser = string(optarg); break;
                case 'Y': mqPassword = string(optarg); break;
                case 'R': 
                    mqFrameSize = (unsigned int) atoi(optarg);
                    break;
                case ':': usage(1); break;

            }
        }
	
        if (outputTextFilename != "") {
            outputFlags |= OUTPUT_TYPE_TEXT;
        }
#ifdef __USE_HDF5
        if (outputHDF5Filename != "") {
            outputFlags |= OUTPUT_TYPE_HDF5;
        }
#endif
        if (mqServer != "" && mqServer != "__NONE__") {
            outputFlags |= OUTPUT_TYPE_AMQP;
        }

        if (outputFlags == 0) {
            cout << "No output mechanism specified (text, hdf5, or AMQP)" << endl;
            usage(1);
        }

        /* deal with finding secondary gid mapping if applicable */
        if (gid_range_min > 0 && gid_range_max > 0) {
            if (gid_range_min > gid_range_max) {
                int temp = gid_range_min;
                gid_range_min = gid_range_max;
                gid_range_max = temp;
            }
            procstat self;
            int processGids[64];
            int foundGroups = parseProcStatus(getpid(),-1,&self,processGids,64);
            for (int i = 0; i < foundGroups; i++) {
                if (processGids[i] >= gid_range_min && processGids[i] <= gid_range_max) {
                    tgtGid = processGids[i];
                    break;
                }
            }
        }
	}

    void usage(int err_code) {
        cout << "procmon - NERSC process monitor" << endl << endl;
        cout << "Basic Options:" << endl;
        cout << "  -h [ --help ]      Print help message" << endl;
        cout << "  -V [ --version ]   Print procmon version" << endl;
        cout << "  -v [ --verbose ]   Print extra information" << endl;
        cout << endl;
        cout << "Configuration Options:" << endl;
        cout << "  -d [ --daemonize ]                 Daemonize the procmon process" << endl;
        cout << "  -f [ --frequency ] integer (=30)   Time elapsed between measurements" << endl;
        cout << "                                     during normal data collection (in" << endl;
        cout << "                                     seconds)" << endl;
        cout << "  -i [ --initialphase ] integer (=30)     Length of the initial phase (in" << endl;
        cout << "                                          seconds)" << endl;
        cout << "  -F [ --initialfrequency ] integer (=30) Time elapsed between measurements" << endl;
        cout << "                                          during initial phase (in seconds)" << endl;
        cout << "  -p [ --ppid ] integer (=1)              Parent process ID of monitorying" << endl;
        cout << "                                          hierarchy" << endl;
        cout << "  -g [ --group_min ] integer              Minimum group id in GridEngine gid" << endl;
        cout << "                                          range (second group process ident)" << endl;
        cout << "  -G [ --group_max ] integer              Maximum group id in GridEngine gid" << endl;
        cout << "                                          range (second group process ident)" << endl;
        cout << "  -I [ --identifier ] string (=JOB_ID)    identifier for tagging data" << endl;
        cout << "  -S [ --subidentifier ] string (=SGE_TASK_ID)" << endl;
        cout << "                                          secondary identifier for tagging" << endl;
        cout << "                                          data" << endl;
        cout << "  -o [ --outputtext ] string              filename for text output (optional)" << endl;
        cout << "  -O [ --outputhdf5 ] string              filename for HDF5 output (optional)" << endl;
        cout << "  -D [ --debug ] integer (=0)             debugging level" << endl;
        cout << endl;
        cout << "AMQP Configuration Options:" << endl;
        cout << "  -H [ --mqhostname ] string (=genepool10.nersc.gov)" << endl;
        cout << "                                          hostname for AMQP Server" << endl;
        cout << "  -P [ --mqport ] integer (=5672)         port for AMQP Server" << endl;
        cout << "  -Q [ --mqvhost ] string (=jgi)          virtual-host for AMQP Server" << endl;
        cout << "  -E [ --mqexchange ] string (=procmon)   exchange name for AMQP Server" << endl;
        cout << "  -U [ --mquser ] string  (built-in)      username for AMQP Server" << endl;
        cout << "  -Y [ --mqpasssword ] string (build-in)  password for AMQP Server" << endl;
        cout << "  -R [ --mqframe ] integer (=131072)      maximum frame size for AMQP Messages" << endl;
        cout << "                                          (bytes)" << endl;
        cout << endl;
        exit(err_code);
    }
    void version() {
        cout << "Procmon " << PROCMON_VERSION;
#ifdef __PROCMON_SECURED__
        cout << " (interactive)";
#endif
        cout << endl;
        exit(0);
    }
};


#endif
