#ifndef __PROCMON_CONFIG_HH_
#define __PROCMON_CONFIG_HH_

#include "ProcData.hh"
#include <unistd.h>
#include <getopt.h>
#include <stdlib.h>
#include <vector>
#include <pwd.h>
#include <grp.h>

#include "config.h"

#define PROCMON_VERSION "2.4"

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
#ifdef SECURED
    int target_uid;
    int target_gid;
#endif
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
#ifdef USE_HDF5
    std::string outputHDF5Filename;
#endif
    string pidfile;

#ifdef USE_AMQP
    /* AMQP options */
    std::string mqServer;
    unsigned int mqPort;
	std::string mqVHost;
	std::string mqUser;
	std::string mqPassword;
    std::string mqExchangeName;
    unsigned int mqFrameSize;
#endif

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
#ifdef SECURED
        target_uid = -1;
        target_gid = -1;
#endif
#ifdef USE_AMQP
		mqServer = DEFAULT_AMQP_HOST;
		mqPort = DEFAULT_AMQP_PORT;
		mqUser = DEFAULT_AMQP_USER;
        mqPassword = DEFAULT_AMQP_PASSWORD;
        mqVHost = DEFAULT_AMQP_VHOST;
		mqExchangeName = DEFAULT_AMQP_EXCHANGE_NAME;
		mqFrameSize = DEFAULT_AMQP_FRAMESIZE;
#endif

		identifier = DEFAULT_IDENTIFIER;
		subidentifier = DEFAULT_SUBIDENTIFIER;
        pidfile = "";

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
#ifdef SECURED
            {"user", required_argument, 0, 'u'},
            {"group", required_argument, 0, 'r'},
#endif
            {"ppid", required_argument, 0, 'p'},
            {"group_min", required_argument, 0, 'g'},
            {"group_max", required_argument, 0, 'G'},
            {"fd_max", required_argument, 0, 'W'},
            {"identifier", required_argument, 0, 'I'},
            {"subidentifier", required_argument, 0, 'S'},
            {"outputtext", required_argument, 0, 'o'},
            {"pid", required_argument, 0, 'q'},
#ifdef USE_HDF5
            {"outputhdf5", required_argument, 0, 'O'},
#endif
            {"debug", required_argument, 0, 'D'},
#ifdef USE_AMQP
            {"mqhostname", required_argument, 0, 'H'},
            {"mqport", required_argument, 0, 'P'},
            {"mqvhost", required_argument, 0, 'Q'},
            {"mqexchange", required_argument, 0, 'E'},
            {"mquser", required_argument, 0, 'U'},
            {"mqpassword", required_argument, 0, 'Y'},
            {"mqframe", required_argument, 0, 'R'},
#endif
            { 0, 0, 0, 0},
        };
        int c;
        string getopt_str = "hVvdf:i:F:p:W:g:G:I:S:o:q:D:";
#ifdef USE_AMQP
        getopt_str += "H:P:E:Q:U:Y:R:";
#endif
#ifdef USE_HDF5
        getopt_str += "O:";
#endif
#ifdef SECURED
        getopt_str += "u:r:";
#endif

        for ( ; ; ) {
            int option_index = 0;
            c = getopt_long(argc, argv, getopt_str.c_str(),
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
#ifdef SECURED
                case 'u':
                    /* deal with USER arg */
                    if (optarg != NULL && strlen(optarg) > 0) {
                        struct passwd *user_data = getpwnam(optarg);
                        if (user_data == NULL) {
                            int uid = atoi(optarg);
                            if (uid > 0) {
                                user_data = getpwuid(uid);
                                if (user_data != NULL) {
                                    target_uid = user_data->pw_uid;
                                }
                            }
                        } else {
                            target_uid = user_data->pw_uid;
                        }
                    }
                    if (target_uid <= 0) {
                        cerr << "user, if specified, must resolve to a uid > 0" << endl;
                        usage(1);
                    }
                    break;
                case 'r':
                    /* deal with GROUP arg */
                    if (optarg != NULL && strlen(optarg) > 0) {
                        struct group *group_data = getgrnam(optarg);
                        if (group_data == NULL) {
                            int gid = atoi(optarg);
                            if (group_data != NULL) {
                                target_gid = group_data->gr_gid;
                            }
                        } else {
                            target_gid = group_data->gr_gid;
                        }
                    }
                    if (target_gid <= 0) {
                        cerr << "group, if specified, must resolve to a gid > 0" << endl;
                    }
                    break;
#endif
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
#ifdef USE_HDF5
                case 'O':
                    outputHDF5Filename = string(optarg);
                    break;
#endif                   
                case 'q':
                    pidfile = string(optarg);
                    break;
                case 'D':
                    debug = atoi(optarg);
                    if (debug < 0) {
                        cerr << "debug must be a non-negative integer" << endl;
                        usage(1);
                    }
                    break;
#ifdef USE_AMQP
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
#endif
                case ':': usage(1); break;

            }
        }
	
        if (outputTextFilename != "") {
            outputFlags |= OUTPUT_TYPE_TEXT;
        }
#ifdef USE_HDF5
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
        printf("procmon - NERSC process monitor");
#ifdef SECURED
        printf("; **Interactive Edition**");
#endif
        printf("\n");
        printf("Basic Options:\n");
        printf("  -h [ --help ]      Print help message\n");
        printf("  -V [ --version ]   Print procmon version\n");
        printf("  -v [ --verbose ]   Print extra information\n");
        printf("\n");
        printf("Configuration Options:\n");
        printf("  -d [ --daemonize ]                      Daemonize the procmon process\n");
#ifdef SECURED
        printf("  -u [ --user ] string/integer            username/uid to setuid\n");
        printf("  -g [ --group ] string/integer           group/gid to setgid\n");
#endif
        printf("  -f [ --frequency ] integer (=%02d)        Time elapsed between measurements\n", DEFAULT_FREQUENCY);
        printf("                                          during normal data collection (in\n");
        printf("                                          seconds)\n");
        printf("  -i [ --initialphase ] integer (=%02d)     Length of the initial phase (in\n", DEFAULT_INITIAL_PHASE);
        printf("                                          seconds)\n");
        printf("  -F [ --initialfrequency ] integer (=%02d) Time elapsed between measurements\n", DEFAULT_INITIAL_PHASE_FREQUENCY);
        printf("                                          during initial phase (in seconds)\n");
        printf("  -p [ --ppid ] integer (=%d)              Parent process ID of monitorying\n", 1);
        printf("                                          hierarchy\n");
        printf("  -g [ --group_min ] integer              Minimum group id in GridEngine gid\n");
        printf("                                          range (second group process ident)\n");
        printf("  -G [ --group_max ] integer              Maximum group id in GridEngine gid\n");
        printf("                                          range (second group process ident)\n");
        printf("  -I [ --identifier ] string (=%s)    identifier for tagging data\n", DEFAULT_IDENTIFIER);
        printf("  -S [ --subidentifier ] string (=%s)\n", DEFAULT_SUBIDENTIFIER);
        printf("                                          secondary identifier for tagging\n");
        printf("                                          data\n");
        printf("  -o [ --outputtext ] string              filename for text output (optional)\n");
#ifdef USE_HDF5
        printf("  -O [ --outputhdf5 ] string              filename for HDF5 output (optional)\n");
#endif
        printf("  -q [ --pid ] string                     filename for optional pid file\n");
        printf("  -D [ --debug ] integer (=0)             debugging level\n");
        printf("\n");
#ifdef USE_AMQP
        printf("AMQP Configuration Options:\n");
        printf("  -H [ --mqhostname ] string (=%s)\n", DEFAULT_AMQP_HOST);
        printf("                                          hostname for AMQP Server\n");
        printf("  -P [ --mqport ] integer (=%d)         port for AMQP Server\n", DEFAULT_AMQP_PORT);
        printf("  -Q [ --mqvhost ] string (=%s)          virtual-host for AMQP Server\n", DEFAULT_AMQP_VHOST);
        printf("  -E [ --mqexchange ] string (=%s)   exchange name for AMQP Server\n", DEFAULT_AMQP_EXCHANGE_NAME);
        printf("  -U [ --mquser ] string  (built-in)      username for AMQP Server\n");
        printf("  -Y [ --mqpasssword ] string (built-in)  password for AMQP Server\n");
        printf("  -R [ --mqframe ] integer (=%d)      maximum frame size for AMQP Messages\n", DEFAULT_AMQP_FRAMESIZE);
        printf("                                          (bytes)\n");
#endif
        printf("\n");
        exit(err_code);
    }
    void version() {
        cout << "Procmon " << PROCMON_VERSION;
#ifdef SECURED
        cout << " (interactive)";
#endif
        cout << endl;
        exit(0);
    }
};


#endif
