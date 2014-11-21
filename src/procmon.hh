#ifndef __PROCMON_CONFIG_HH_
#define __PROCMON_CONFIG_HH_

#include "ProcData.hh"
#include <unistd.h>
#include <stdlib.h>
#include <vector>
#include <pwd.h>
#include <grp.h>

#include <boost/program_options.hpp>
#include <boost/filesystem.hpp>
#include <boost/bind.hpp>

#include <iostream>
#include <fstream>

namespace po = boost::program_options;
namespace fs = boost::filesystem;

using namespace std;

#include "config.h"

#ifndef DEFAULT_FREQUENCY
#define DEFAULT_FREQUENCY 60
#define DEFAULT_INITIAL_PHASE 0
#define DEFAULT_INITIAL_PHASE_FREQUENCY 1
#define DEFAULT_OUTPUT_FLAGS 0
#define DEFAULT_SYSTEM "cluster"
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
int fileFillBuffer(FILE* fp, char* buffer, int buffSize, char** sptr, char** ptr, char** eptr);

class ProcmonException : public exception {
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
    protected:
    po::options_description procmonOptions;
    string userConfigFile;
    int maxIterations;
    int syslog_facility;
    int syslog_priority_min;

    void setSyslogFacility(const string &facility);
    void setSyslogPriorityMin(const string &priority);

    public:
    inline const int getSyslogFacility() const {
        return syslog_facility;
    }
    inline const int getSyslogPriorityMin() const {
        return syslog_priority_min;
    }

    /* Configurable monitoring options */
    int targetPPid;
    int frequency;
    int initialFrequency;
    int initialPhase;
    bool daemonize;
    bool verbose;
    bool craylock;
    int maxfd;
#ifdef SECURED
    int target_uid;
    int target_gid;
    string user;
    string group;
#endif
    string system;
    string identifier;
    string subidentifier;
    string identifier_env;
    string subidentifier_env;

    int gid_range_min;
    int gid_range_max;

    /* Derived monitoring inputs */
    int tgtGid;
    int tgtSid;
    int tgtPgid;
    long clockTicksPerSec;
    long pageSize;
    time_t boottime;
    string hostname;

    /* Output Options */
    unsigned int outputFlags;
    string outputTextFilename;
#ifdef USE_HDF5
    string outputHDF5Filename;
#endif
    bool noOutput;
    string pidfile;

#ifdef USE_AMQP
    /* AMQP options */
    string mqServer;
    vector<string> mqServers;
    unsigned int mqPort;
	string mqVHost;
	string mqUser;
	string mqPassword;
    string mqExchangeName;
    unsigned int mqFrameSize;
#endif

    ProcmonConfig() {
        gid_range_min = -1;
        gid_range_max = -1;
        /* Initialize defaults */
        targetPPid = 1;
        frequency = DEFAULT_FREQUENCY;
        initialFrequency = DEFAULT_INITIAL_PHASE_FREQUENCY;
        initialPhase = DEFAULT_INITIAL_PHASE;
        clockTicksPerSec = 0;
        pageSize = 0;
        daemonize = false;
        verbose = false;
        craylock = false;
        outputFlags = DEFAULT_OUTPUT_FLAGS;
        tgtGid = 0;
        maxfd = 0;
        tgtSid = 0;
        tgtPgid = 0;
        maxIterations = 0;
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
        noOutput = false;

        /* Setup Context-derived values */
        char buffer[BUFFER_SIZE];
        bzero(buffer, BUFFER_SIZE);
        if (gethostname(buffer, BUFFER_SIZE) == 0) {
            hostname = buffer;
        }

        clockTicksPerSec = sysconf(_SC_CLK_TCK);
        pageSize = sysconf(_SC_PAGESIZE);
        boottime = getBootTime();

        setupDefaultProcmonOptions();
    }

    ~ProcmonConfig() {
    }

    void setupDefaultProcmonOptions() {
        po::options_description basic("Basic Options");
        basic.add_options()
            ("version", "Print version information")
            ("help,h", "Print help message")
            ("verbose,v", "Print extra information (syslog mirrored to stderr)")
            ("daemonize,d", "Daemonize the procmon process")
            ("craylock,c", "Create and lock /tmp/procmon; exit if no lock")
            ("config.file", po::value<string>(&userConfigFile)
                ->default_value(""), "Configuration file from which to read "
                "options.")
            ("syslog.facility", po::value<string>()
                ->notifier(boost::bind(&ProcmonConfig::setSyslogFacility, this, _1))
                ->default_value("USER"), "Name of syslog facility to use for"
                " info/error output. Supports: DAEMON, USER, LOCAL0 - LOCAL7")
            ("syslog.level.min", po::value<string>()
                ->notifier(boost::bind(&ProcmonConfig::setSyslogPriorityMin, this, _1))
                ->default_value("NOTICE"), "Minimum level of log data to send."
                " Supports: EMERG, ALERT, CRIT, ERR, WARNING, NOTICE, INFO, "
                " DEBUG" )
        ;
        procmonOptions.add(basic);

        po::options_description configOptions("Configuration Options");
        configOptions.add_options()
            ("frequency,f", po::value<int>(&frequency)
                ->default_value(DEFAULT_FREQUENCY), "Time elapsed between "
                "measurements during normal data collection (in seconds)")
            ("initialphase,i", po::value<int>(&initialPhase)
                ->default_value(DEFAULT_INITIAL_PHASE_FREQUENCY), "Length of the "
                "initial phase (in seconds)")
            ("initialfrequency,F", po::value<int>(&initialFrequency)
                ->default_value(DEFAULT_INITIAL_PHASE_FREQUENCY), "Time elapsed "
                "between measurements during initial phase (in seconds)")
            ("nooutput,n", "prevent any type of output from being generated "
                "(for testing)")
            ("outputtext,o", po::value<string>(&outputTextFilename)->default_value(""),
                "filename for text output (optional)")
#ifdef USE_HDF5
            ("outputhdf5,O", po::value<string>(&outputhdf5)->default_value(""),
                "filename for HDF5 output (optional)")
#endif
            ("pid,q", po::value<string>(&pidfile)->default_value(""), "filename for "
                "optional pid file")
            ("debug.maxiterations", po::value<int>(&maxIterations)
                ->default_value(0), "Debugging: max iterations to complete")
#ifdef SECURED
            ("user,u", po::value<string>(&user)->default_value(""), "username/uid to "
                "setuid")
            ("group,r", po::value<string>(&group)->default_value(""), "group/gid to "
                "setgid")
#endif
        ;
        procmonOptions.add(configOptions);

        po::options_description contextOptions("Monitoring Context Options");
        contextOptions.add_options()
            ("system", po::value<string>(&system)->default_value(DEFAULT_SYSTEM),
                "system tag")
            ("identifier,I", po::value<string>(&identifier)
                ->default_value(DEFAULT_IDENTIFIER), "identifier for tagging data")
            ("subidentifier,S", po::value<string>(&subidentifier)
                ->default_value(DEFAULT_SUBIDENTIFIER), "secondary identifier for "
                "tagging")
            ("no_system", "Do not use the system tag in the context identifier"
                " string")
        ;
        procmonOptions.add(contextOptions);

        po::options_description processOptions("Process Monitoring Options");
        processOptions.add_options()
            ("ppid,p", po::value<pid_t>(&targetPPid)->default_value(1), "Parent process ID "
                "of monitoring hierarchy")
            ("group_min,g", po::value<int>(&gid_range_min), "Minimum "
                "group id in GridEngine gid range (second group process ident)")
            ("group_max,G", po::value<int>(&gid_range_max), "Maximum "
                "group id in GridEngine gid range (second group process ident)")
            ("sid,s", po::value<int>(&tgtSid)->implicit_value(getsid(0)),
                "Track any processes (or children) matching the specified"
                "session id. Use self with just -s")
            ("pgrp,l", po::value<int>(&tgtPgid)
                ->implicit_value(getpgrp()), "Track any processes (or children)"
                " matching the specied pgrp id. Use self with just -l")
            ("fd_max,W", po::value<int>(&maxfd)->default_value(0), "Maximum"
                " number of fds per process to track")
            ("identifier_env,x", po::value<string>(&identifier_env)
                ->default_value(""), "Read identifier from process environment with "
                "specified environment value")
            ("subidentifier_env,X", po::value<string>(&subidentifier_env)
                ->default_value(""), "Read subidentifier from process environment "
                "with specified environment value")
        ;
        procmonOptions.add(processOptions);

#ifdef USE_AMQP
        po::options_description amqpConfig("AMQP Configuration Options");
        amqpConfig.add_options()
            ("mqhostname,H", po::value<string>(&mqServer)
                ->default_value(DEFAULT_AMQP_HOST), "hostname for AMQP Server")
            ("mqport,P", po::value<unsigned int>(&mqPort)
                ->default_value(DEFAULT_AMQP_PORT), "port for AMQP Server")
            ("mqvhost,Q", po::value<string>(&mqVHost)
                ->default_value(DEFAULT_AMQP_VHOST), "virtual-host for AMQP"
                " Server")
            ("mqexchange,E", po::value<string>(&mqExchangeName)
                ->default_value(DEFAULT_AMQP_EXCHANGE_NAME), "exchange name for"
                " AMQP Server")
            ("mquser,U", po::value<string>(&mqUser), "username for AMQP "
                "Server (default built-in)")
            ("mqpassword,Y", po::value<string>(&mqPassword), "password for "
                "AMQP Server (default built-in)")
            ("mqframe,R", po::value<unsigned int>(&mqFrameSize)
                ->default_value(DEFAULT_AMQP_FRAMESIZE), "maximum frame size "
                "for AMQP Messages (bytes)")
        ;
        procmonOptions.add(amqpConfig);
#endif
    }

    void parseOptions(int argc, char **argv) {
        /* try to read config file first */
        string baseConfigFile = string(SYSTEM_CONFIG_DIR) + "/procmon.conf";
        char *configEnv = NULL;
        if ((configEnv = getenv("PROCMON_DIR")) != NULL) {
            baseConfigFile = configEnv;
        }

        /* read command line options first */
        po::variables_map vm;
        try {
            po::store(po::command_line_parser(argc, argv).options(procmonOptions).run(), vm);

            if (vm.count("config.file") > 0) {
                userConfigFile = vm["config.file"].as<string>();
                if (userConfigFile.length() > 0 && fs::exists(userConfigFile)) {
                    ifstream ifs(userConfigFile.c_str());
                    if (!ifs) {
                        invalid_argument e(string("Config file doesn't exist: ") + userConfigFile);
                        throw &e;
                    }
                    po::store(po::parse_config_file(ifs, procmonOptions), vm);
                    ifs.close();
                }
            }
            if (fs::exists(baseConfigFile)) {
                ifstream input(baseConfigFile.c_str());
                if (!input) {
                    invalid_argument e("Base config file not readable: " + baseConfigFile);
                    throw &e;
                }
                po::store(po::parse_config_file(input, procmonOptions), vm);
                input.close();
            }
        } catch (exception &e) {
            cout << e.what() << endl;
            cout << procmonOptions << endl;
            exit(1);
        }
        po::notify(vm);
        if (vm.count("help")) {
            cout << procmonOptions << endl;
            exit(0);
        }
        if (vm.count("version")) {
            version();
            exit(0);
        }

        craylock = vm.count("craylock") != 0;
        daemonize = vm.count("daemonize") != 0;
        verbose = vm.count("verbose") != 0;
        noOutput = vm.count("nooutput") != 0;

        if (frequency <= 0) {
            cerr << "Frequency must be at least 1 second!" << endl;
            cerr << procmonOptions << endl;
            exit(1);
        }
        if (initialPhase <= 0) {
            cerr << "Initial-phase, if specified, must be at least 1 second!" << endl;
            cerr << procmonOptions << endl;
            exit(1);
        }
        if (initialFrequency <= 0) {
            cerr << "Initial-frequency, if specified, must be at least 1 second!" << endl;
            cerr << procmonOptions << endl;
            exit(1);
        }

#ifdef SECURED
        if (vm.count("user")) {
            struct passwd *user_data = getpwnam(user.c_str());
            if (user_data == NULL) {
                int uid = atoi(user.c_str());
                if (uid > 0) {
                    user_data = getpwuid(uid);
                    if (user_data != NULL) {
                        target_uid = user_data->pw_uid;
                    }
                }
            } else {
                target_uid = user_data->pw_uid;
            }
            if (target_uid <= 0) {
                cerr << "user, if specified, must resolve to a uid > 0" << endl;
                cerr << procmonOptions << endl;
                exit(1);
            }
        }
        if (vm.count("group")) {
            struct group *group_data = getgrnam(group.c_str());
            if (group_data == NULL) {
                int gid = atoi(group.c_str());
                group_data = getgrgid(gid);
                if (group_data != NULL) {
                    target_gid = group_data->gr_gid;
                }
            } else {
                target_gid = group_data->gr_gid;
            }
        }
#endif

        if (targetPPid <= 0) {
            cerr << "ppid must be at least 1 (should be 1 to track all user-space processes)" << endl;
            cerr << procmonOptions << endl;
            exit(1);
        }
        if (vm.count("fd_max")) {
            if (maxfd < 0) {
                cerr << "fd_max, if specified, must be at least 0!" << endl;
                cerr << procmonOptions << endl;
                exit(1);
            }
        }
        if (vm.count("group_min")) {
            if (gid_range_min <= 0) {
                cerr << "gid range minimum, if specified, must be at least 1!" << endl;
                cerr << procmonOptions << endl;
                exit(1);
            }
        }
        if (vm.count("group_max")) {
            if (gid_range_max <= 0) {
                cerr << "gid range maximum, if specified, must be at least 1!" << endl;
                cerr << procmonOptions << endl;
                exit(1);
            }
        }
        if (vm.count("sid")) {
            if (tgtSid <= 0) {
                cerr << "specified sid must be at least 1!" << endl;
                cerr << procmonOptions << endl;
                exit(1);
            }
        }
        if (vm.count("pgrp")) {
            if (tgtPgid <= 0) {
                cerr << "specified pgrp id must be at least 1!" << endl;
                cerr << procmonOptions << endl;
                exit(1);
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
#ifdef USE_AMQP
        char *servers = strdup(mqServer.c_str());
        char *token = strtok(servers, ",");
        srand(getpid() | time(NULL));
        while (token != NULL) {
            mqServers.push_back(string(token));
            token = strtok(NULL, ",");
        }
        free(servers);
        mqServer = mqServers[rand() % mqServers.size()];
        if (mqServer != "" && mqServer != "__NONE__") {
            outputFlags |= OUTPUT_TYPE_AMQP;
        }
#endif
        if (noOutput) {
            outputFlags = OUTPUT_TYPE_NONE;
        }
        if (outputFlags == 0) {
            cerr << "No output mechanism specified (text, hdf5, or AMQP)" << endl;
            cerr << procmonOptions << endl;
            exit(1);
        }
        
        // deal with finding secondary gid mapping if applicable
        if (gid_range_min > 0 && gid_range_max > 0) {
            if (gid_range_min > gid_range_max) {
                int temp = gid_range_min;
                gid_range_min = gid_range_max;
                gid_range_max = temp;
            }
            gid_t processGids[512];
            int foundGroups = getgroups(512, processGids);
            for (int i = 0; i < foundGroups; i++) {
                if (processGids[i] >= gid_range_min && processGids[i] <= gid_range_max) {
                    tgtGid = processGids[i];
                    break;
                }
            }
        }
    }

    inline const int getMaxIterations() const {
        return maxIterations;
    }

    const string getContext();

    void version() {
        cout << "Procmon " << PROCMON_VERSION;
#ifdef SECURED
        cout << " (interactive)";
#endif
        cout << endl;
        exit(0);
    }

    friend ostream& operator<<(ostream&, ProcmonConfig&);
};



ostream& operator<<(ostream& os, const ProcmonConfig& pc);

#endif
