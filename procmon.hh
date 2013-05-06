#ifndef __PROCMON_CONFIG_HH_
#define __PROCMON_CONFIG_HH_

#include "ProcData.hh"

#include <boost/program_options.hpp>
namespace po = boost::program_options;

#include "config.h"

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
	int debug;
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
        std::vector<int> gidRange;

        /* Initialize defaults */
        targetPPid = 1;
        frequency = DEFAULT_FREQUENCY;
        initialFrequency = DEFAULT_INITIAL_PHASE_FREQUENCY;
        initialPhase = DEFAULT_INITIAL_PHASE;
        clockTicksPerSec = 0;
        pageSize = 0;
        daemonize = false;
		debug = 0;
        outputFlags = DEFAULT_OUTPUT_FLAGS;
        tgtGid = 0;

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
		po::options_description basic("Basic Options");
		basic.add_options()
			("version", "Print version information")
			("help,h", "Print help message")
			("verbose,v", "Print extra (debugging) information")
		;
		po::options_description config("Configuration Options");
		config.add_options()
            ("daemonize,d", "Daemonize the procmon process")
            ("frequency,f", po::value<int>(&(this->frequency))->default_value(DEFAULT_FREQUENCY), "Time elapsed between measurements during normal data collection (in seconds)")
            ("initialphase,i", po::value<int>(&(this->initialPhase))->default_value(DEFAULT_INITIAL_PHASE), "Length of the initial phase (in seconds)")
            ("initialfrequency,F", po::value<int>(&(this->initialFrequency))->default_value(DEFAULT_INITIAL_PHASE_FREQUENCY), "Time elapsed between measurements during initial phase (in seconds)")
            ("ppid,p",po::value<int>(&(this->targetPPid))->default_value(1), "parent process id of monitoring hierarchy")
            ("group,g",po::value<std::vector<int> >(&gidRange)->multitoken(), "min and max group ids to search for secondary group process identification (GridEngine integration)")
            ("identifier,I",po::value<std::string>(&(this->identifier))->default_value(DEFAULT_IDENTIFIER), "identifier for tagging data")
            ("subidentifier,S",po::value<std::string>(&(this->subidentifier))->default_value(DEFAULT_SUBIDENTIFIER), "secondary identifier for tagging data")
            ("outputtext,o",po::value<std::string>(&(this->outputTextFilename)), "filename for text output (optional)")
#ifdef __USE_HDF5
            ("outputhdf5,O",po::value<std::string>(&(this->outputHDF5Filename)), "filename for hdf5 output (optional)")
#endif
			("debug", po::value<int>(&(this->debug)), "enter debugging mode")
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

            if (outputTextFilename != "") {
                outputFlags |= OUTPUT_TYPE_TEXT;
            }
#ifdef __USE_HDF5
            if (outputHDF5Filename != "") {
                outputFlags |= OUTPUT_TYPE_HDF5;
            }
#endif
            if (mqServer != "") {
                outputFlags |= OUTPUT_TYPE_AMQP;
            }

            if (vm.count("group") > 0 && (gidRange.size() != 2 || (gidRange[0] == 0 && gidRange[1] == 0) || gidRange[0] > gidRange[1])) {
                throw ProcmonException("group id range must have exactly two integral arguments of the min gid and max gid inclusive (ordered)");
            }

            if (outputFlags == 0) {
                throw ProcmonException("No output mechanism specified (text, hdf5, or AMQP)");
            }
            
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

        /* deal with finding secondary gid mapping if applicable */
        if (gidRange.size() == 2) {
            procstat self;
            int processGids[64];
            int foundGroups = parseProcStatus(getpid(),-1,&self,processGids,64);
            for (int i = 0; i < foundGroups; i++) {
                if (processGids[i] >= gidRange[0] && processGids[i] <= gidRange[1]) {
                    tgtGid = processGids[i];
                    break;
                }
            }
        }
	}
};


#endif
