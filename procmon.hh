#ifndef __PROCMON_CONFIG_HH_
#define __PROCMON_CONFIG_HH_

#include <boost/program_options.hpp>
namespace po = boost::program_options;

#define DEFAULT_FREQUENCY 60
#define DEFAULT_INITFREQ 1
#define DEFAULT_AMQP_FRAMESIZE 131072

#define OUTPUT_TYPE_TEXT 0x1
#define OUTPUT_TYPE_HDF5 0x2
#define OUTPUT_TYPE_MQ   0x4

#define BUFFER_SIZE 1024
#define LBUFFER_SIZE 8192
#define EXEBUFFER_SIZE 256

typedef struct _procdata {
    char execName[EXEBUFFER_SIZE];
    unsigned long cmdArgBytes;
    char cmdArgs[BUFFER_SIZE];
    char exePath[BUFFER_SIZE];
    char cwdPath[BUFFER_SIZE];
    unsigned long recTime;
    unsigned long recTimeUSec;
    unsigned long startTime;
    unsigned long startTimeUSec;
    unsigned int pid;
    unsigned int ppid;
    unsigned int nextRec;
    unsigned int prevRec;
} procdata;

typedef struct _procstat {
    unsigned int pid;
    unsigned int nextRec;
    unsigned int prevRec;
    unsigned long recTime;
    unsigned long recTimeUSec;
    unsigned long startTime;
    unsigned long startTimeUSec;
    char state;
    unsigned int ppid;
    int pgrp;
    int session;
    int tty;
    int tpgid;
    unsigned long realUid;
    unsigned long effUid;
    unsigned long realGid;
    unsigned long effGid;
    unsigned int flags;
    unsigned long utime;
    unsigned long stime;
    long priority;
    long nice;
    long numThreads;
    unsigned long vsize; /* virtual mem in bytes */
    unsigned long rss;   /* number of pages in physical memory */
    unsigned long rsslim;/* limit of rss bytes */
    unsigned long signal;
    unsigned long blocked;
    unsigned long sigignore;
    unsigned long sigcatch;
    unsigned int rtPriority;
    unsigned int policy;
    unsigned long long delayacctBlkIOTicks;
    unsigned long guestTime;

    /* fields from /proc/[pid]/status */
    unsigned long vmpeak;  /* kB */
    unsigned long rsspeak; /* kB */
    int cpusAllowed;

    /* fields from /proc/[pid]/io */
    unsigned long long io_rchar;
    unsigned long long io_wchar;
    unsigned long long io_syscr;
    unsigned long long io_syscw;
    unsigned long long io_readBytes;
    unsigned long long io_writeBytes;
    unsigned long long io_cancelledWriteBytes;

    /* fields from /proc/[pid]/statm */
    unsigned long m_size;
    unsigned long m_resident;
    unsigned long m_share;
    unsigned long m_text;
    unsigned long m_data;
} procstat;

class ProcmonConfig {
public:
    /* Configurable monitoring options */
    int targetPPid;
    int frequency;
    int initialFrequency;
    int initialPhase;
    bool daemonize;
    std::string identifier;
    std::string subidentifier;

    /* Derived monitoring inputs */
    int tgtGid;
    long clockTicksPerSec = 0;
    long pageSize = 0;
    time_t boottime;
    std::string hostname;

    /* Output Options */
    unsigned int outputFlags;
    std::string outputTextFilename;
    std::string outputHDF5Filename;

    /* AMQP options */
    std::string mqServer;
    unsigned int mqPort;
    std::string mqDomain;
    std::string mqExchangeName;
    unsigned int mqFrameSize;

    ProcmonConfig(int argc, char** argv) {
        std::vector<int> gidRange;

        /* Initialize defaults */
        targetPPid = 1;
        frequency = DEFAULT_FREQUENCY;
        initialFrequency = DEFAULT_INITFREQ;
        initialPhase = 0;
        O
        clockTicksPerSec = 0;
        pageSize = 0;
        daemonize = false;
        outputFlags = 0;
        tgtGid = 0;

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
            ("daemonize,d", po::value<bool>(&(this->daemonize))->default_value(false), "Daemonize the procmon process")
            ("frequency,f", po::value<int>(&(this->frequency))->default_value(DEFAULT_FREQUENCY), "Time elapsed between measurements during normal data collection (in seconds)")
            ("initialphase,i", po::value<int>(&(this->initialPhase))->default_value(0), "Length of the initial phase (in seconds)")
            ("initialfrequency,if", po::value<int>(&(this->initialFrequency))->default_value(DEFAULT_INITFREQ), "Time elapsed between measurements during initial phase (in seconds)")
            ("ppid,p",po::value<int>(&(this->targetPPid))->default_value(1), "parent process id of monitoring hierarchy")
            ("group,g",po::value<std::vector<int> >(&gidRange)->multitoken(), "min and max group ids to search for secondary group process identification (GridEngine integration)")
            ("identifier,I",po::value<std::string>(&(this->identifier))->default_value("proc"), "identifier for tagging data")
            ("subidentifier,S",po::value<std::string>(&(this->subidentifier))->default_value("mon"), "secondary identifier for tagging data")
            ("outputtext","o",po::value<std::string>(&(this->outputTextFilename)), "filename for text output (optional)")
            ("outputhdf5","O",po::value<std::string>(&(this->outputHDF5Filename)), "filename for hdf5 output (optional)")
        ;
        po::options_description mqconfig("AMQP Configuration Options");
        mqconfig.add_options()
            ("mqhostname,H", po::value<std::string>(&(this->mqServer)), "hostname for AMQP Server")
            ("mqport,P",po::value<int>(&(this->mqPort)), "port for AMQP Server")
            ("mqvhost,V",po::value<std::string>(&(this->mqVHost)), "virtual-host for AMQP Server")
            ("mqframe,F",po::value<int>(&(this->mqFrameSize))->default_value(DEFAULT_AMQP_FRAMESIZE), "maximum frame size for AMQP Messages (bytes)")
        ;

		po::options_description options;
		options.add(basic).add(config).add(mqconfig);
	
		po::variables_map vm;
		try {
			po::store(po::command_line_parser(argc, argv).options(options).run(), vm);
			po::notify(vm);

            if (vm.count("outputtext") > 0) {
                outputFlags |= OUTPUT_TYPE_TEXT;
            }
            if (vm.count("outputhdf5") > 0) {
                outputFlags |= OUTPUT_TYPE_HDF5;
            }
            if (vm.count("mqhostname") > 0) {
                outputFlags |= OUTPUT_TYPE_MQ;
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
		} catch (std::exception &e) {
            std::cout << e.what() << std::endl;
            std::cout << options << std::endl;
            exit(1);
        }

        /* deal with finding secondary gid mapping if applicable */
        if (groups.size() == 2) {
            procstat self;
            int processGids[64];
            int foundGroups = parseProcStatus(getpid(),-1,&self,processGids,64);
            for (int i = 0; i < foundGroups; i++) {
                if (processGids[i] >= groups[0] && processGids[i] <= groups[1]) {
                    tgtGid = processGids[i];
                    break;
                }
            }
        }
	}
};

class ProcmonException : public std::exception {
public:
    ProcFileException(const char* t_error) {
        error = t_error;
    }

    virtual const char* what() const throw() {
        return error;
    }
private:
    const char *error;
};

#endif
