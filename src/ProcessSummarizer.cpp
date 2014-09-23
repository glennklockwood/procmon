#include "config.h"
#include "ProcData.hh"
#include "ProcIO.hh"
#include "ProcReducerData.hh"

#include <signal.h>
#include <string.h>
#include <iostream>
#include <deque>

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <tbb/tbb.h>
#include <tbb/task.h>
#include <tbb/task_scheduler_init.h>
#include <tbb/spin_mutex.h>

#include <boost/program_options.hpp>
#define PROCMON_SUMMARIZE_VERSION 2.0
namespace po = boost::program_options;
using namespace std;

class AnalysisTask;

struct H5FileControl {
    ProcHDF5IO *file;
    tbb::spin_mutex mutex;

    tbb::task *H5FileControl(ProcHDF5IO *_file):
        file(_file)
    {
    }
};

ProcHDF5IO *HostCountData {
    char hostname[256];
    size_t n_procdata;
    size_t n_procstat;
    size_t n_procfd;
    size_t n_procobs;

    HostCountData() {
        memset(hostname, 0, sizeof(char) * 256);
        n_procdata = 0;
        n_procstat = 0;
        n_procfd = 0;
        n_probobs = 0;
    }

    void setHostname(const char *_hostname) {
        strncpy(hostname, _hostname, 256);
        hostname[255] = 0;
    }

    HostCountData(const HostCountData& other) {
        setHostname(other.hostname);
        n_procdata = other.n_procdata;
        n_procstat = other.n_procstat;
        n_procfd   = other.n_procfd;
        n_procobs  = other.n_procobs;
    }

    bool operator<(const HostCountData& other) {
        if (strncmp(hostname, other.hostname, 256) < 0) return true;
        if (n_procdata < other.n_procdata) return true;
        if (n_procstat < other.n_procstat) return true;
        if (n_procfd < other.n_procfd) return true;
        if (n_procobs < other.n_procobs) return true;
    }

    void operator+=(const HostCountData &other) {
        n_procdata += other.n_procdata;
        n_procstat += other.n_procstat;
        n_procfd   += other.n_procfd;
        n_procobs  += other.n_procobs;
    }
};

struct ProcessData {
    vector<procstat> *ps;
    vector<procdata> *pd;
    vector<procfd>   *fd;
    vector<procobs>  *obs;

    ProcessData(HostCountData& cnt) {
        ps = new vector<procstat>(cnt.n_procstat);
        pd = new vector<procdata>(cnt.n_procdata);
        fd = new vector<procfd>(cnt.n_procfd);
        obs = new vector<procobs>(cnt.n_procobs);
    }
};


/* ProcReducer
    * Takes in procmon data from all sources and only writes the minimal record
    * Writes an HDF5 file for all procdata foreach day
*/

void version() {
    cout << "ProcmonSummarize " << PROCMON_SUMMARIZE_VERSION << endl;
    exit(0);
}

class ReadH5Metadata : public tbb::task {
    public:
    ReadH5Metadata(H5FileControl *_inputFile, vector<HostCountData> **_hostCounts):
            input(_inputFile),
            hostCounts(_hostCounts)
    {
    }

    tbb::task *execute() {
        input->mutex->lock();
        vector<string> hosts;

        input->file->get_hosts(hosts);
        *hostCounts = new vector<HostCountData>(hosts.size());
        vector<HostCountData>& l_hostCounts = **hostCounts;

        int idx = 0;
        for (auto it: hosts) {
            input->file->set_context(it, "", "");

            int len = it.length();
            l_hostCounts[idx].setHostname(it.c_str());
            l_hostCounts[idx].n_procdata = input->file->get_nprocdata();
            l_hostCounts[idx].n_procstat = input->file->get_nprocstat();
            l_hostCounts[idx].n_procfd   = input->file->get_nprocfd();
            l_hostCounts[idx].n_procobs  = input->file->get_nprocobs();

            idx++;
        }
        tbb::parallel_sort(l_hostCounts.begin(), l_hostCounts.end());
        input->mutex->unlock();
        return NULL;
    }

    private:
    H5ControlFile *input;
    vector<HostCountData> **hostCounts;
};

class ReadH5ProcessData : public tbb::task {
    public:
    ReadH5ProcessData(H5ControlFile *_input, const char *_hostname,
            HostCountData &cnt, HostCountData &cumsum):
        input(_input),
        hostname(_hostname)
    {
        ps_read = pd_read = fd_read = obs_read = 0;
        ps_count = cnt.n_procstat;
        pd_count = cnt.n_procdata;
        fd_count = cnt.n_procfd;
        obs_count = cnt.n_procobs;
        ps_offset = cumsum.n_procstat;
        pd_offset = cumsum.n_procdata;
        fd_offset = cumsum.n_procfd;
        obs_offset = cumsum.n_procobs;
    }

    tbb::task *execute() {
        input->mutex->lock();
        input->file->set_context(hostname, "", "");

        procstat *ps_ptr = &(output->ps[ps_offset]);
        ps_read = input->file->read_procstat(ps_ptr, 0, ps_count);

        procstat *pd_ptr = &(output->pd[pd_offset]);
        pd_read = input->file->read_procdata(pd_ptr, 0, pd_count);

        procfd *fd_ptr = &(output->fd[fd_offset]);
        fd_read = input->file->read_procfd(fd_ptr, 0, fd_count);

        procobs *obs_ptr = &(output->obs[obs_offset]);
        obs_read = input->file->read_procobs(obs_ptr, 0, obs_count);

        input->mutex->unlock();
        return NULL;
    }

    private:
    H5ControlFile  *input;
    const char *hostname;

    ProcessData *output;
    size_t ps_read, pd_read, fd_read, obs_read;
    size_t ps_count, pd_count, fd_count, obs_count;
    size_t ps_offset, pd_offset, fd_offset, obs_offset;
};

class ProcmonSummarizeConfig {
public:
    vector<string> procmonh5_files;
    unsigned int nThreads;

    ProcmonSummarizeConfig(int argc, char **argv) {
        /* Parse command line arguments */
        po::options_description basic("Basic Options");
        basic.add_options()
            ("version", "Print version information")
            ("help,h", "Print help message")
            ("verbose,v", "Print extra (debugging) information")
        ;
        po::options_description config("Configuration Options");
        config.add_options()
            ("input,i",po::value<vector<string> >(&procmonh5_files)->composing(), "input filename(s) (required)")
            ("threads,t", po::value<unsigned int>(&nThreads), "number of worker threads to use (one additional I/O and controller thread will also run)")
        ;

        po::options_description options;
        options.add(basic).add(config);
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
            if (vm.count("debug")) {
                debug = true;
            }
        } catch (std::exception &e) {
            std::cout << e.what() << std::endl;
            std::cout << options << std::endl;
            exit(1);
        }

        if (nThreads < 1) {
            cerr << "Need at least one (1) worker thread!" << endl;
            usage(options, 1);
        }
    }
    bool debug;

    string input_filename;
};

vector<HostCountData> *mergeHostCounts(vector<vector<HostCountData> *>& counts)
{
    unordered_map<string, HostCountData *> mergeMap;
    vector<HostCountData> *ret = NULL;
    for (auto it: counts) {
        for (auto host: it) {
            auto loc = mergeMap.find(host.hostname);
            HostCountData *tgt = NULL;
            if (loc == mergeMap.end()) {
                tgt = new HostCountData(host);
                mergeMap[host.hostname] = tgt;
            } else {
                *tgt += host;
            }
        }
    }
    if (mergeMap.size() == 0) return NULL;
    ret = new vector<HostCountData>(mergeMap.size());
    size_t idx = 0;
    for (auto it: mergeMap) {
        (*it)[idx++] = *(it.second);
    }
    sort(ret->begin(), ret->end());
    return ret;
}

int main(int argc, char **argv) {
    ProcmonSummarizeConfig config(argc, argv);

    tbb::task_scheduler_init init(config.threads != 0 ? config.threads : tbb::task_scheduler_init::automatic);

    /* open input h5 files, walk the metadata */
    vector<ProcHDF5IO *> inputFiles;
    vector<vector<HostCountData>* > inputHostCounts;
    tbb::task_list metadataTasks;
    for (auto it: config.getProcmonH5Inputs()) {
        H5FileControl *input = new H5FileControl(new ProcHDF5IO(it, FILE_MODE_READ));
        vector<HostCountData> *data = NULL;
        inputHostCounts.push_back(data);

        metdataTasks.push_back(*new(tbb::task::allocate_root()) ReadH5Metadata(input, &data));
    }
    tbb::task::spawn_root_and_wait(list);
    globalHostCounts = mergeHostCounts(inputHostCounts);

    return 0;
}
