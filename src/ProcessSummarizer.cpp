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
#include <pthread.h>

#include <boost/program_options.hpp>
#define PROCMON_SUMMARIZE_VERSION 2.0
namespace po = boost::program_options;
using namespace std;

class AnalysisTask;

struct H5FileControl {
    ProcHDF5IO *file;
    pthread_mutex_t mutex;
};




struct HostCountData {
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


pthread_mutex_t workQueue_rw_mutex;
deque<AnalysisTask *> workQueue;

void *workerThread(void *args) {
    while (!cleanUp) {
        pthread_mutex_lock(&workQueue_rw_mutex);
        AnalysisTask *task = *(workQueue.begin());
        workQueue.pop_front();
        pthread_mutex_unlock(&workQueue_rw_mutex);
        task->runTask();
        task->signal();
    }
}

/* ProcReducer
    * Takes in procmon data from all sources and only writes the minimal record
    * Writes an HDF5 file for all procdata foreach day
*/

void version() {
    cout << "ProcmonSummarize " << PROCMON_SUMMARIZE_VERSION << endl;
    exit(0);
}

class AnalysisTask {
    public:
    AnalysisTask(pthread_cond_t *_cond) {
        completed = false;
        status = false;
        cond = _cond;
    }
    virtual bool runTask() = 0;
    virtual const bool isComplete() const {
        return completed;
    }
    virtual const bool exitStatus() const {
        return status;
    }
    void signal() {
        if (cond != NULL) pthread_cond_signal(cond);
    }

    protected:
    bool completed;
    bool status;
};

class ReadH5Metadata : public AnalysisTask {
    public:
    ReadH5Metadata(pthread_cond_t *_cond, ProcHDF5IO *_inputFile, vector<HostCountData> **_hostCounts):
            inputFile(_h5file),
            hostCounts(_hostCounts),
            AnalysisTask(_cond)
    {
    }

    virtual bool runTask() {
        if (completed) return status;
        vector<string> hosts;

        inputFile->get_hosts(hosts);
        *hostCounts = new vector<HostCountData>(hosts.size());
        vector<HostCountData>& l_hostCounts = **hostCounts;

        int idx = 0;
        for (auto it: hosts) {
            inputFile->set_context(it, "any", "any");

            int len = it.length();
            l_hostCounts[idx].setHostname(it.c_str());
            l_hostCounts[idx].n_procdata = inputFile->get_nprocdata();
            l_hostCounts[idx].n_procstat = inputFile->get_nprocstat();
            l_hostCounts[idx].n_procfd   = inputFile->get_nprocfd();
            l_hostCounts[idx].n_procobs  = inputFile->get_nprocobs();

            idx++;
        }
        sort(l_hostCounts.begin(), l_hostCounts.end());
        status = true;
        completed = true;
        return status;
    }

    private:
    ProcHDF5IO *inputFile;
    vector<HostCountData> **hostCounts;
};

class ReadH5ProcessData : public AnalysisTask {
    public:
    ReadH5ProcessData(pthread_cond_t *_cond, ProcHDF5IO *_inputFile,
            pthread_mutex_t *_h5mutex, const char *_hostname, HostCountData &cnt,
            HostCountData &cumsum):
        inputFile(_inputFile),
        hostname(_hostname),
        h5mutex(_h5mutex),
        AnalysisTask(_cond)
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

    bool runTask() {
        if (completed) return status;

        pthread_mutex_lock(h5mutex);
        inputFile->set_context(hostname, "", "");

        procstat *ps_ptr = &(output->ps[ps_offset]);
        ps_read = inputFile->read_procstat(ps_ptr, 0, ps_count);

        procstat *pd_ptr = &(output->pd[pd_offset]);
        pd_read = inputFile->read_procdata(pd_ptr, 0, pd_count);

        procfd *fd_ptr = &(output->fd[fd_offset]);
        fd_read = inputFile->read_procfd(fd_ptr, 0, fd_count);

        procobs *obs_ptr = &(output->obs[obs_offset]);
        obs_read = inputFile->read_procobs(obs_ptr, 0, obs_count);

        pthread_mutex_unlock(h5mutex);

        status = true;
        completed = true; 
        return status;
    }

    private:
    ProcHDF5IO  *inputFile;
    pthread_mutex_t *h5mutex;
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
    pthread_mutex_t barlock;
    pthread_cond_t  barsig;

    pthread_mutex_init(&workQueue_rw_lock);
    pthread_mutex_init(&barlock, NULL);
    pthread_mutex_lock(&barlock);
    pthread_mutex_lock(&workQueue_rw_lock);

    /* start up worker threads */
    pthread_t tid;
    for (int i = 0; i < config.nThreads; ++i) {
        int ret = pthread_create(&tid, NULL, worker_start, &config);
        if (ret != 0) {
            cerr << "Failed to create worker thread, exiting." << endl;
            exit(1);
        }
    }

    /* open input h5 files, walk the metadata */
    vector<ProcHDF5IO *> inputFiles;
    vector<vector<HostCountData>* > inputHostCounts;
    vector<ReadH5Metadata*> readTasks;
    for (auto it: config.getProcmonH5Inputs()) {
        ProcHDF5IO *input = new ProcHDF5IO(it, FILE_MODE_READ);
        vector<HostCountData> *data = NULL;
        inputHostCounts.push_back(data);

        ReadH5Metadata *task = new ReadH5Metadata(&barlock, input, &data);
        readTasks.push_back(task);
        workQueue.push_back(task);
    }
    pthread_mutex_unlock(&workQueue_rw_lock);
    bool done = false;
    while (!done) {
        done = true
        pthread_cond_wait(&barsig); //prevent busy wait
        for (auto task: readTasks) {
            done &= task->isComplete(); //safe to look at isComplete because it 
                                        //is changed well before barsig is tripped
        }
    }
    globalHostCounts = mergeHostCounts(inputHostCounts);







    string hostname, identifier, subidentifier;

    int saveCnt = 0;
    int nRecords = 0;

    char buffer[1024];
    vector<string> hosts;
    inputFile->get_hosts(hosts);
    cout << "\"hosts\":{" << endl;
    for (auto ptr = hosts.begin(), end = hosts.end(); ptr != end; ++ptr) {
        hostname = *ptr;
        identifier = "any";
        subidentifier = "any";
        inputFile->set_context(hostname, identifier, subidentifier);
        int n_procdata = inputFile->get_nprocdata();
        int n_procstat = inputFile->get_nprocstat();
        int n_procfd = inputFile->get_nprocfd();
        int n_procobs = inputFile->get_nprocobs();

        cout << "\"" << hostname << "\": {" << endl;
        cout << "  \"nprocdata\":" << n_procdata << "," << endl;
        cout << "  \"nprocstat\":" << n_procstat << "," << endl;
        cout << "  \"nprocfd\":" << n_procfd << "," << endl;
        cout << "  \"nprocobs\":" << n_procobs << endl;
        cout << "}";
        if (ptr + 1 != hosts.end()) cout << ",";
        cout << endl;
    }
    cout << "}" << endl;
    cout << "}" << endl;

    delete inputFile;

    return 0;
}
