#include "config.h"
#include "ProcData.hh"
#include "ProcIO.hh"
#include "ProcReducerData.hh"
#include "ProcessSummary.hh"

#include <algorithm>
#include <signal.h>
#include <string.h>
#include <iostream>
#include <deque>
#include <unordered_map>

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <tbb/tbb.h>
#include <tbb/task.h>
#include <tbb/task_scheduler_init.h>
#include <tbb/cache_aligned_allocator.h>
#include <tbb/blocked_range.h>
#include <tbb/spin_mutex.h>
#include <tbb/concurrent_vector.h>
#include <tbb/concurrent_unordered_map.h>

#include <boost/program_options.hpp>
#define PROCMON_SUMMARIZE_VERSION 2.0
namespace po = boost::program_options;
using namespace std;

class AnalysisTask;

struct H5FileControl {
    ProcHDF5IO *file;
    tbb::spin_mutex mutex;

    H5FileControl(ProcHDF5IO *_file):
        file(_file)
    {
    }

    ~H5FileControl() {
        delete file;
    }

};

struct HostCountData {
    string hostname;
    size_t n_procdata;
    size_t n_procstat;
    size_t n_procfd;
    size_t n_procobs;
    bool set;

    HostCountData() {
        hostname = "";
        n_procdata = 0;
        n_procstat = 0;
        n_procfd = 0;
        n_procobs = 0;
        set = false;
    }

    void setHostname(const char *_hostname) {
        hostname = _hostname;
        set = true;
    }

    HostCountData(const HostCountData& other) {
        setHostname(other.hostname.c_str());
        n_procdata = other.n_procdata;
        n_procstat = other.n_procstat;
        n_procfd   = other.n_procfd;
        n_procobs  = other.n_procobs;
    }

    const bool operator<(const HostCountData& other) const {
        return hostname < other.hostname;
    }

    void operator+=(const HostCountData &other) {
        n_procdata += other.n_procdata;
        n_procstat += other.n_procstat;
        n_procfd   += other.n_procfd;
        n_procobs  += other.n_procobs;
    }
};

bool HostCountDataPtrCmp(const HostCountData *a, const HostCountData *b) {
    printf("Compare: %lu, %lu\n", a, b);
    if (a == NULL && b != NULL) { return true; }
    if (b == NULL) return false;
    return *a < *b;
}

/* procmon data structure comparison routines for sort() */
template <typename pmType>
bool less_byprocess(const pmType& a, const pmType& b) {
    int cmp = strncmp(a.identifier, b.identifier, IDENTIFIER_SIZE);
    if (cmp < 0) return true;
    if (cmp > 0) return false;

    cmp = strncmp(a.subidentifier, b.subidentifier, IDENTIFIER_SIZE);
    if (cmp < 0) return true;
    if (cmp > 0) return false;

    if (a.startTime < b.startTime) return true;
    if (a.startTime > b.startTime) return false;

    if (a.pid < b.pid) return true;
    if (a.pid > b.pid) return false;

    if (a.recTime < b.recTime) return true;
    return false;
}

template <>
bool less_byprocess<procfd>(const procfd& a, const procfd& b) {
    int cmp = strncmp(a.identifier, b.identifier, IDENTIFIER_SIZE);
    if (cmp < 0) return true;
    if (cmp > 0) return false;

    cmp = strncmp(a.subidentifier, b.subidentifier, IDENTIFIER_SIZE);
    if (cmp < 0) return true;
    if (cmp > 0) return false;

    if (a.startTime < b.startTime) return true;
    if (a.startTime > b.startTime) return false;

    if (a.pid < b.pid) return true;
    if (a.pid > b.pid) return false;

    ssize_t pathVal = strncmp(a.path, b.path, BUFFER_SIZE);
    if (pathVal < 0) return true;
    if (pathVal > 0) return false;

    if (a.recTime < b.recTime) return true;
    return false;
}

template<typename pmType>
inline bool equiv_byprocess(const pmType& a, const pmType& b) {
    return a.startTime == b.startTime & a.pid == b.pid;
}

template<typename pmType>
class ProcessMasker {
    bool *mask;
    pmType *start_ptr;
    size_t nelem;

    vector<pair<pmType*, pmType*> > processes;
    bool setprocs;
    bool ownmask;
    size_t count;

    public:
    ProcessMasker(pmType *_start_ptr, size_t _nelem):
        start_ptr(_start_ptr), nelem(_nelem)
    {
        mask = new bool[nelem];
        memset(mask, 0, sizeof(mask));
        setprocs = false;
        ownmask = true;
        count = 0;
    }

    ProcessMasker(ProcessMasker<pmType>& other, tbb::split) {
        start_ptr = other.start_ptr;
        nelem = other.nelem;
        mask = other.mask;
        ownmask = false;
        count = other.count;
    }

    ~ProcessMasker() {
        if (ownmask) delete mask;
    }

    void join(ProcessMasker<pmType>& other) {
        count += other.count;
    }

    void operator()(const tbb::blocked_range<pmType*> &r) {
        count += r.end() - r.begin();

        for (pmType *ptr = r.begin(); ptr != r.end(); ++ptr) {
            size_t idx = ptr - start_ptr;
            mask[idx] = (idx == 0) | equiv_byprocess<pmType>(*(ptr-1), *ptr);
        }
    }

    const vector<pair<pmType*,pmType*> >& getProcessBoundaries() {
        if (setprocs) return processes;
        pmType *start = NULL;
        pmType *end = NULL;
        for (size_t idx = 0; idx < nelem; ++idx) {
            if (mask[idx]) {
                if (idx > 0) {
                    end = &(start_ptr[idx]);
                    processes.push_back(pair<pmType *, pmType *>(start, end));
                }
                start = &(start_ptr[idx]);
            }
        }
        if (start == NULL) {
            start = start_ptr;
        }
        end = &(start_ptr[nelem]);
        processes.push_back(pair<pmType *, pmType *>(start, end));
        return processes;
    }
};

template <class pmType>
class ProcessReducer {
    public:
    ProcessReducer(tbb::concurrent_unordered_map<pair<time_t,int>, ProcessSummary *>& _index, tbb::concurrent_vector<ProcessSummary>& _summaries, size_t _maxSummaries, const string& _hostname, const string& _identifier, const string& _subidentifier):
        hostname(_hostname), identifier(_identifier), subidentifier(_subidentifier)
    {
        summaryIndex = &_index;
        summaries    = &_summaries;
        maxSummaries = _maxSummaries;
    }

    void operator()(const tbb::blocked_range<pair<pmType*,pmType*> > &r) {
        for (auto it: r) {
            reduce(it.first, it.second);
        }
    }
    private:
    void reduce(pmType *, pmType *);

    tbb::concurrent_unordered_map<pair<time_t,int>, ProcessSummary *> *summaryIndex;
    tbb::concurrent_vector<ProcessSummary> *summaries;
    size_t maxSummaries;
    string hostname;
    string identifier;
    string subidentifier;

    ProcessSummary *findSummary(time_t start, int pid) {
        ProcessSummary *ret = NULL;
        pair<time_t, int> key(start, pid);
        auto it = summaryIndex->find(key);
        if (it == summaryIndex->end()) {
            auto pos = summaries->emplace_back(hostname, identifier, subidentifier, start, pid);
            ret = &*pos;
        } else {
            ret = it->second;
        }
        return ret;
    }
};


template <>
void ProcessReducer<procstat>::reduce(procstat *start, procstat *end) {

    const size_t nRecords = end-start;
    if (nRecords == 0) {
        /* XXX throw exception, cause problems, etc XXX */
        return;
    }
    ProcessSummary *summary = findSummary(start->startTime, start->pid);
    vector<double> recTime(nRecords);
    vector<double> startTime(nRecords);
    vector<double> duration(nRecords);
    vector<double> cpu(nRecords);

    /* find most recent, undamaged record */
    procstat *record = end - 1;
    for ( ; record > start; --record) {
        if (record->utime >= (record-1)->utime) break;
    }

    summary->state = record->state;
    summary->pgrp = record->pgrp;
    summary->session = record->session;
    summary->tty = record->tty;
    summary->tpgid = record->tpgid;
    summary->realUid = record->realUid;
    summary->effUid = record->effUid;
    summary->realGid = record->realGid;
    summary->effGid = record->effGid;
    summary->flags = record->flags;
    summary->utime = record->utime;
    summary->stime = record->stime;
    summary->priority = record->priority;
    summary->nice = record->nice;
    summary->numThreads = record->numThreads;
    summary->vsize = record->vsize;
    summary->rss = record->rss;
    summary->rsslim = record->rsslim;
    summary->signal = record->signal;
    summary->blocked = record->blocked;
    summary->sigignore = record->sigignore;
    summary->sigcatch = record->sigcatch;
    summary->rtPriority = record->rtPriority;
    summary->policy = record->policy;
    summary->delayacctBlkIOTicks = record->delayacctBlkIOTicks;
    summary->guestTime = record->guestTime;
    summary->vmpeak = record->vmpeak;
    summary->rsspeak = record->rsspeak;
    summary->cpusAllowed = record->cpusAllowed;
    summary->io_rchar = record->io_rchar;
    summary->io_wchar = record->io_wchar;
    summary->io_syscr = record->io_syscr;
    summary->io_syscw = record->io_syscw;
    summary->io_readBytes = record->io_readBytes;
    summary->io_writeBytes = record->io_writeBytes;
    summary->io_cancelledWriteBytes = record->io_cancelledWriteBytes;
    summary->m_size = record->m_size;
    summary->m_resident = record->m_resident;
    summary->m_share = record->m_share;
    summary->m_text = record->m_text;
    summary->m_data = record->m_data;

    for (procstat *ptr = start; ptr < end; ++ptr) {
        size_t idx = ptr - start;
        recTime[idx] = ptr->recTime + ptr->recTimeUSec * 1e-6;
        startTime[idx] = ptr->startTime + ptr->recTimeUSec * 1e-6;
        duration[idx] = recTime[idx] - startTime[idx];
        cpu[idx] = (ptr->utime + ptr->stime) / 100.;
    }

    if (nRecords == 1) {
        /* no deltas to compute */
        return;
    }
    const size_t nDeltas  = nRecords - 1;
    vector<double> delta(nDeltas);
    vector<double> cpuRate(nDeltas);
    vector<double> iowRate(nDeltas);
    vector<double> iorRate(nDeltas);
    vector<double> msizeRate(nDeltas);
    vector<double> mresidentRate(nDeltas);
    /* calcuate deltas */
    for (size_t idx = 0; idx < end-start-1; ++idx) {
        delta[idx] = recTime[idx+1]-recTime[idx];
        cpuRate[idx] = (cpu[idx+1] - cpu[idx]) / delta[idx];
        iowRate[idx] = (start[idx+1].io_wchar - start[idx].io_wchar) / delta[idx];
        iorRate[idx] = (start[idx+1].io_rchar - start[idx].io_rchar) / delta[idx];
        msizeRate[idx] = (start[idx+1].m_size - start[idx].m_size) / delta[idx];
        mresidentRate[idx] = (start[idx+1].m_resident - start[idx].m_resident) / delta[idx];
    }
    summary->cpuRateMax = *( max_element(cpuRate.begin(), cpuRate.end()) );
    summary->iowRateMax = *( max_element(iowRate.begin(), iowRate.end()) );
    summary->iorRateMax = *( max_element(iorRate.begin(), iorRate.end()) );
    summary->msizeRateMax = *( max_element(msizeRate.begin(), msizeRate.end()) );
    summary->mresidentRateMax = *( max_element(mresidentRate.begin(), mresidentRate.end()) );
    summary->duration = *( max_element(duration.begin(), duration.end()) );
    summary->cpuTime = *( max_element(cpu.begin(), cpu.end()) );
    summary->nRecords = nRecords;

    /* calculate cross-correlation of the rates */
}

template <>
void ProcessReducer<procdata>::reduce(procdata *start, procdata *end) {

    size_t nRecords = end-start;
    if (nRecords == 0) {
        /* XXX throw exception, cause problems, etc XXX */
        return;
    }
    ProcessSummary *summary = findSummary(start->startTime, start->pid);

    /* find most recent, undamaged record */
    procdata *record = end - 1;
    for ( ; record >= start; --record) {
        if (strstr(record->exePath, "Unknown") == NULL) break;
    }
    if (record < start) {
        /* bad process record, bail */
        return;
    }

    strncpy(summary->execName, record->execName, EXEBUFFER_SIZE);
    memcpy(summary->cmdArgs, record->cmdArgs, record->cmdArgBytes < BUFFER_SIZE ? record->cmdArgBytes : BUFFER_SIZE);
    summary->cmdArgBytes = record->cmdArgBytes;
    strncpy(summary->exePath, record->exePath, BUFFER_SIZE);
    strncpy(summary->cwdPath, record->cwdPath, BUFFER_SIZE);
    summary->ppid = record->ppid;



}

class ProcessData {
    public:
    procstat *ps;
    procdata *pd;
    procfd   *fd;
    procobs  *obs;
    int n_ps;
    int n_pd;
    int n_fd;
    int n_obs;
    bool owndata;

    ProcessData(const HostCountData &cnt) {
        owndata = true;
        ps = new procstat[cnt.n_procstat];
        pd = new procdata[cnt.n_procdata];
        fd = new procfd[cnt.n_procfd];
        obs = new procobs[cnt.n_procobs];
        n_ps = cnt.n_procstat;
        n_pd = cnt.n_procdata;
        n_fd = cnt.n_procfd;
        n_obs = cnt.n_procobs;
    }

    void summarizeProcesses(const string& hostname) {
        /* summarization algorithm:
         *    sort process datastructures by identifier,subidentifier,starttime, pid, rectime
         */
        tbb::parallel_sort(ps, ps+n_ps, less_byprocess<procstat>);
        tbb::parallel_sort(pd, pd+n_pd, less_byprocess<procdata>);
        tbb::parallel_sort(fd, fd+n_fd, less_byprocess<procfd>);
        tbb::parallel_sort(obs, obs+n_obs, less_byprocess<procobs>);

        ProcessMasker<procstat> ps_mask(ps, n_ps);
        ProcessMasker<procdata> pd_mask(pd, n_pd);
        ProcessMasker<procfd>   fd_mask(fd, n_fd);
        ProcessMasker<procobs>  obs_mask(obs, n_obs);

        tbb::parallel_reduce(tbb::blocked_range<procstat*>(ps, (ps+n_ps), sizeof(procstat)), ps_mask);
        tbb::parallel_reduce(tbb::blocked_range<procdata*>(pd, (pd+n_pd), sizeof(procdata)), pd_mask);
        tbb::parallel_reduce(tbb::blocked_range<procfd*>(fd, (fd+n_fd), sizeof(procfd)), fd_mask);
        tbb::parallel_reduce(tbb::blocked_range<procobs*>(obs, (obs+n_obs), sizeof(procobs)), obs_mask);

        auto &ps_boundaries = ps_mask.getProcessBoundaries();
        auto &pd_boundaries = pd_mask.getProcessBoundaries();
        auto &fd_boundaries = fd_mask.getProcessBoundaries();
        auto &obs_boundaries = obs_mask.getProcessBoundaries();
        tbb::concurrent_unordered_map<pair<time_t,int>, ProcessSummary *> summaryIndex;
        size_t maxRecords = max({
            ps_boundaries.size(), pd_boundaries.size(),
            fd_boundaries.size(), obs_boundaries.size(),
        });

        tbb::concurrent_vector<ProcessSummary> summaries;
        summaries.reserve(maxRecords);

        /* XXX HERE XXX
        ProcessReducer<procstat> ps_red(summaryIndex, summaries, maxRecords, hostname);
        ProcessReducer<procdata> pd_red(summaryIndex, summaries, maxRecords, hostname);
        ProcessReducer<procfd>   fd_red(summaryIndex, summaries, maxRecords, hostname);
        ProcessReducer<procobs>  obs_red(summaryIndex, summaries, maxRecords);

        tbb::parallel_reduce(tbb::blocked_range<procstat>(ps_boundaries.begin(), ps_boundaries.end()), ps_red);
        tbb::parallel_reduce(tbb::blocked_range<procdata>(pd_boundaries.begin(), pd_boundaries.end()), pd_red);
        tbb::parallel_reduce(tbb::blocked_range<procfd>(fd_boundaries.begin(), fd_boundaries.end()), fd_red);
        tbb::parallel_reduce(tbb::blocked_range<procobs>(obs_boundaries.begin(), obs_boundaires.end()), obs_red);
        */
    }

    ~ProcessData() {
        if (owndata) {
            delete ps;
            delete pd;
            delete fd;
            delete obs;
        }
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
        input->mutex.lock();
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
        sort(l_hostCounts.begin(), l_hostCounts.end());
        input->mutex.unlock();
        return NULL;
    }

    private:
    H5FileControl *input;
    vector<HostCountData> **hostCounts;
};

class ReadH5ProcessData : public tbb::task {
    public:
    ReadH5ProcessData(H5FileControl *_input, const char *_hostname,
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
        input->mutex.lock();
        input->file->set_context(hostname, "", "");

        procstat *ps_ptr = &(output->ps[ps_offset]);
        ps_read = input->file->read_procstat(ps_ptr, 0, ps_count);

        procdata *pd_ptr = &(output->pd[pd_offset]);
        pd_read = input->file->read_procdata(pd_ptr, 0, pd_count);

        procfd *fd_ptr = &(output->fd[fd_offset]);
        fd_read = input->file->read_procfd(fd_ptr, 0, fd_count);

        procobs *obs_ptr = &(output->obs[obs_offset]);
        obs_read = input->file->read_procobs(obs_ptr, 0, obs_count);

        input->mutex.unlock();
        return NULL;
    }

    private:
    H5FileControl  *input;
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
            //usage(options, 1);
        }
    }
    const vector<string> &getProcmonH5Inputs() {
        return procmonh5_files;
    }
    bool debug;

    string input_filename;
};

vector<HostCountData> *mergeHostCounts(vector<vector<HostCountData> **>& counts)
{
    unordered_map<string, HostCountData *> mergeMap;
    vector<HostCountData> *ret = NULL;
    for (auto it = counts.begin(); it != counts.end(); ++it) {
        if (*it == NULL) {
            /* something failed with the host count parsing */
            cerr << "FAILURE: no data for host count" << endl;
            exit(1);
        }
        vector<HostCountData> &count = ***it;
        for (auto host: count) {
            auto loc = mergeMap.find(host.hostname);
            HostCountData *tgt = NULL;
            if (loc == mergeMap.end()) {
                tgt = new HostCountData(host);
                mergeMap[host.hostname] = tgt;
            } else {
                tgt = loc->second;
                *tgt += host;
            }
        }
    }
    if (mergeMap.size() == 0) return NULL;
    ret = new vector<HostCountData>(mergeMap.size());
    size_t idx = 0;
    for (auto it: mergeMap) {
        (*ret)[idx++] = *(it.second);
        delete it.second;
    }
    sort(ret->begin(), ret->end());
    tbb::parallel_sort(ret->begin(), ret->end());
    return ret;
}

int main(int argc, char **argv) {
    ProcmonSummarizeConfig config(argc, argv);

    tbb::task_scheduler_init init(config.nThreads != 0 ? config.nThreads : tbb::task_scheduler_init::automatic);

    /* open input h5 files, walk the metadata */
    vector<H5FileControl *> inputFiles;
    vector<vector<HostCountData>** > inputHostCounts;
    tbb::task_list metadataTasks;
    for (auto it: config.getProcmonH5Inputs()) {
        H5FileControl *input = new H5FileControl(new ProcHDF5IO(it, FILE_MODE_READ));
        vector<HostCountData> *data = NULL;
        inputHostCounts.push_back(&data);
        inputFiles.push_back(input);

        ReadH5Metadata task(input, &data);
        task.execute();
    }
    vector<HostCountData> *globalHostCounts = mergeHostCounts(inputHostCounts);

    for (auto it: *globalHostCounts) {
        cout << it.hostname << "; " << it.n_procstat << endl;
    }
    vector<HostCountData> cumulativeSum(globalHostCounts->size());
    vector<HostCountData*> hostCountPtrs(inputFiles.size());
    for (size_t idx = 0; idx < hostCountPtrs.size(); ++idx) {
        hostCountPtrs[idx] = &((**(inputHostCounts[idx]))[0]);
    }
    for (size_t idx = 0; idx < globalHostCounts->size(); ++idx) {
        HostCountData *host = &((*globalHostCounts)[idx]);
        ProcessData *processData = new ProcessData((*globalHostCounts)[idx]);

        for (size_t fileIdx = 0; fileIdx < inputFiles.size(); ++fileIdx) {
            HostCountData *ptr = hostCountPtrs[fileIdx];
            while (ptr != NULL && host != NULL && ptr->hostname < host->hostname) {
                ptr++;
            }
            if (ptr == NULL || ptr->hostname != host->hostname) {
                continue; // skip this file, the host wasn't found
            }
            hostCountPtrs[fileIdx] = ptr;
            cumulativeSum[fileIdx] += *ptr;

            tbb::task *reader = new ReadH5ProcessData(inputFiles[fileIdx], host->hostname.c_str(), *ptr, cumulativeSum[fileIdx]);
            reader->execute();
            delete reader;
        }

        /* at this point all of the process data for the host should be in processData */
        // first, sort the data in the most appropriate form for each dataset
        processData->summarizeProcesses(host->hostname);


        delete processData;
    }


    return 0;
}
