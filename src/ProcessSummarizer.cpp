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
#include <regex>

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <pwd.h>

#include <boost/program_options.hpp>
#define PROCMON_SUMMARIZE_VERSION 2.0
namespace po = boost::program_options;
using namespace std;

namespace std {
    template <>
    struct hash<pair<time_t,int> > {
        size_t operator()(const pair<time_t,int> &k) const {
            return ((hash<time_t>()(k.first) ^ (hash<int>()(k.second) << 1)) >> 1);
        }
    };
}

class ProcessSummary;
typedef unordered_map<pair<time_t,int>, ProcessSummary *> SummaryHashMap;

class AnalysisTask;

struct H5FileControl {
    ProcHDF5IO *file;

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
    return a.startTime == b.startTime && a.pid == b.pid;
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

    ~ProcessMasker() {
        if (ownmask) delete mask;
    }

    void operator()(const pmType *begin, const pmType *end) {
        count += end - begin;

        for (const pmType *ptr = begin; ptr != end; ++ptr) {
            size_t idx = ptr - start_ptr;
            mask[idx] = (idx == 0) || !equiv_byprocess<pmType>(*(ptr-1), *ptr);
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
    ProcessReducer(SummaryHashMap& _index, vector<ProcessSummary>& _summaries, size_t _maxSummaries, const string& _hostname):
        hostname(_hostname)
    {
        summaries    = &_summaries;
        maxSummaries = _maxSummaries;
        summaryIndex = &_index;
    }

    void operator()(const pair<pmType*,pmType*> *begin, const pair<pmType*,pmType*> *end) {
        for (const pair<pmType*,pmType*> *ptr = begin; ptr != end; ++ptr) {
            reduce(ptr->first, ptr->second);
        }
    }

    private:
    void reduce(pmType *, pmType *);

    SummaryHashMap *summaryIndex;
    vector<ProcessSummary> *summaries;
    size_t maxSummaries;
    string hostname;

    ProcessSummary *findSummary(const string& identifier, const string& subidentifier, time_t start, int pid) {
        ProcessSummary *ret = NULL;
        pair<time_t,int> key(start, pid);
        auto it = summaryIndex->find(key);
        if (it == summaryIndex->end()) {
            summaries->emplace_back(hostname, identifier, subidentifier, start, pid);
            auto pos = summaries->rbegin();
            ret = &*pos;
            (*summaryIndex)[key] = ret;
        } else {
            ret = it->second;
        }
        return ret;
    }
};

void calculateVariance(const vector<double>& vec, vector<double>& var) {
    double mean = 0;
    for (size_t idx = 0; idx < vec.size(); ++idx) {
        mean += vec[idx];
    }
    for (size_t idx = 0; idx < vec.size(); ++idx) {
        var[idx] = vec[idx] - mean;
    }
}

double calculateCovariance(const vector<double>& var1, const vector<double>& var2) {
    double num = 0;
    double denom1 = 0;
    double denom2 = 0;
    for (size_t idx = 0; idx < var1.size(); ++idx) {
        num += var1[idx] * var2[idx];
        denom1 += var1[idx] * var1[idx];
        denom2 += var2[idx] * var2[idx];
    }
    return num / ( sqrt(denom1) * sqrt(denom2) );
}

template <>
void ProcessReducer<procstat>::reduce(procstat *start, procstat *end) {

    const size_t nRecords = end-start;
    if (nRecords == 0) {
        /* XXX throw exception, cause problems, etc XXX */
        return;
    }
    ProcessSummary *summary = findSummary(start->identifier, start->subidentifier, start->startTime, start->pid);
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

    struct passwd *pwd = getpwuid(summary->realUid);
    if (pwd != NULL) {
        snprintf(summary->user, EXEBUFFER_SIZE, "%s", pwd->pw_name);
    } else {
        snprintf(summary->user, EXEBUFFER_SIZE, "%d", summary->realUid);
    }

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

    /* calculate covariance coefficients of the rates */
    vector<double> cpuRateVar(nDeltas);
    vector<double> iowRateVar(nDeltas);
    vector<double> iorRateVar(nDeltas);
    vector<double> msizeRateVar(nDeltas);
    vector<double> mresidentRateVar(nDeltas);

    calculateVariance(cpuRate, cpuRateVar);
    calculateVariance(iowRate, iowRateVar);
    calculateVariance(iorRate, iorRateVar);
    calculateVariance(msizeRate, msizeRateVar);
    calculateVariance(mresidentRate, mresidentRateVar);

    summary->cov_cpuXiow = calculateCovariance(cpuRateVar, iowRateVar);
    summary->cov_cpuXior = calculateCovariance(cpuRateVar, iorRateVar);
    summary->cov_cpuXmsize = calculateCovariance(cpuRateVar, msizeRateVar);
    summary->cov_cpuXmresident = calculateCovariance(cpuRateVar, mresidentRateVar);
    summary->cov_iowXior = calculateCovariance(iowRateVar, iorRateVar);
    summary->cov_iowXmsize = calculateCovariance(iowRateVar, msizeRateVar);
    summary->cov_iowXmresident = calculateCovariance(iowRateVar, mresidentRateVar);
    summary->cov_iorXmsize = calculateCovariance(iorRateVar, msizeRateVar);
    summary->cov_iorXmresident = calculateCovariance(iorRateVar, mresidentRateVar);
    summary->cov_msizeXmresident = calculateCovariance(msizeRateVar, mresidentRateVar);
}

template <>
void ProcessReducer<procdata>::reduce(procdata *start, procdata *end) {

    size_t nRecords = end-start;
    if (nRecords == 0) {
        /* XXX throw exception, cause problems, etc XXX */
        return;
    }
    ProcessSummary *summary = findSummary(start->identifier, start->subidentifier, start->startTime, start->pid);

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

    Scriptable *scriptObj = Scriptable::getScriptable(record->exePath, record->cmdArgs);
    if (scriptObj != NULL) {
        string script = (*scriptObj)();
        strncpy(summary->script, script.c_str(), BUFFER_SIZE);
        delete scriptObj;
    } else {
        summary->script[0] = 0;
    }

    char *slashPtr = strrchr(record->exePath, '/');
    if (slashPtr != NULL) {
        slashPtr += 1;
    } else {
        slashPtr = record->exePath;
    }
    strncpy(summary->execCommand, slashPtr, BUFFER_SIZE);

    slashPtr = strrchr(summary->script, '/');
    if (slashPtr != NULL) {
        slashPtr += 1;
    } else {
        slashPtr = summary->script;
    }
    if (strcmp(slashPtr, "COMMAND") == 0 || *slashPtr == 0) {
        strncpy(summary->command, summary->execCommand, BUFFER_SIZE);
    } else {
        strncpy(summary->command, slashPtr, BUFFER_SIZE);
    }
}

template <>
void ProcessReducer<procobs>::reduce(procobs *start, procobs *end) {
    size_t nObs = end-start;
    if (nObs == 0) {
        /* XXX throw exception, cause problems, etc XXX */
        return;
    }
    ProcessSummary *summary = findSummary(start->identifier, start->subidentifier, start->startTime, start->pid);
    summary->nObservations = nObs;
}

class ProcessData {
    private:
    SummaryHashMap summaryIndex;
    vector<ProcessSummary> summaries;

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

    ProcessData() {
        owndata = true;
        ps = NULL;
        pd = NULL;
        fd = NULL;
        obs = NULL;
        n_ps = 0;
        n_pd = 0;
        n_fd = 0;
        n_obs = 0;
    }

    void readData(const string& hostname, H5FileControl *input) {
        input->file->set_context(hostname, "", "");

        size_t ln_pd = input->file->get_nprocdata();
        size_t ln_ps = input->file->get_nprocstat();
        size_t ln_fd = input->file->get_nprocfd();
        size_t ln_obs = input->file->get_nprocobs();

        procstat *newps = new procstat[n_ps + ln_ps];
        procstat *ps_ptr = &(newps[n_ps]);
        size_t ps_read = input->file->read_procstat(ps_ptr, 0, ln_ps);
        if (ps != NULL) {
            memcpy(newps, ps, sizeof(procstat) * n_ps);
            delete[] ps;
        }
        ps = newps;
        n_ps = n_ps + ln_ps;

        procdata *newpd = new procdata[n_pd + ln_pd];
        procdata *pd_ptr = &(newpd[n_pd]);
        size_t pd_read = input->file->read_procdata(pd_ptr, 0, ln_pd);
        if (pd != NULL) {
            memcpy(newpd, pd, sizeof(procdata) * n_pd);
            delete[] pd;
        }
        pd = newpd;
        n_pd = n_pd + ln_pd;

        procfd *newfd = new procfd[n_fd + ln_fd];
        procfd *fd_ptr = &(newfd[n_fd]);
        size_t fd_read = input->file->read_procfd(fd_ptr, 0, ln_fd);
        if (fd != NULL) {
            memcpy(newfd, fd, sizeof(procfd) * n_fd);
            delete[] fd;
        }
        fd = newfd;
        n_fd = n_fd + ln_fd;

        procobs *newobs = new procobs[n_obs + ln_obs];
        procobs *obs_ptr = &(newobs[n_obs]);
        size_t obs_read = input->file->read_procobs(obs_ptr, 0, ln_obs);
        if (obs != NULL) {
            memcpy(newobs, obs, sizeof(procobs) * n_obs);
            delete[] obs;
        }
        obs = newobs;
        n_obs = n_obs + ln_obs;
    }

    void summarizeProcesses(const string& hostname, ProcessData *baseline, time_t baselineTime) {
        /* summarization algorithm:
         *    sort process datastructures by identifier,subidentifier,starttime, pid, rectime
         */
        sort(ps, ps+n_ps, less_byprocess<procstat>);
        sort(pd, pd+n_pd, less_byprocess<procdata>);
        sort(fd, fd+n_fd, less_byprocess<procfd>);
        sort(obs, obs+n_obs, less_byprocess<procobs>);

        ProcessMasker<procstat> ps_mask(ps, n_ps);
        ProcessMasker<procdata> pd_mask(pd, n_pd);
        ProcessMasker<procfd>   fd_mask(fd, n_fd);
        ProcessMasker<procobs>  obs_mask(obs, n_obs);

        ps_mask(ps, ps+n_ps);
        pd_mask(pd, pd+n_pd);
        fd_mask(fd, fd+n_fd);
        obs_mask(obs, obs+n_obs);

        auto &ps_boundaries = ps_mask.getProcessBoundaries();
        auto &pd_boundaries = pd_mask.getProcessBoundaries();
        auto &fd_boundaries = fd_mask.getProcessBoundaries();
        auto &obs_boundaries = obs_mask.getProcessBoundaries();
        size_t maxRecords = max({
            ps_boundaries.size(), pd_boundaries.size(),
            fd_boundaries.size(), obs_boundaries.size(),
        });

        cout << hostname << ": ps " << ps_boundaries.size() << "; pd " << pd_boundaries.size() << "; fd " << fd_boundaries.size() << "; obs " << obs_boundaries.size() << endl;

        summaries.reserve(maxRecords);

        ProcessReducer<procstat> ps_red(summaryIndex, summaries, maxRecords, hostname);
        ProcessReducer<procdata> pd_red(summaryIndex, summaries, maxRecords, hostname);
        ProcessReducer<procfd>   fd_red(summaryIndex, summaries, maxRecords, hostname);
        ProcessReducer<procobs>  obs_red(summaryIndex, summaries, maxRecords, hostname);

        ps_red(&*ps_boundaries.begin(), &*ps_boundaries.end());
        pd_red(&*pd_boundaries.begin(), &*pd_boundaries.end());
        obs_red(&*obs_boundaries.begin(), &*obs_boundaries.end());

        /* XXX HERE XXX
        tbb::parallel_reduce(tbb::blocked_range<procfd>(fd_boundaries.begin(), fd_boundaries.end()), fd_red);
        */
    }

    ~ProcessData() {
        if (owndata) {
            cout << "deleting data" << endl;
            delete[] ps;
            delete[] pd;
            delete[] fd;
            delete[] obs;
        }
    }

    ProcessSummary *findSummary(time_t start, int pid) {
        pair<time_t,int> key(start,pid);
        auto it = summaryIndex.find(key);
        if (it != summaryIndex.end()) {
            return it->second;
        }
        return NULL;
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

class ProcmonSummarizeConfig {
    private:
    vector<pair<string,regex*> > fs_monitor_regex;
    vector<string> procmonh5_files;
    unsigned int nThreads;
    string baseline_file;
    bool debug;

    public:

    ProcmonSummarizeConfig(int argc, char **argv) {
        vector<string> fs_monitor;

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
            ("baseline,b",po::value<string>(&baseline_file), "baseline file for process normalization")
            ("threads,t", po::value<unsigned int>(&nThreads), "number of worker threads to use (one additional I/O and controller thread will also run)")
            ("fsMonitor", po::value<vector<string> >(&fs_monitor)->composing(), "regexes matching filesystems to monitor")
        ;

        po::options_description options;
        options.add(basic).add(config);
        po::variables_map vm;
        try {
            po::store(po::command_line_parser(argc, argv).options(options).run(), vm);
            po::notify(vm);
            if (vm.count("help")) {
                cout << options << endl;
                exit(0);
            }
            if (vm.count("version")) {
                version();
                exit(0);
            }
            if (vm.count("debug")) {
                debug = true;
            }
        } catch (exception &e) {
            cout << e.what() << endl;
            cout << options << endl;
            exit(1);
        }

        if (nThreads < 1) {
            cerr << "Need at least one (1) worker thread!" << endl;
            //usage(options, 1);
        }
        for (string& it: fs_monitor) {
            size_t pos = it.find('=');
            if (pos == string::npos) continue;
            string key = it.substr(0, pos);
            string value = it.substr(pos+1);
            fs_monitor_regex.emplace_back(pair<string,regex*>(key,new regex(value)));
        }
    }

    ~ProcmonSummarizeConfig() {
        for (pair<string,regex*>& item: fs_monitor_regex) {
            delete item.second;
        }
    }

    inline const vector<string> &getProcmonH5Inputs() const {
        return procmonh5_files;
    }
    inline const int getNThreads() const {
        return nThreads;
    }

    inline const bool isDebug() const {
        return debug;
    }
    inline const string& getBaselinePath() const {
        return baseline_file;
    }
    inline const vector<pair<string,regex *> >& getFilesystemMonitorRegexes() const {
        return fs_monitor_regex;
    }
};

int main(int argc, char **argv) {
    ProcmonSummarizeConfig config(argc, argv);

    /* open input h5 files, walk the metadata */
    H5FileControl *baselineInput = NULL;
    vector<H5FileControl *> inputFiles;
    vector<vector<string> > inputHosts;
    vector<string> allHosts;
    vector<string> baselineHosts;
    if (config.getBaselinePath() != "") {
        baselineInput = new H5FileControl(new ProcHDF5IO(config.getBaselinePath(), FILE_MODE_READ));
        baselineInput->file->get_hosts(baselineHosts);
    }
    for (auto it: config.getProcmonH5Inputs()) {
        H5FileControl *input = new H5FileControl(new ProcHDF5IO(it, FILE_MODE_READ));
        inputFiles.push_back(input);

        vector<string> l_hosts;
        input->file->get_hosts(l_hosts);
        sort(l_hosts.begin(), l_hosts.end());
        inputHosts.push_back(l_hosts);
        allHosts.insert(allHosts.end(), l_hosts.begin(), l_hosts.end());
    }
    sort(allHosts.begin(), allHosts.end());
    string lastHost = "";
    size_t count = 0;
    for (string host: allHosts) {
        if (host == lastHost) continue;
        lastHost = host;

        ProcessData *processData = new ProcessData();
        ProcessData *baselineData = NULL;
        if (find(baselineHosts.begin(), baselineHosts.end(), host) != baselineHosts.end()) {
            baselineData = new ProcessData();
            baselineData->readData(host, baselineInput);
            baselineData->summarizeProcesses(host, NULL, 0);
        }

        for (size_t idx = 0; idx < inputFiles.size(); ++idx) {
            if (find(inputHosts[idx].begin(), inputHosts[idx].end(), host) == inputHosts[idx].end()) {
                continue;
            }
            processData->readData(host, inputFiles[idx]);
        }
        processData->summarizeProcesses(host, baselineData, 0);

        if (baselineData != NULL) {
            delete baselineData;
        }

        delete processData;
        count++;
    }

    for (auto file: inputFiles) delete file;
    if (baselineInput != NULL) {
        delete baselineInput;
    }

    return 0;
}
