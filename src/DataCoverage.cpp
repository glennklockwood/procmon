#include "config.h"
#include "ProcData.hh"
#include "ProcIO.hh"

#include <algorithm>
#include <signal.h>
#include <string.h>
#include <iostream>
#include <deque>
#include <unordered_map>
#include <vector>
#include <regex>

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <pwd.h>

#include <boost/program_options.hpp>
namespace po = boost::program_options;
using namespace std;

#define DATA_COVERAGE_VERSION "1.0"

struct ProcIdent {
    string hostname;
    int pid;
    time_t startTime;

    ProcIdent(const string& _hostname, const int _pid, const time_t _startTime):
        hostname(_hostname), pid(_pid), startTime(_startTime)
    {
    }
};

struct Ident {
    string identifier;
    string subidentifier;

    Ident(const char *_identifier, const char *_subidentifier): identifier(_identifier), subidentifier(_subidentifier) {}
};

const bool operator==(const Ident &lhs, const Ident &rhs) {
    return lhs.identifier == rhs.identifier && lhs.subidentifier == rhs.subidentifier;
}

const bool operator==(const ProcIdent &lhs, const ProcIdent &rhs) {
    return lhs.hostname == rhs.hostname && lhs.pid == rhs.pid && lhs.startTime == rhs.startTime;
}

template <class pmData>
const bool less_rectime(const pmData &a, const pmData &b) {
    int val = strncmp(a.identifier, b.identifier, IDENTIFIER_SIZE);
    if (val < 0) return true;
    if (val > 0) return false;
    val = strncmp(a.subidentifier, b.subidentifier, IDENTIFIER_SIZE);
    if (val < 0) return true;
    if (val > 0) return false;
    return a.recTime < b.recTime;
}

template <class pmData>
const bool equiv_rectime(const pmData &a, const pmData &b) {
    int val = strncmp(a.identifier, b.identifier, IDENTIFIER_SIZE);
    if (val != 0) return false;
    val = strncmp(a.subidentifier, b.subidentifier, IDENTIFIER_SIZE);
    if (val != 0) return false;
    return a.recTime == b.recTime;
}

namespace std {
    template <>
    struct hash<ProcIdent> {
        size_t operator()(const ProcIdent &k) const {
            return ((hash<string>()(k.hostname) ^ (k.pid << 1)) ^ k.startTime) >> 1;
        }
    };
}

struct ObsData {
    string host;
    string messageType;
    time_t start;
    time_t end;
    unsigned int nObs;
    unordered_map<ProcIdent, int> *procMap;
    vector<time_t> obsTimes;
    int gap;

    ObsData(const string &_host, const string &_messageType, const time_t _start, const time_t _end, unsigned int _nObs):
        host(_host), messageType(_messageType), start(_start), end(_end), nObs(_nObs)
    {
        procMap = new unordered_map<ProcIdent, int>();
        gap = 0;
    }
    ~ObsData() {
    }

    void mergeData(ObsData &other) {
        if (host != other.host) return;
        if (messageType == other.messageType) {
            if (other.start < start) {
                start = other.start;
            }
            if (other.end > end) {
                end = other.end;
            }
            nObs += other.nObs;
            for (auto it: *(other.procMap)) {
                auto local_it = (*procMap).find(it.first);
                if (local_it == procMap->end()) {
                    (*procMap)[it.first] = it.second;
                } else {
                    (*procMap)[it.first] += it.second;
                }
            }
            obsTimes.insert(obsTimes.end(), other.obsTimes.begin(), other.obsTimes.end());
        }
    }

    void calculateGap() {
        sort(obsTimes.begin(), obsTimes.end());
        for (size_t idx = 1; idx < obsTimes.size(); ++idx) {
            int l_gap = obsTimes[idx] - obsTimes[idx-1];
            if (idx == 1 || l_gap > gap) {
                gap = l_gap;
            }
        }
    }

    const bool operator<(const ObsData &other) const {
        int cmp1 = strcmp(host.c_str(), other.host.c_str());
        if (cmp1 < 0) return true;
        if (cmp1 > 0) return false;
        int cmp2 = strcmp(messageType.c_str(), other.messageType.c_str());
        if (cmp2 < 0) return true;
        if (cmp2 > 0) return false;
        if (start < other.start) return true;
        if (start > other.start) return false;
        if (end < other.end) return true;
        if (end > other.end) return false;
        if (nObs < other.nObs) return true;
        if (nObs > other.nObs) return false;
        if (procMap->size() < other.procMap->size()) return true;
        return false;
    }
};

const bool operator==(const ObsData &lhs, const ObsData &rhs) {
    int cmp1 = strcmp(lhs.host.c_str(), rhs.host.c_str());
    if (cmp1 != 0) return false;
    int cmp2 = strcmp(lhs.messageType.c_str(), rhs.messageType.c_str());
    if (cmp2 != 0) return false;
    return true;
}
const bool operator!=(const ObsData &lhs, const ObsData &rhs) {
    return !(lhs == rhs);
}

ostream& operator<<(ostream &os, const Ident &id) {
    os << id.identifier << "," << id.subidentifier;
    return os;
}

ostream& operator<<(ostream &os, const ObsData &obs) {
    os << obs.host << "," << obs.messageType << "," << obs.start << "," << obs.end << "," << obs.nObs << "," << obs.procMap->size() << "," << obs.gap;
    return os;
}

namespace std {
    template <>
    struct hash<Ident> {
        size_t operator()(const Ident &k) const {
            return ((hash<string>()(k.identifier) ^ (hash<string>()(k.subidentifier) << 1)) >> 1);
        }
    };
}

void version() {
    cout << "DataCoverage " << DATA_COVERAGE_VERSION << endl;
    exit(0);
}

class DataCoverageConfig {
    private:
    vector<string> procmonh5_files;
    bool debug;

    public:

    DataCoverageConfig(int argc, char **argv) {

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
    }

    ~DataCoverageConfig() {
    }

    inline const vector<string> &getProcmonH5Inputs() const {
        return procmonh5_files;
    }

    inline const bool isDebug() const {
        return debug;
    }
};

template <class pmType>
void set_summary(const string &host, const string &messageType, unordered_map<Ident, vector<ObsData>* > &jobData, vector<pmType> &data) {
    ObsData *currObs = NULL;
    for (size_t idx = 0; idx < data.size(); ++idx) {
        if (idx == 0 || !equiv_rectime<pmType>(data[idx], data[idx-1])) {
            Ident key(data[idx].identifier, data[idx].subidentifier);
            auto it = jobData.find(key);
            vector<ObsData> *vec = NULL;
            if (it == jobData.end()) {
                vec = new vector<ObsData>();
                jobData[key] = vec;
            } else {
                vec = it->second;
            }
            vector<ObsData> &obsvec = *vec;
            obsvec.emplace_back(host, messageType, data[idx].recTime, data[idx].recTime, 1);
            currObs = &(obsvec.back());
            currObs->obsTimes.push_back(data[idx].recTime);
        }
        if (currObs != NULL) {
            ProcIdent key(host, data[idx].pid, data[idx].startTime);
            auto pos = currObs->procMap->find(key);
            if (pos == currObs->procMap->end()) {
                (*(currObs->procMap))[key] = 1;
            } else {
                (*(currObs->procMap))[key] += 1;
            }
        }
    }
}

size_t summarize(ProcHDF5IO *file, const string &host, unordered_map<Ident, vector<ObsData>* >& jobData) {
    file->set_context(host, "*", "*");
    size_t read = 0;

    size_t n_procstat = file->get_nprocstat();
    vector<procstat> ps(n_procstat);
    read = file->read_procstat(&(ps[0]), 0, n_procstat);
    ps.resize(read);
    if (read != n_procstat) {
        cerr << "WARNING: should have gotten " << n_procstat << " but only got " << read << endl;
    }

    size_t n_procdata = file->get_nprocdata();
    vector<procdata> pd(n_procdata);
    read = file->read_procdata(&(pd[0]), 0, n_procdata);
    pd.resize(read);
    if (read != n_procdata) {
        cerr << "WARNING: should have gotten " << n_procdata << " but only got " << read << endl;
    }
    size_t n_procfd = file->get_nprocfd();
    vector<procfd> fd(n_procfd);
    read = file->read_procfd(&(fd[0]), 0, n_procfd);
    fd.resize(read);
    if (read != n_procfd) {
        cerr << "WARNING: should have gotten " << n_procfd << " but only got " << read << endl;
    }

    size_t n_procobs = file->get_nprocobs();
    vector<procobs> obs(n_procobs);
    read = file->read_procobs(&(obs[0]), 0, n_procobs);
    obs.resize(read);
    if (read != n_procobs) {
        cerr << "WARNING: should have gotten " << n_procobs << " but only got " << read << endl;
    }

    sort(ps.begin(), ps.end(), less_rectime<procstat>);
    sort(pd.begin(), pd.end(), less_rectime<procdata>);
    sort(fd.begin(), fd.end(), less_rectime<procfd>);
    sort(obs.begin(), obs.end(), less_rectime<procobs>);

    set_summary<procstat>(host, "procstat", jobData, ps);
    set_summary<procdata>(host, "procdata", jobData, pd);
    set_summary<procfd>(host, "procfd", jobData, fd);
    set_summary<procobs>(host, "procobs", jobData, obs);
}

int main(int argc, char **argv) {
    DataCoverageConfig config(argc, argv);

    vector<vector<string> > inputHosts;
    unordered_map<Ident, vector<ObsData>* > jobData;
    for (const string &it: config.getProcmonH5Inputs()) {
        ProcHDF5IO *input = new ProcHDF5IO(it, FILE_MODE_READ);

        vector<string> hosts;
        input->get_hosts(hosts);
        sort(hosts.begin(), hosts.end());
        for (string &host: hosts) {
            summarize(input, host, jobData);
        }

        delete input;
    }

    for(auto &it: jobData) {
        const Ident &key = it.first;
        vector<ObsData> &vec = *(it.second);
        vector<ObsData> *final = new vector<ObsData>();
        sort(vec.begin(), vec.end());
        ObsData *currObs = &(vec[0]);
        for (size_t idx = 1; idx < vec.size(); ++idx) {
            if ((*currObs) != vec[idx]) {
                final->push_back(*currObs);
                currObs = &(vec[idx]);
            } else {
                currObs->mergeData(vec[idx]);
            }
        }
        final->push_back(*currObs);
        delete it.second;

        sort(final->begin(), final->end());
        for (ObsData &obs: *final) {
            obs.calculateGap();
        }
        for (const ObsData &obs: *final) {
            cout << key << "," << obs << endl;
        }
    }

    return 0;
}
