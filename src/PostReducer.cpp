#include "config.h"
#include "ProcData.hh"
#include "ProcIO.hh"
#include <signal.h>
#include <string.h>
#include <iostream>
#include <unordered_map>
#include <vector>
#include "ProcReducerData.hh"

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <boost/program_options.hpp>
#define POSTREDUCER_VERSION "2.4"
namespace po = boost::program_options;
using namespace std;

/* ProcReducer
    * Takes in procmon data from all sources and only writes the minimal record
    * Writes an HDF5 file for all procdata foreach day
*/

void version() {
    cout << "PostReducer " << POSTREDUCER_VERSION;
    cout << endl;
    exit(0);
}

class ReducerInput {
    public:
    ReducerInput(const string input_filename) {
        file = new ProcHDF5IO(input_filename, FILE_MODE_READ);
        file->get_hosts(hosts);
    }
    ~ReducerInput() {
        delete file;
    }
    ProcHDF5IO *file;
    vector<string> hosts;
    bool has_host(const string& host) {
        vector<string>::iterator iter = find(hosts.begin(), hosts.end(), host);
        return iter != hosts.end();
    }

};


static int cmp_procstat_rec(const void *p1, const void *p2) {
    const procstat *a = *((const procstat **) p1);
    const procstat *b = *((const procstat **) p2);

    if (a->recTime < b->recTime) {
        return -1;
    }
    if (a->recTime > b->recTime) {
        return 1;
    }
    if (a->recTimeUSec < b->recTimeUSec) {
        return -1;
    }
    if (a->recTimeUSec > b->recTimeUSec) {
        return 1;
    }
    return 0;
}

static int cmp_procdata_rec(const void *p1, const void *p2) {
    const procdata *a = *((const procdata **) p1);
    const procdata *b = *((const procdata **) p2);

    if (a->recTime < b->recTime) {
        return -1;
    }
    if (a->recTime > b->recTime) {
        return 1;
    }
    if (a->recTimeUSec < b->recTimeUSec) {
        return -1;
    }
    if (a->recTimeUSec > b->recTimeUSec) {
        return 1;
    }
    return 0;
}

static int cmp_procfd_rec(const void *p1, const void *p2) {
    const procfd *a = *((const procfd **) p1);
    const procfd *b = *((const procfd **) p2);

    if (a->recTime < b->recTime) {
        return -1;
    }
    if (a->recTime > b->recTime) {
        return 1;
    }
    if (a->recTimeUSec < b->recTimeUSec) {
        return -1;
    }
    if (a->recTimeUSec > b->recTimeUSec) {
        return 1;
    }
    return 0;
}

static int cmp_netstat_rec(const void *p1, const void *p2) {
    const netstat *a = *((const netstat **) p1);
    const netstat *b = *((const netstat **) p2);

    if (a->recTime < b->recTime) {
        return -1;
    }
    if (a->recTime > b->recTime) {
        return 1;
    }
    if (a->recTimeUSec < b->recTimeUSec) {
        return -1;
    }
    if (a->recTimeUSec > b->recTimeUSec) {
        return 1;
    }
    return 0;
}

template<class T> size_t get_dataset_count(ProcHDF5IO *);
template<class T> unsigned int read_dataset(ProcHDF5IO *input, T *data, size_t n);
template<class T> unsigned int write_dataset(ProcHDF5IO *input, T *data, size_t n);
template<class T> bool isBad(const T&);
template<class T> bool isChanged(const T&, const T&);

template <>
size_t get_dataset_count<netstat>(ProcHDF5IO *input) {
    return input->get_nnetstat();
}

template <>
unsigned int read_dataset<netstat>(ProcHDF5IO *input, netstat *data, size_t n) {
    return input->read_netstat(data, 0, n);
}

template <>
unsigned int write_dataset<netstat>(ProcHDF5IO *input, netstat *data, size_t n) {
    return input->write_netstat(data, 0, n);
}

template <class pmType>
const inline bool lessRecTime(const pmType& a, const pmType& b) {
    return (a.recTime + (double)a.recTimeUSec * 1e-6) < (b.recTime + (double)b.recTimeUSec * 1e-6);
}

template <class pmType>
struct SelfSameRecHasher {
    size_t operator()(const pmType&) const { } 
};

template <class pmType>
struct SelfSameRecEqual {
    bool operator()(const pmType&, const pmType&) const { }
};


template <>
struct SelfSameRecHasher<netstat*> {

    size_t operator()(const netstat* net) const {
        // ip addresses should be relatively well distributed over the 32bit address space
        // port numbers favor the lower bits, so shift them left 16 bits
        size_t remoteAddrHash = net->remote_address ^ (net->remote_port << 16);
        size_t localAddrHash  = net->local_address ^ (net->local_port << 16);
        size_t inodeHashUpper = net->inode >> 16;
        size_t inodeHashLower = net->inode & 0xFFFFFFFF;
        remoteAddrHash ^= inodeHashUpper;
        localAddrHash ^= inodeHashLower;
        return (remoteAddrHash << 32) ^ localAddrHash;
    }
};

template <>
struct SelfSameRecEqual<netstat*> {
    bool operator()(const netstat* a, const netstat* b) const {
        return a->remote_address == b->remote_address && a->remote_port == b->remote_port &&
            a->local_address == b->local_address && a->local_port == b->local_port &&
            a->inode == b->inode;
    }
};

template <>
bool isBad<netstat>(const netstat& a) {
    return false;
}

template <>
bool isChanged<netstat>(const netstat& a, const netstat& b) {
    return true;
}

template <class pmType>
size_t read_and_reduce_data(const string &hostname, vector<ReducerInput*> &local_inputs, ProcHDF5IO *bad_outputFile, ProcHDF5IO *outputFile) {
    /* read all the procfd records */
    vector<pmType> data;
    size_t n_read = 0;
    for (auto iter = local_inputs.begin(); iter != local_inputs.end(); ++iter) {
        ReducerInput *input = *iter;
        input->file->set_context(hostname, "", "");
        size_t local_n = get_dataset_count<pmType>(input->file);
        data.resize(data.size() + local_n);
        pmType *ptr = &(data[n_read]);
        unsigned int l_nRead = read_dataset<pmType>(input->file, ptr, local_n);
        n_read += l_nRead;
    }

    /* sort the procfd records by observation time */
    sort(data.begin(), data.end(), lessRecTime<pmType>);

    /* reduce the data */
    size_t nBad = 0;
    size_t nWrite = 0;
    unordered_map<pmType*,pmType*,SelfSameRecHasher<pmType*>,SelfSameRecEqual<pmType*> > dataMap;
    vector<pmType*> writeRecords;
    for (pmType &obj: data) {
        if (isBad<pmType>(obj)) {
            bad_outputFile->set_context(hostname, obj.identifier, obj.subidentifier);
            write_dataset<pmType>(bad_outputFile, &obj, 1);
            nBad++;
            continue;
        }

        /* find the most recent record we've examined with this pid */
        auto it = dataMap.find(&obj);

        /* if this record differs from previous observation, mark previous
           observation for explicit write (everything in map will be written
           as well */
        if (it != dataMap.end() && isChanged<pmType>(obj, *(it->second))) {
            writeRecords.push_back(it->second);
        }
        dataMap[&obj] = &obj;
    }
    for (auto it = dataMap.begin(); it != dataMap.end(); ++it) {
        writeRecords.push_back(it->second);
    }

    if (writeRecords.size() > 0) {
        vector<pmType> scratch(writeRecords.size());
        pmType *ptr = &*scratch.begin();;
        for (auto it = writeRecords.begin(); it != writeRecords.end(); ++it) {
            memcpy(ptr++, *it, sizeof(pmType));
        }
        sort(scratch.begin(), scratch.end(), lessRecTime<pmType>);

        outputFile->set_context(hostname, "Any", "Any");
        outputFile->set_override_context(true);
        write_dataset<pmType>(outputFile, &*scratch.begin(), scratch.size());
        outputFile->set_override_context(false);
    }
    return writeRecords.size();
}

class PostReducerConfig {
public:
    PostReducerConfig(int argc, char **argv) {
        /* Parse command line arguments */
        po::options_description basic("Basic Options");
        basic.add_options()
            ("version", "Print version information")
            ("help,h", "Print help message")
            ("verbose,v", "Print extra (debugging) information")
        ;
        po::options_description config("Configuration Options");
        config.add_options()
            ("input,i",po::value<std::vector< std::string> >(&(this->input_filenames))->composing(), "input filename (required)")
            ("output,o",po::value<std::string>(&(this->output_filename)), "output filename (required)")
            ("badoutput,b",po::value<std::string>(&(this->bad_output_filename)), "bad output filename (required)")
            ("statblock",po::value<int>(&(this->statBlockSize))->default_value(DEFAULT_STAT_BLOCK_SIZE), "number of stat records per block in hdf5 file" )
            ("datablock",po::value<int>(&(this->dataBlockSize))->default_value(DEFAULT_DATA_BLOCK_SIZE), "number of data records per block in hdf5 file" )
            ("fdblock",po::value<int>(&(this->dataBlockSize))->default_value(DEFAULT_FD_BLOCK_SIZE), "number of fd records per block in hdf5 file" )
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
    }
    bool debug;

    vector<string> input_filenames;
    string output_filename;
    string bad_output_filename;

    int dataBlockSize;
    int statBlockSize;
    int fdBlockSize;
};


int main(int argc, char **argv) {
    PostReducerConfig config(argc, argv);

    cout << "output: " << config.output_filename << endl;
    vector<ReducerInput*> inputs;
    vector<vector<string> > input_hosts;
    vector<string> all_hosts;
    char buffer[BUFFER_SIZE];
    char *ptr = buffer;
    unsigned long start_time = 0;
    unsigned long stop_time = 0;
    unsigned long n_writes = 0;

    for (unsigned int idx = 0; idx < config.input_filenames.size(); idx++) {
        ReducerInput *l_input = new ReducerInput(config.input_filenames[idx]);;
        unsigned long t_start = 0;
        unsigned long t_stop = 0;
        unsigned long t_n_writes = 0;
        l_input->file->metadata_get_uint("recording_start", &t_start);
        if (start_time == 0 || t_start < start_time) {
            start_time = t_start;
        }
        l_input->file->metadata_get_uint("recording_stop", &t_stop);
        if (stop_time == 0 || t_stop > stop_time) {
            stop_time = t_stop;
        }
        l_input->file->metadata_get_uint("n_writes", &t_n_writes);
        n_writes += t_n_writes;

        ptr += snprintf(ptr, BUFFER_SIZE - (ptr - buffer), "%s%s", idx > 0 ? "," : "", config.input_filenames[idx].c_str());
        for (vector<string>::iterator host = l_input->hosts.begin(), end = l_input->hosts.end(); host != end; ++host) {
            vector<string>::iterator found_host = find(all_hosts.begin(), all_hosts.end(), *host);
            if (found_host == all_hosts.end()) {
                all_hosts.push_back(*host);
            }
        }
        inputs.push_back(l_input);
    }
    ProcHDF5IO* outputFile = new ProcHDF5IO(config.output_filename, FILE_MODE_WRITE);
    ProcHDF5IO* bad_outputFile = new ProcHDF5IO(config.bad_output_filename, FILE_MODE_WRITE);
    outputFile->metadata_set_string("source", buffer);
    bad_outputFile->metadata_set_string("source", buffer);
    outputFile->metadata_set_string("writer", "PostReducer");
    bad_outputFile->metadata_set_string("writer", "PostReducer");
    outputFile->metadata_set_string("writer_version", POSTREDUCER_VERSION);
    bad_outputFile->metadata_set_string("writer_version", POSTREDUCER_VERSION);
    gethostname(buffer, BUFFER_SIZE);
    buffer[BUFFER_SIZE-1] = 0;
    outputFile->metadata_set_string("writer_host", buffer);
    bad_outputFile->metadata_set_string("writer_host", buffer);
    outputFile->metadata_set_uint("recording_start", start_time);
    bad_outputFile->metadata_set_uint("recording_start", start_time);
    outputFile->metadata_set_uint("recording_stop", stop_time);
    bad_outputFile->metadata_set_uint("recording_stop", stop_time);
    outputFile->metadata_set_uint("n_writes", n_writes);
    bad_outputFile->metadata_set_uint("n_writes", n_writes);

    string hostname, identifier, subidentifier;

    int saveCnt = 0;
    int nRecords = 0;
    void* data = NULL;
    size_t data_size = 0;
    ProcessList spare_deck(0);

    int host_num = 0;
    for (auto ptr = all_hosts.begin(), end = all_hosts.end(); ptr != end; ++ptr) {
        vector<ReducerInput*> local_inputs;
        ProcessList p_list(0);

        hostname = *ptr;
        identifier = "";
        subidentifier = "";

        int n_procdata = 0;
        int n_procstat = 0;
        int n_procfd = 0;
        int n_netstat = 0;

        host_num++;
        cout << "Processing " << hostname << " (" << host_num << "/" << all_hosts.size() << ")" << endl;

        /* work out which inputs have this host, determine counts for each record type */
        for (auto iter = inputs.begin(), end = inputs.end(); iter != end; ++iter) {
            ReducerInput *input = *iter;
            if (input->has_host(hostname)) {
                local_inputs.push_back(input);
                input->file->set_context(hostname, identifier, subidentifier);
                n_procdata += input->file->get_nprocdata();
                n_procstat += input->file->get_nprocstat();
                n_procfd += input->file->get_nprocfd();
                n_netstat += input->file->get_nnetstat();
            }
        }

        procobs *observations = new procobs[n_procstat];
        bzero(observations, sizeof(procobs)*n_procstat);
        int obs_idx = 0;

        /* ensure enough memory is allocated for reading all these data */
        size_t alloc_size = sizeof(procstat) * n_procstat;
        size_t talloc = sizeof(procdata) * n_procdata;
        size_t talloc2 = sizeof(procfd) * n_procfd;
        size_t talloc3 = sizeof(netstat) * n_netstat;
        alloc_size = alloc_size > talloc ? alloc_size : talloc;
        alloc_size = alloc_size > talloc2 ? alloc_size : talloc2;
        alloc_size = alloc_size > talloc3 ? alloc_size : talloc3;

        if (data == NULL || data_size < alloc_size) {
            data = realloc(data, alloc_size);
            data_size = alloc_size;
        }
        if (data == NULL) { cerr << "failed to alloc" << endl; exit(1); }

        /* read all the procstat records */
        bzero(data, data_size);
        procstat *ps_ptr = (procstat *)data;
        procstat *ps_sort[n_procstat];
        procstat *ps_keep[n_procstat];
        procstat **ps_pptr = ps_sort;
        unsigned int nReadPS = 0;
        for (auto iter = local_inputs.begin(); iter != local_inputs.end(); ++iter) {
            ReducerInput *input = *iter;
            input->file->set_context(hostname, identifier, subidentifier);
            int local_n_procstat = input->file->get_nprocstat();
            unsigned int l_nReadPS = input->file->read_procstat(ps_ptr, 0, local_n_procstat);
            for (int i = 0; i < local_n_procstat; i++) {
                *ps_pptr++ = &(ps_ptr[i]);
            }
            ps_ptr += local_n_procstat;
            nReadPS += l_nReadPS;
        }

        /* sort the procstat records by observation time */
        qsort(ps_sort, n_procstat, sizeof(procstat *), cmp_procstat_rec);

        /* reduce the data */
        ps_pptr = ps_keep;
        unsigned int nWritePS = 0;
        unsigned int nBadPS = 0;
        for (int i = 0; i < nReadPS; i++) {
            procstat *procStat = ps_sort[i];

            if (procstatbad(procStat) > 0) {
                bad_outputFile->set_context(hostname, string(procStat->identifier), string(procStat->subidentifier));
                bad_outputFile->write_procstat(procStat, 0, 1);
                nBadPS++;
                continue;
            }

            procobs *procObs = &(observations[obs_idx++]);
            strncpy(procObs->identifier, procStat->identifier, IDENTIFIER_SIZE);
            strncpy(procObs->subidentifier, procStat->subidentifier, IDENTIFIER_SIZE);
            procObs->pid = procStat->pid;
            procObs->recTime = procStat->recTime;
            procObs->recTimeUSec = procStat->recTimeUSec;
            procObs->startTime = procStat->startTime;
            procObs->startTimeUSec = procStat->startTimeUSec;

            /* find the most recent record we've examined with this pid */
            procstat **rec = NULL;
            for (procstat **ptr = ps_pptr - 1; ptr >= ps_keep; ptr--) {
                if ((*ptr)->pid == procStat->pid) {
                    rec = ptr;
                    break;
                }
            }
            /* if we haven't seen this pid before, or if the record differs
               then add this record to the keep list */
            if (rec == NULL || procstatcmp(*procStat, **rec) != 0) {
                *ps_pptr++ = procStat;
            } else if (rec != NULL) {
                *rec = procStat;
            }
        }
        int nKeepPS = ps_pptr - ps_keep;
        procstat *ps_buff = new procstat[nKeepPS];
        bzero(ps_buff, sizeof(procstat)*nKeepPS);
        for (procstat **ptr = ps_keep; ptr < ps_pptr; ptr++) {
            memcpy(&(ps_buff[ptr-ps_keep]), *ptr, sizeof(procstat));
        }
        /* write out all of the procstat records at once */
        if (nKeepPS > 0) {
            outputFile->set_context(hostname, "Any", "Any");
            outputFile->set_override_context(true);
            outputFile->write_procstat(ps_buff, 0, nKeepPS);
            outputFile->set_override_context(false);
            nWritePS = nKeepPS;
        }
        delete ps_buff;

        /* write out all the observations */
        outputFile->set_context(hostname, "Any", "Any");
        outputFile->set_override_context(true);
        outputFile->write_procobs(observations, 0, nReadPS);
        outputFile->set_override_context(false);

        delete observations;

        /* read all the procdata records */
        bzero(data, data_size);
        procdata *pd_ptr = (procdata *)data;
        procdata *pd_sort[n_procdata];
        procdata *pd_keep[n_procdata];
        procdata **pd_pptr = pd_sort;
        unsigned int nReadPD = 0;
        for (auto iter = local_inputs.begin(); iter != local_inputs.end(); ++iter) {
            ReducerInput *input = *iter;
            input->file->set_context(hostname, identifier, subidentifier);
            int local_n_procdata = input->file->get_nprocdata();
            unsigned int l_nReadPD = input->file->read_procdata(pd_ptr, 0, local_n_procdata);
            for (int i = 0; i < local_n_procdata; i++) {
                *pd_pptr++ = &(pd_ptr[i]);
            }
            pd_ptr += local_n_procdata;
            nReadPD += l_nReadPD;
        }

        /* sort the procdata records by observation time */
        qsort(pd_sort, n_procdata, sizeof(procdata *), cmp_procdata_rec);

        /* reduce the data */
        pd_pptr = pd_keep;
        unsigned int nWritePD = 0;
        unsigned int nBadPD = 0;
        for (int i = 0; i < nReadPD; i++) {
            procdata *procData = pd_sort[i];

            if (procdatabad(procData) > 0) {
                bad_outputFile->set_context(hostname, string(procData->identifier), string(procData->subidentifier));
                bad_outputFile->write_procdata(procData, 0, 1);
                nBadPD++;
                continue;
            }

            /* find the most recent record we've examined with this pid */
            procdata **rec = NULL;
            for (procdata **ptr = pd_pptr - 1; ptr >= pd_keep; ptr--) {
                if ((*ptr)->pid == procData->pid) {
                    rec = ptr;
                    break;
                }
            }
            /* if we haven't seen this pid before, or if the record differs
               then add this record to the keep list */
            if (rec == NULL || procdatacmp(*procData, **rec) != 0) {
                *pd_pptr++ = procData;
            } else if (rec != NULL) {
                *rec = procData;
            }
        }
        int nKeepPD = pd_pptr - pd_keep;
        procdata *pd_buff = new procdata[nKeepPD];
        bzero(pd_buff, sizeof(procdata)*nKeepPD);
        for (procdata **ptr = pd_keep; ptr < pd_pptr; ptr++) {
            memcpy(&(pd_buff[ptr-pd_keep]), *ptr, sizeof(procdata));
        }
        /* write out all of the procdata records at once */
        if (nKeepPD > 0) {
            outputFile->set_context(hostname, "Any", "Any");
            outputFile->set_override_context(true);
            outputFile->write_procdata(pd_buff, 0, nKeepPD);
            outputFile->set_override_context(false);
            nWritePD = nKeepPD;
        }
        delete pd_buff;

        /* read all the procfd records */
        bzero(data, data_size);
        procfd *fd_ptr = (procfd *)data;
        procfd *fd_sort[n_procfd];
        procfd *fd_keep[n_procfd];
        procfd **fd_pptr = fd_sort;
        unsigned int nReadFD = 0;
        for (auto iter = local_inputs.begin(); iter != local_inputs.end(); ++iter) {
            ReducerInput *input = *iter;
            input->file->set_context(hostname, identifier, subidentifier);
            int local_n_procfd = input->file->get_nprocfd();
            unsigned int l_nReadFD = input->file->read_procfd(fd_ptr, 0, local_n_procfd);
            for (int i = 0; i < local_n_procfd; i++) {
                *fd_pptr++ = &(fd_ptr[i]);
            }
            fd_ptr += local_n_procfd;
            nReadFD += l_nReadFD;
        }

        /* sort the procfd records by observation time */
        qsort(fd_sort, n_procfd, sizeof(procfd *), cmp_procfd_rec);

        /* reduce the data */
        fd_pptr = fd_keep;
        unsigned int nWriteFD = 0;
        unsigned int nBadFD = 0;
        for (int i = 0; i < nReadFD; i++) {
            procfd *procFD = fd_sort[i];

            if (procfdbad(procFD) > 0) {
                bad_outputFile->set_context(hostname, string(procFD->identifier), string(procFD->subidentifier));
                bad_outputFile->write_procfd(procFD, 0, 1);
                nBadFD++;
                continue;
            }

            /* find the most recent record we've examined with this pid */
            procfd **rec = NULL;
            for (procfd **ptr = fd_pptr - 1; ptr >= fd_keep; ptr--) {
                if ((*ptr)->pid == procFD->pid && (*ptr)->fd == procFD->fd) {
                    rec = ptr;
                    break;
                }
            }
            /* if we haven't seen this pid before, or if the record differs
               then add this record to the keep list */
            if (rec == NULL || procfdcmp(*procFD, **rec) != 0) {
                *fd_pptr++ = procFD;
            } else if (rec != NULL) {
                *rec = procFD;
            }
        }
        int nKeepFD = fd_pptr - fd_keep;
        procfd *fd_buff = new procfd[nKeepFD];
        bzero(fd_buff, sizeof(procfd)*nKeepFD);
        for (procfd **ptr = fd_keep; ptr < fd_pptr; ptr++) {
            memcpy(&(fd_buff[ptr-fd_keep]), *ptr, sizeof(procfd));
        }
        /* write out all of the procfd records at once */
        if (nKeepFD > 0) {
            outputFile->set_context(hostname, "Any", "Any");
            outputFile->set_override_context(true);
            outputFile->write_procfd(fd_buff, 0, nKeepFD);
            outputFile->set_override_context(false);
            nWriteFD = nKeepFD;
        }
        delete fd_buff;

        read_and_reduce_data<netstat>(hostname, local_inputs, bad_outputFile, outputFile);

        cout << *ptr << "," << n_procstat << "(" << nReadPS << "," << nWritePS << ", BAD:" << nBadPS << "),";
        cout << "," << n_procdata << "(" << nReadPD << "," << nWritePD << ", BAD:" << nBadPD << "),";
        cout << "," << n_procfd << "(" << nReadFD << "," << nWriteFD << ", BAD:" << nBadFD << "),";
        cout << endl;
        p_list.find_expired_processes(&spare_deck);
        outputFile->trim_segments(time(NULL)+1);
        outputFile->flush();
        bad_outputFile->flush();

    }

    delete outputFile;
    delete bad_outputFile;
    for (unsigned int idx = 0; idx < inputs.size(); idx++) {
        delete inputs[idx];
    }

    return 0;
}
