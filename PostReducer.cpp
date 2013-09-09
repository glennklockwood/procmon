#include "config.h"
#include "ProcData.hh"
#include "ProcIO.hh"
#include <signal.h>
#include <string.h>
#include <iostream>
#include "ProcReducerData.hh"

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <boost/program_options.hpp>
#define PROCREDUCER_VERSION 2.2
namespace po = boost::program_options;

/* ProcReducer
    * Takes in procmon data from all sources and only writes the minimal record
    * Writes an HDF5 file for all procdata foreach day
*/

void version() {
    cout << "PostReducer " << PROCREDUCER_VERSION;
    cout << endl;
    exit(0);
}

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

class ReducerInput {
    public:
    ReducerInput(const string input_filename) {
        file = new ProcHDF5IO(input_filename, FILE_MODE_READ);
        file->get_hosts(hosts);
    }
    ProcHDF5IO *file;
    vector<string> hosts;
    bool has_host(const string& host) {
        vector<string>::iterator iter = find(hosts.begin(), hosts.end(), host);
        return iter != hosts.end();
    }

};


int main(int argc, char **argv) {
    PostReducerConfig config(argc, argv);

    cout << "output: " << config.output_filename << endl;
    vector<ReducerInput*> inputs;
    vector<vector<string> > input_hosts;
    vector<string> all_hosts;

    for (unsigned int idx = 0; idx < config.input_filenames.size(); idx++) {
        ReducerInput *l_input = new ReducerInput(config.input_filenames[idx]);;
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

    string hostname, identifier, subidentifier;

    int saveCnt = 0;
    int nRecords = 0;
    void* data = NULL;
    size_t data_size = 0;
    ProcessList spare_deck(0);

    char buffer[1024];
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
            }
        }

        procobs *observations = new procobs[n_procstat];
        bzero(observations, sizeof(procobs)*n_procstat);
        int obs_idx = 0;

        /* ensure enough memory is allocated for reading all these data */
        size_t alloc_size = sizeof(procstat) * n_procstat;
        size_t talloc = sizeof(procdata) * n_procdata;
        size_t talloc2 = sizeof(procfd) * n_procfd;
        alloc_size = alloc_size > talloc ? alloc_size : talloc;
        alloc_size = alloc_size > talloc2 ? alloc_size : talloc2;
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