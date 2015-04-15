/*******************************************************************************
procmon, Copyright (c) 2014, The Regents of the University of California,
through Lawrence Berkeley National Laboratory (subject to receipt of any
required approvals from the U.S. Dept. of Energy).  All rights reserved.

If you have questions about your rights to use or distribute this software,
please contact Berkeley Lab's Technology Transfer Department at  TTD@lbl.gov.

The LICENSE file in the root directory of the source code archive describes the
licensing and distribution rights and restrictions on this software.

Author:   Douglas Jacobsen <dmj@nersc.gov>
*******************************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <syslog.h>
#include <dirent.h>
#include <unistd.h>
#include <string.h>
#include <signal.h>
#include <time.h>
#include <sys/file.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <math.h>
#include <iostream>
#include <string>
#include <map>
#include <unordered_map>
#include <vector>
#include <algorithm>
#include <boost/algorithm/string.hpp>

#ifdef SECURED
#include <pthread.h>
#include <sys/capability.h>
#include <sys/prctl.h>
#include <errno.h>
#endif

#include "ProcData.hh"
#include "ProcIO.hh"
#include "procmon.hh"

inline void fatal_error(const char *error, int err) {
    fprintf(stderr, "Failed: %s; %d; bailing out.\n", error, err);
    exit(1);
}

/* global variables - these are global for signal handling, and inter-thread communication */
int cleanUpFlag = 0;
int search_procfs_count = 0;
vector<int> pids;
vector<int> keepflag;
vector<procstat> tmp_procStat;
vector<procdata> tmp_procData;
vector<procstat> global_procStat;
vector<procdata> global_procData;
vector<procfd>   global_procFD;
unordered_map<size_t,netstat>  global_netstat;

void sig_handler(int signum) {
    /* if we receive any trapped signal, just set the cleanUpFlag
     * this will break the infinite loop and cause the message
     * buffer to get written out
     */
    cleanUpFlag = 1;
}

static bool cmp_procstat_ident(const procstat &a, const procstat &b) {
    int cmp = 0;
    cmp = strncmp(a.identifier, b.identifier, IDENTIFIER_SIZE);
    if (cmp < 0) return true;
    if (cmp > 0) return false;
    cmp = strncmp(a.subidentifier, b.subidentifier, IDENTIFIER_SIZE);
    if (cmp < 0) return true;
    if (cmp > 0) return false;
    return a.pid < b.pid;
}

static bool cmp_procdata_ident(const procdata &a, const procdata &b) {
    int cmp = 0;
    cmp = strncmp(a.identifier, b.identifier, IDENTIFIER_SIZE);
    if (cmp < 0) return true;
    if (cmp > 0) return false;
    cmp = strncmp(a.subidentifier, b.subidentifier, IDENTIFIER_SIZE);
    if (cmp < 0) return true;
    if (cmp > 0) return false;
    return a.pid < b.pid;
}

static bool cmp_procfd_ident(const procfd &a, const procfd &b) {
    int cmp = 0;
    cmp = strncmp(a.identifier, b.identifier, IDENTIFIER_SIZE);
    if (cmp < 0) return true;
    if (cmp > 0) return false;
    cmp = strncmp(a.subidentifier, b.subidentifier, IDENTIFIER_SIZE);
    if (cmp < 0) return true;
    if (cmp > 0) return false;
    return a.pid < b.pid;
}

ostream& operator<<(ostream& os, const ProcmonConfig& pc) {
    os << "Procmon Configuration:" << endl
        << "\ttargetPPid: " << pc.targetPPid << endl
        << "\tfrequency: " << pc.frequency << endl
        << "\tinitialFrequency: " << pc.initialFrequency << endl
        << "\tinitialPhase: " << pc.initialPhase << endl
        << "\tdaemonize: " << pc.daemonize << endl
        << "\tverbose: " << pc.verbose << endl
        << "\tcraylock: " << pc.craylock << endl
        << "\tmaxIterations: " << pc.getMaxIterations() << endl
        << "\tmaxfd: " << pc.maxfd << endl
#ifdef SECURED
        << "\ttarget_uid: " << pc.target_uid << endl
        << "\ttarget_gid: " << pc.target_gid << endl
        << "\tuser: " << pc.user << endl
        << "\tgroup: " << pc.group << endl
#endif
        << "\tsystem: " << pc.system << endl
        << "\tidentifier: " << pc.identifier << endl
        << "\tsubidentifier: " << pc.subidentifier << endl
        << "\tidentifier_env: " << pc.identifier_env << endl
        << "\tsubidentifier_env: " << pc.subidentifier_env << endl
        << "\tgid_range_min: " << pc.gid_range_min << endl
        << "\tgid_range_max: " << pc.gid_range_max << endl
        << "\ttgtGid: " << pc.tgtGid << endl
        << "\ttgtSid: " << pc.tgtSid << endl
        << "\ttgtPgid: " << pc.tgtPgid << endl
        << "\tclockTicksPerSec: " << pc.clockTicksPerSec << endl
        << "\tpageSize: " << pc.pageSize << endl
        << "\tboottime: " << pc.boottime << endl
        << "\thostname: " << pc.hostname << endl
        << "\toutputFlags: " << pc.outputFlags << endl
        << "\toutputTextFilename: " << pc.outputTextFilename << endl
#ifdef USE_HDF5
        << "\toutputHDF5Filename: " << pc.outputHDF5Filename << endl
#endif
        << "\tnoOutput: " << pc.noOutput << endl
        << "\tpidfile: " << pc.pidfile << endl
#ifdef USE_AMQP
        << "\tmqServer: " << pc.mqServer << endl
        << "\tmqServers:" << endl;
    for (const string &server: pc.mqServers) {
        cout << "\t\t" << server << endl;
    }
    cout << "\tmqPort: " << pc.mqPort << endl
        << "\tmqVHost: " << pc.mqVHost << endl
        << "\tmqUser: " << pc.mqUser << endl
        << "\tmqPassword: " << pc.mqPassword << endl
        << "\tmqExchangeName: " << pc.mqExchangeName << endl
        << "\tmqFrameSize: " << pc.mqFrameSize << endl
    ;
#endif
    return os;
}

int parseProcStat(int pid, procstat* statData, procdata* procData, time_t boottime, long clockTicksPerSec) {
    char *ptr = NULL;
    char *sptr = NULL;
    char *eptr = NULL;
    int idx = 0;
    int rbytes;
    unsigned long long starttime;
    double temp_time;
    char filename[BUFFER_SIZE];
    char lbuffer[LBUFFER_SIZE];
    FILE* fp = NULL;

    snprintf(filename, BUFFER_SIZE, "/proc/%d/stat", pid);
    fp = fopen(filename, "r");
    if (fp == NULL) return -1;

    while (true) {
        if (sptr == NULL || ptr == eptr) {
            rbytes = fileFillBuffer(fp, lbuffer, LBUFFER_SIZE, &sptr, &ptr, &eptr);
            if (rbytes == 0) break;
        }
        if (*ptr == ' ' || *ptr == 0) {
            *ptr = 0;
            switch (idx) {
                case 0:        statData->pid = atoi(sptr);  break;
                case 1:        {
                    int n = ((ptr - 1) - (sptr+1));
                    n = n > EXEBUFFER_SIZE ? EXEBUFFER_SIZE : n;
                    memcpy(procData->execName, sptr+1, n);
                    procData->execName[n] = 0;
                }
                case 2:        statData->state = *sptr; break;
                case 3:        statData->ppid = atoi(sptr); break;
                case 4:        statData->pgrp = atoi(sptr); break;
                case 5:        statData->session = atoi(sptr); break;
                case 6:        statData->tty = atoi(sptr); break;
                case 7:        statData->tpgid = atoi(sptr); break;
                case 8:        statData->flags = atoi(sptr); break;
                //case 9:    statData->minorFaults = strtoul(sptr, &ptr, 10); break;
                //case 10:    statData->cminorFaults = strtoul(sptr, &ptr, 10); break;
                //case 11:    statData->majorFaults = strtoul(sptr, &ptr, 10); break;
                //case 12:    statData->cmajorFaults = strtoul(sptr, &ptr, 10); break;
                case 13:    statData->utime = strtoul(sptr, &ptr, 10); break;
                case 14:    statData->stime = strtoul(sptr, &ptr, 10); break;
                //case 15:    statData->cutime = atol(sptr); break;
                //case 16:    statData->cstime = atol(sptr); break;
                case 17:    statData->priority = atol(sptr); break;
                case 18:    statData->nice = atol(sptr); break;
                case 19:    statData->numThreads = atol(sptr); break;
                //case 20:    statData->itrealvalue = atol(sptr); break;
                case 21:
                    starttime = strtoull(sptr, &ptr, 10);
                    temp_time = boottime + starttime / (double)clockTicksPerSec;
                    statData->startTime = (time_t) floor(temp_time);
                    statData->startTimeUSec = (time_t) floor( (temp_time - statData->startTime) * 1e6);
                    break;
                case 22:    statData->vsize = strtoul(sptr, &ptr, 10); break;
                case 23:    statData->rss = strtoul(sptr, &ptr, 10); break;
                case 24:    statData->rsslim = strtoul(sptr, &ptr, 10); break;
                //case 25:    statData->startcode = strtoul(sptr, &ptr, 10); break;
                //case 26:    statData->endcode = strtoul(sptr, &ptr, 10); break;
                //case 27:    statData->startstack = strtoul(sptr, &ptr, 10); break;
                //case 28:    statData->kstkesp = strtoul(sptr, &ptr, 10); break;
                //case 29:    statData->kstkeip = strtoul(sptr, &ptr, 10); break;
                case 30:    statData->signal = strtoul(sptr, &ptr, 10); break;
                case 31:    statData->blocked = strtoul(sptr, &ptr, 10); break;
                case 32:    statData->sigignore = strtoul(sptr, &ptr, 10); break;
                case 33:    statData->sigcatch = strtoul(sptr, &ptr, 10); break;
                //case 34:    statData->wchan = strtoul(sptr, &ptr, 10); break;
                //case 35:    statData->nswap = strtoul(sptr, &ptr, 10); break;
                //case 36:    statData->cnswap = strtoul(sptr, &ptr, 10); break;
                //case 37:    statData->exitSignal = atoi(sptr); break;
                //case 38:    statData->processor = atoi(sptr); break;
                case 39:    statData->rtPriority = atoi(sptr); break;
                case 40:    statData->policy = atoi(sptr); break;
                case 41:    statData->delayacctBlkIOTicks = strtoull(sptr, &ptr, 10); break;
                case 42:    statData->guestTime = strtoul(sptr, &ptr, 10); break;
                //case 43:    statData->cguestTime = strtoul(sptr, &ptr, 10); break;
            }
            idx++;
            sptr = ptr+1;
        }
        ptr++;
    }
    fclose(fp);
    return 0;
}

int parseProcIO(int pid, procdata* procData, procstat* statData) {
    char *ptr = NULL;
    char *sptr = NULL;
    char *eptr = NULL;
    char* label = NULL;
    int rbytes = 0;
    int stage = 0;  /* 0 = parsing Label; >1 parsing values */
    char filename[BUFFER_SIZE] = "";
    char lbuffer[LBUFFER_SIZE] = "";
    FILE* fp = NULL;

    snprintf(filename, BUFFER_SIZE, "/proc/%d/io", pid);
    fp = fopen(filename, "r");
    if (fp == NULL) return -1;

    while (true) {
        if (sptr == NULL || ptr == eptr) {
            rbytes = fileFillBuffer(fp, lbuffer, LBUFFER_SIZE, &sptr, &ptr, &eptr);
            if (rbytes == 0) break;
        }
        if (stage <= 0) {
            if (*ptr == ':') {
                *ptr = 0;
                label = sptr;
                sptr = ptr + 1;
                stage = 1;
                ptr++;
                continue;
            }
        }
        if (stage > 0) {
            if (*ptr == ' ' || *ptr == '\n' || *ptr == 0) {
                if (*ptr == '\n' || *ptr == 0) {
                    stage = -1;
                }
                *ptr = 0;
                if (ptr != sptr) {
                    /* got a real value here */
                    if (strcmp(label, "rchar") == 0) {
                        statData->io_rchar = strtoull(sptr, &ptr, 10);
                    } else if (strcmp(label, "wchar") == 0) {
                        statData->io_wchar = strtoull(sptr, &ptr, 10);
                    } else if (strcmp(label, "syscr") == 0) {
                        statData->io_syscr = strtoull(sptr, &ptr, 10);
                    } else if (strcmp(label, "syscw") == 0) {
                        statData->io_syscw = strtoull(sptr, &ptr, 10);
                    } else if (strcmp(label, "read_bytes") == 0) {
                        statData->io_readBytes = strtoull(sptr, &ptr, 10);
                    } else if (strcmp(label, "write_bytes") == 0) {
                        statData->io_writeBytes = strtoull(sptr, &ptr, 10);
                    } else if (strcmp(label, "cancelled_write_bytes") == 0) {
                        statData->io_cancelledWriteBytes = strtoull(sptr, &ptr, 10);
                    }
                    stage++;
                }
                sptr = ptr + 1;
            }
        }
        ptr++;
    }
    fclose(fp);
    return 0;
}

int parseNetstat(const char *protocol, unordered_map<size_t,netstat>& data) {
    char *ptr = NULL;
    char *sptr = NULL;
    char *eptr = NULL;
    int rbytes = 0;
    int stage = 0;  /* 0 = parsing header; 1 = parsing data */
    char filename[BUFFER_SIZE];
    char lbuffer[LBUFFER_SIZE] = "";
    FILE* fp = NULL;
    netstat local_data;
    bool uncommitedChange = false;
    memset(&local_data, 0, sizeof(netstat));
    size_t dataCount = data.size();
    int netType = -1;
    snprintf(filename, BUFFER_SIZE, "/proc/net/%s", protocol);
    if (strcmp(protocol, "tcp") == 0) {
        netType = 0;
    } else if (strcmp(protocol, "udp") == 0) {
        netType = 1;
    } else {
        return -1;
    }

    fp = fopen(filename, "r");
    if (fp == NULL) return -1;

    /* read and skip the header line */
    while (true) {
        if (sptr == NULL || ptr == eptr) {
            rbytes = fileFillBuffer(fp, lbuffer, LBUFFER_SIZE, &sptr, &ptr, &eptr);
            if (rbytes == 0) break;
        }
        for ( ; ptr < eptr && *ptr != '\n'; ++ptr) {
            // the logic is the loop
        }
        if (*ptr++ == '\n') {
            break;
        }
    }
    /* parse the body */
    bool lastInvalid = true;
    int fieldCount = 0;
    while (true) {
        if (sptr == NULL || ptr == eptr) {
            rbytes = fileFillBuffer(fp, lbuffer, LBUFFER_SIZE, &sptr, &ptr, &eptr);
            if (rbytes == 0) break;
        }
        for ( ; ptr < eptr; ++ptr) {
            bool newline = *ptr == '\n';
            bool currInvalid = isspace(*ptr) || *ptr == ':' || *ptr == 0;
            if (lastInvalid && currInvalid) {
                // still between tokens
                if (newline) {
                    fieldCount = 0;
                    data[local_data.inode] = local_data;
                    uncommitedChange = false;
                    memset(&local_data, 0, sizeof(netstat));
                }
                continue;
            }
            if (currInvalid) {
                // end of token
                *ptr = 0;
                switch (fieldCount) {
                    case 0: break;
                    case 1:
                        local_data.local_address = strtoul(sptr, &ptr, 16);
                        break;
                    case 2:
                        local_data.local_port = strtoul(sptr, &ptr, 16);
                        break;
                    case 3:
                        local_data.remote_address = strtoul(sptr, &ptr, 16);
                        break;
                    case 4:
                        local_data.remote_port = strtoul(sptr, &ptr, 16);
                        break;
                    case 5:
                        local_data.state = strtoul(sptr, &ptr, 16);
                        break;
                    case 6:
                        local_data.tx_queue = strtoul(sptr, &ptr, 16);
                        break;
                    case 7:
                        local_data.rx_queue = strtoul(sptr, &ptr, 16);
                        break;
                    case 8:
                        local_data.tr = strtoul(sptr, &ptr, 16);
                        break;
                    case 9:
                        local_data.ticks_expire = strtoul(sptr, &ptr, 16);
                        break;
                    case 10:
                        local_data.retransmit = strtoul(sptr, &ptr, 16);
                        break;
                    case 11:
                        local_data.uid = strtoul(sptr, &ptr, 10);
                        break;
                    case 12:
                        local_data.timeout = strtoul(sptr, &ptr, 10);
                        break;
                    case 13:
                        local_data.inode = strtoul(sptr, &ptr, 10);
                        break;
                    case 14:
                        local_data.refCount = strtoul(sptr, &ptr, 10);
                        break;
                }
                if (newline) {
                    fieldCount = 0;
                    data[local_data.inode] = local_data;
                    uncommitedChange = false;
                    memset(&local_data, 0, sizeof(netstat));
                } else {
                    uncommitedChange = true;
                    fieldCount++;
                }
            } else if (lastInvalid) {
                // beginning of token
                sptr = ptr;
            }
            lastInvalid = currInvalid;
        }
    }
    if (uncommitedChange) {
        data[local_data.inode] = local_data;
    }
    fclose(fp);
    return 0;
}

time_t getBootTime() {
    char *ptr = NULL;
    char *sptr = NULL;
    char *eptr = NULL;
    char* label = NULL;
    char lbuffer[LBUFFER_SIZE];
    time_t timestamp;
    int stage = 0;
    FILE* fp = fopen("/proc/stat", "r");
    if (fp == NULL) {
        return 0;
    }

    for ( ; ; ptr++) {
        if (sptr == NULL || ptr == eptr) {
            /* populate the buffer, if the last line was larger than LBUFFER_SIZE,
             * then it can't be parsed by this scheme, and will be abandoned
             * thus, the buffer is reset (sptr == ptr == lbuffer)
             */
            int bytes = fileFillBuffer(fp, lbuffer, LBUFFER_SIZE, &sptr, &ptr, &eptr);
            if (bytes == 0) {
                break;
            }
        }

        if (stage <= 0) {
            if (*ptr == ' ' || *ptr == '\t') {
                *ptr = 0;
                label = sptr;
                sptr = ptr + 1;
                stage = 1;
                continue;
            }
            if (*ptr == '\n') {
                sptr = ptr + 1;
                stage = -1;
                continue;
            }
        }
        if (stage > 0) {
            if (*ptr == ' ' || *ptr == '\t' || *ptr == '\n' || *ptr == 0) {
                if (*ptr == '\n' || *ptr == 0) {
                    stage = -1;
                }
                *ptr = 0;
                if (ptr != sptr) {
                    /* got a real value here */
                    if (strcmp(label, "btime") == 0) {
                        timestamp = (time_t) strtoul(sptr, &ptr, 10);
                    }
                    stage++;
                }
                sptr = ptr + 1;
                continue;
            }
        }
    }
    fclose(fp);
    return timestamp;
}

int parseProcStatM(int pid, procdata* procData, procstat* statData) {
    char *ptr = NULL;
    char *sptr = NULL;
    char *eptr = NULL;
    int idx = 0;
    int rbytes = 0;
    char filename[BUFFER_SIZE];
    char lbuffer[LBUFFER_SIZE];
    FILE* fp = NULL;

    snprintf(filename, BUFFER_SIZE, "/proc/%d/statm", pid);
    fp = fopen(filename, "r");
    if (fp == NULL) return -1;

    while (true) {
        if (sptr == NULL || ptr == eptr) {
            rbytes = fileFillBuffer(fp, lbuffer, LBUFFER_SIZE, &sptr, &ptr, &eptr);
            if (rbytes == 0) break;
        }
        if (*ptr == ' ' || *ptr == '\n' || *ptr == 0) {
            *ptr = 0;
            switch (idx) {
                case 0: statData->m_size = strtoul(sptr, &ptr, 10); break;
                case 1: statData->m_resident = strtoul(sptr, &ptr, 10); break;
                case 2: statData->m_share = strtoul(sptr, &ptr, 10); break;
                case 3: statData->m_text = strtoul(sptr, &ptr, 10); break;
                case 4: break;
                case 5: statData->m_data = strtoul(sptr, &ptr, 10); break;
                case 6: break;
            }
            idx++;
            sptr = ptr + 1;
        }
        ptr++;
    }
    fclose(fp);
    return 0;
}

int parseProcEnvironment(int pid, std::map<std::string, std::string>& env) {
    char *ptr = NULL;
    char *sptr = NULL;
    char *eptr = NULL;
    char *keptr = NULL;
    char *vsptr = NULL;
    int rbytes = 0;
    char filename[BUFFER_SIZE];
    char lbuffer[LBUFFER_SIZE];
    FILE* fp = NULL;

    snprintf(filename, BUFFER_SIZE, "/proc/%d/environ", pid);
    fp = fopen(filename, "r");
    if (fp == NULL) return -1;

    while (true) {
        if (sptr == NULL || ptr == eptr) {
            rbytes = fileFillBuffer(fp, lbuffer, LBUFFER_SIZE, &sptr, &ptr, &eptr);
            if (rbytes == 0) break;
        }
        if (*ptr == 0) {
            *ptr = 0;
            keptr = strchr(sptr, '=');
            if (keptr != NULL) {
                *keptr = 0;
                vsptr = keptr + 1;
                env[std::string(sptr)] = std::string(vsptr);
            }
            sptr = ptr + 1;
        }
        ptr++;
    }
    fclose(fp);
    return 0;
}

int parse_fds(int pid, size_t maxfd, vector<procfd> &all_procfd, size_t *p_idx, procstat *statData) {
    char buffer[BUFFER_SIZE];
    struct stat link;
    int start_idx = *p_idx;
    int rbytes = 0;
    int count = 0;
    DIR *fdDir;
    struct dirent *dptr;
    snprintf(buffer, BUFFER_SIZE, "/proc/%d/fd", pid);
    if ( (fdDir=opendir(buffer)) == NULL) {
        return 0;
    }
    vector<int> seen_fds;
    while( (dptr = readdir(fdDir)) != NULL ) {
        int tgt_fd = atoi(dptr->d_name);
        if (tgt_fd == 0 && dptr->d_name[0] != '0') {
            continue;
        }
        seen_fds.push_back(tgt_fd);
    }
    closedir(fdDir);
    sort(seen_fds.begin(), seen_fds.end());
    for (vector<int>::iterator it = seen_fds.begin(); (it != seen_fds.end()) && (count < maxfd); ++it) {
        int &fd = *it;

        snprintf(buffer, BUFFER_SIZE, "/proc/%d/fd/%d", pid, fd);
        if (lstat(buffer, &link) != 0) {
            continue;
        }
        int idx = (*p_idx)++;
        count++;
        all_procfd[idx].pid = pid;
        all_procfd[idx].ppid = statData->ppid;
        all_procfd[idx].recTime = statData->recTime;
        all_procfd[idx].recTimeUSec = statData->recTimeUSec;
        all_procfd[idx].startTime = statData->startTime;
        all_procfd[idx].startTimeUSec = statData->startTimeUSec;
        all_procfd[idx].fd = fd;
        all_procfd[idx].mode = link.st_mode;
        if ((rbytes = readlink(buffer, all_procfd[idx].path, BUFFER_SIZE)) <= 0) {
            snprintf(all_procfd[idx].path, BUFFER_SIZE, "Unknown");
        } else {
            all_procfd[idx].path[rbytes] = 0;
        }
    }
    return *p_idx - start_idx;
}

int fileFillBuffer(FILE* fp, char* buffer, int buffSize, char** sptr, char** ptr, char** eptr) {
    if (fp == NULL) return 0;
    if (*sptr != NULL) {
        bcopy(buffer, *sptr, sizeof(char)*(*ptr - *sptr));
        *ptr = buffer + (*ptr - *sptr);
        *sptr = buffer;
    } else {
        *sptr = buffer;
        *ptr = buffer;
    }
    int readBytes = fread(*ptr, sizeof(char), buffSize - (*ptr - *sptr), fp);
    *eptr = *ptr + readBytes;
    return readBytes;
}


/*    parseProcStatus
---------------------------
Purpose: Parses the relevant portions of /proc/<pid>/status into a procstat
structure; optionally return list of member gids for GridEngine integration

Arguments:
         pid: pid in /proc to examine
      tgtGid: if a gid for monitoring is known, then that gid should be specified
    statData: procstat datastruct to begin populating

Effects: populates some fields in statData;

Returns: number of Groups matching tgtGid criteria (all groups match if 
tgtGid < 0); -1 for file error error

*/
int parseProcStatus(int pid, int tgtGid, procstat* statData) {
    char *ptr = NULL;
    char *sptr = NULL;
    char *eptr = NULL;
    char filename[BUFFER_SIZE];
    char lbuffer[LBUFFER_SIZE];
    char* label;
    int stage = 0;  /* 0 = parsing Label; >1 parsing values */
    int nextStage = 0;
    int retVal = 0;
    FILE* fp = NULL;
    int rbytes = 0;
    snprintf(filename, BUFFER_SIZE, "/proc/%d/status", pid);
    fp = fopen(filename, "r");
    if (fp == NULL) return -1;

    for ( ; ; ptr++) {
        if (sptr == NULL || ptr == eptr) {
            rbytes = fileFillBuffer(fp, lbuffer, LBUFFER_SIZE, &sptr, &ptr, &eptr);
            if (rbytes == 0) break;
        }
        if (stage <= 0) {
            if (*ptr == ':') {
                *ptr = 0;
                label = sptr;
                sptr = ptr + 1;
                stage = 1;
                continue;
            }
        }
        if (stage > 0) {
            if (*ptr == ' ' || *ptr == '\t' || *ptr == '\n' || *ptr == 0) {
                if (*ptr == '\n' || *ptr == 0) {
                    nextStage = -1;
                } else {
                    nextStage = stage;
                }
                *ptr = 0;
                if (ptr != sptr) {
                    /* got a real value here */
                    /*if (stage == 1 && strcmp(label, "Name") == 0) {
                        snprintf(procData->execName, EXEBUFFER_SIZE, "%s", sptr);
                    } else */
                    if (stage == 1 && strcmp(label, "Pid") == 0) {
                        statData->pid = (unsigned int) strtoul(sptr, &ptr, 10);
                    } else if (stage == 1 && strcmp(label, "PPid") == 0) {
                        statData->ppid = (unsigned int) strtoul(sptr, &ptr, 10);
                    } else if (stage == 1 && strcmp(label, "Uid") == 0) {
                        statData->realUid = strtoul(sptr, &ptr, 10);
                    } else if (stage == 2 && strcmp(label, "Uid") == 0) {
                        statData->effUid = strtoul(sptr, &ptr, 10);
                    } else if (stage == 1 && strcmp(label, "Gid") == 0) {
                        statData->realGid = strtoul(sptr, &ptr, 10);
                    } else if (stage == 2 && strcmp(label, "Gid") == 0) {
                        statData->effGid = strtoul(sptr, &ptr, 10);
                    } else if (stage == 1 && strcmp(label, "VmPeak") == 0) {
                        statData->vmpeak = strtoul(sptr, &ptr, 10);
                    } else if (stage == 1 && strcmp(label, "VmHWM") == 0) {
                        statData->rsspeak = strtoull(sptr, &ptr, 10);
                    } else if (strcmp(label, "Cpus_allowed") == 0) {
                        statData->cpusAllowed = atoi(sptr);
                    } else if (stage > 0 && strcmp(label, "Groups") == 0) {
                        int gid = atoi(sptr);
                        if (tgtGid > 0 && tgtGid == gid) {
                            retVal++;
                        }
                    }
                    nextStage++;
                }
                sptr = ptr + 1;
                stage = nextStage;
            }
        }
    }
    fclose(fp);
    return retVal;
}

int format_socket_connection(char *buffer, size_t len, netstat *net) {
    if (net == NULL || buffer == NULL || len == 0 || net->type > 1 || net->type < 0) return -1;
    const char *labels[] = {"tcp","udp","unknown"};
    unsigned short local_addr[4];
    unsigned short remote_addr[4];
    memset(local_addr, 0, sizeof(short) * 4);
    memset(remote_addr, 0, sizeof(short) * 4);
    size_t addr = net->remote_address;
    for (int i = 0; i < 4; ++i, addr >>= 8) {
        remote_addr[i] = addr & 0xFF;
    }
    addr = net->local_address;
    for (int i = 0; i < 4; ++i, addr >>= 8) {
        local_addr[i] = addr & 0xFF;
    }
    return snprintf(buffer, len, "%s:%u.%u.%u.%u:%u:%u.%u.%u.%u:%u", labels[net->type], local_addr[0], local_addr[1], local_addr[2], local_addr[3], net->local_port, remote_addr[0], remote_addr[1], remote_addr[2], remote_addr[3], net->remote_port);
}


/* on first pass:
 *   1) read /proc/<pid>/stat and
 *   save all contents in-memory
 */
int searchProcFs(ProcmonConfig *config) {
    DIR* procDir;
    struct dirent* dptr;
    char buffer[BUFFER_SIZE];
    FILE* fp;
    int tgt_pid;
    int npids = 0;
    int ntargets = 0;
    int idx = 0;
    int nchange = 0;
    int nNewTargets = ntargets;
    int nstart = 0;
    struct timeval before;
    int foundParent;
    bool procEnv = false;
    int readStatFirst = (config->tgtSid > 0 || config->tgtPgid > 0) ? 1 : 0;

    if (config->identifier_env != "" || config->subidentifier_env != "") {
        procEnv = true;
    }

    if (gettimeofday(&before, NULL) != 0) {
        fprintf(stderr, "FAILED to get time (before)\n");
        return -4;
    }

    if ( (procDir=opendir("/proc")) == NULL) {
        fprintf(stderr, "FAILED to open /proc\n");
        return -3;
    }

    pids.resize(512);

    while( (dptr = readdir(procDir)) != NULL) {
        tgt_pid = atoi(dptr->d_name);
        if (tgt_pid <= 0) {
            continue;
        }
        if (pids.size() <= npids) {
            pids.resize(npids*2,0);
        }
        pids[npids++] = tgt_pid;
    }
    closedir(procDir);
    pids.resize(npids);

    keepflag.clear();
    tmp_procStat.clear();
    tmp_procData.clear();
    keepflag.resize(npids, 0);
    tmp_procStat.resize(npids);

    if (readStatFirst) {
        tmp_procData.resize(npids);
    }

    if (readStatFirst) {
        for (idx = 0; idx < npids; idx++) {
            tgt_pid = pids[idx];    
            parseProcStat(tgt_pid, &(tmp_procStat[idx]), &(tmp_procData[idx]), config->boottime, config->clockTicksPerSec);
        }
    } else {
        for (idx = 0; idx < npids; idx++) {
            tgt_pid = pids[idx];    
            keepflag[idx] = parseProcStatus(tgt_pid, config->tgtGid, &(tmp_procStat[idx]));
        }
    }

    /* === Discover processes of interest === 
     * Phase 1:  find target parent process, and all the processes with the target
     * gid (if applicable).  Mark each process that is found by setting keepflags.
     * store interesting pids in the pids array, and their procstat indices in the
     * indices array */
    int indices[npids];
    pids[0] = config->targetPPid;
    foundParent = 0;
    ntargets = 0;
    for (idx = 0; idx < npids; idx++) {
        if (tmp_procStat[idx].pid == config->targetPPid ||
            keepflag[idx] > 0 ||
            (config->tgtSid > 0 && tmp_procStat[idx].session == config->tgtSid) ||
            (config->tgtPgid > 0 && tmp_procStat[idx].pgrp == config->tgtPgid)
        ) {
            keepflag[idx] = 1;
            pids[ntargets] = tmp_procStat[idx].pid;
            indices[ntargets++] = idx;
        }
        if (tmp_procStat[idx].pid == config->targetPPid) {
            foundParent = 1;
        }
    }
    if (ntargets == 0 || foundParent == 0) {
        return 0;
    }
    /* === Discover processes of interest ===
     * Phase 2: loop through all processes looking to find previously 
     * undiscovered (keepflag == 0) pids which inherit from any of the already-
     * found processes (indices/pids up to ntargets); interate until covergence
     */
    nstart = 0;
    do {
        int innerIdx = 0;
        nchange = 0;
        nNewTargets = ntargets;
        for (idx = 0; idx < npids; idx++) {
            for (innerIdx = nstart; innerIdx < ntargets; innerIdx++) {
                if (tmp_procStat[idx].ppid == pids[innerIdx] && keepflag[idx] == 0) {
                    pids[nNewTargets] = tmp_procStat[idx].pid;
                    keepflag[idx] = 1;
                    indices[nNewTargets] = idx;
                    nNewTargets++;
                    nchange++;
                }
            }
        }
        nstart = ntargets;
        ntargets = nNewTargets;
    } while (nchange > 0);

    if (ntargets > 0) {
        global_procStat.clear();
        global_procData.clear();
        global_procFD.clear();

        global_procStat.resize(ntargets);
        global_procData.resize(ntargets);
        if (config->maxfd > 0) {
            global_procFD.resize(ntargets);
        }
    }
    size_t fdidx = 0;

    /* copy data from tmp_procStat to global_procStat, but in order by idx */
    for (idx = 0; idx < ntargets; idx++) {
        memcpy(&(global_procStat[idx]), &(tmp_procStat[indices[idx]]), sizeof(procstat));
    }
    /* if using session id matching, copy tmp_procData as well */
    if (readStatFirst) {
        for (idx = 0; idx < ntargets; idx++) {
            memcpy(&(global_procData[idx]), &(tmp_procData[indices[idx]]), sizeof(procdata));
        }
    }

    /* for each pid, capture:
     *   io data, stat values, exe, cwd
     */
    for (idx = 0; idx < ntargets; idx++) {
        ssize_t rbytes = 0;
        tgt_pid = pids[idx];
        procstat* statData = &(global_procStat[idx]);
        procdata* temp_procData = &(global_procData[idx]); 

        if (readStatFirst) { 
            /* read status */
            parseProcStatus(tgt_pid, config->tgtGid, statData);
        } else {
            /* read stat */
            parseProcStat(tgt_pid, statData, temp_procData, config->boottime, config->clockTicksPerSec);
        }

        /* populate time records */
        temp_procData->recTime = before.tv_sec;
        temp_procData->recTimeUSec = before.tv_usec;
        statData->recTime = before.tv_sec;
        statData->recTimeUSec = before.tv_usec;
        temp_procData->startTime = statData->startTime;
        temp_procData->startTimeUSec = statData->startTimeUSec;
        temp_procData->pid = statData->pid;
        temp_procData->ppid = statData->ppid;

        statData->recTime = before.tv_sec;
        statData->recTimeUSec = before.tv_usec;
        
        /* read io */
        parseProcIO(tgt_pid, temp_procData, statData);

        /* read statm */
        parseProcStatM(tgt_pid, temp_procData, statData);

        /* fix the units of each field */
        statData->vmpeak *= 1024; // convert from kb to bytes
        statData->rsspeak *= 1024;
        statData->rss *= config->pageSize; // convert from pages to bytes
        statData->m_size *= config->pageSize;
        statData->m_resident *= config->pageSize;
        statData->m_share *= config->pageSize;
        statData->m_text *= config->pageSize;
        statData->m_data *= config->pageSize;

        snprintf(buffer, BUFFER_SIZE, "/proc/%d/exe", tgt_pid);
        if ((rbytes = readlink(buffer, temp_procData->exePath, BUFFER_SIZE)) <= 0) {
            snprintf(temp_procData->exePath, BUFFER_SIZE, "Unknown");
        } else {
            temp_procData->exePath[rbytes] = 0;
        }
        snprintf(buffer, BUFFER_SIZE, "/proc/%d/cwd", tgt_pid);
        if ((rbytes = readlink(buffer, temp_procData->cwdPath, BUFFER_SIZE)) <= 0) {
            snprintf(temp_procData->cwdPath, BUFFER_SIZE, "Unknown");
        } else {
            temp_procData->cwdPath[rbytes] = 0;
        }
        snprintf(buffer, BUFFER_SIZE, "/proc/%d/cmdline", tgt_pid);
        fp = fopen(buffer, "r");
        if (fp != NULL) {
            rbytes = fread(temp_procData->cmdArgs, sizeof(char), BUFFER_SIZE, fp);
            temp_procData->cmdArgBytes = rbytes;
            for (int i = 0; i < rbytes; i++) {
                if (temp_procData->cmdArgs[i] == 0) {
                    temp_procData->cmdArgs[i] = '|';
                }
            }
            /* can set cmdArgs[rbytes-1]=0 since rbytes includes what was the 0 termination before */
            temp_procData->cmdArgs[rbytes-1] = 0;
            fclose(fp);
        } else {
            snprintf(temp_procData->cmdArgs, BUFFER_SIZE, "Unknown");
            temp_procData->cmdArgBytes = 0;
        }

        /* if fd tracking is enabled and there is enough room to store any more
           fd information, then parse it!
        */
        size_t start_fdidx = fdidx;
        if (global_procFD.size() - fdidx < config->maxfd) {
            global_procFD.resize(global_procFD.size() + config->maxfd);
        }
        if (config->maxfd > 0) {
            parse_fds(tgt_pid, config->maxfd, global_procFD, &fdidx, statData);
        }
        std::map<std::string, std::string> env;
        std::map<std::string, std::string>::iterator it;
        std::string my_identifier(config->identifier);
        std::string my_subidentifier(config->subidentifier);

        if (procEnv) {
            parseProcEnvironment(tgt_pid, env);
            if ((it = env.find(config->identifier_env)) != env.end()) {
                int idx = 0;
                my_identifier = (*it).second;
                /* only allow alphanumeric characters */
                for (idx = 0; idx < my_identifier.length(); i++) {
                    if (!std::isalnum(my_identifier[idx])) {
                        break;
                    }
                }
                my_identifier = my_identifier.substr(0, idx);

            }
            if ((it = env.find(config->subidentifier_env)) != env.end()) {
                int idx = 0;
                my_subidentifier = (*it).second;
                /* only allow alphanumeric characters */
                for (idx = 0; idx < my_subidentifier.length(); i++) {
                    if (!std::isalnum(my_subidentifier[idx])) {
                        break;
                    }
                }
                my_subidentifier = my_subidentifier.substr(0, idx);
            }
        }

        snprintf(statData->identifier, IDENTIFIER_SIZE, "%s", my_identifier.c_str());
        snprintf(statData->subidentifier, IDENTIFIER_SIZE, "%s", my_subidentifier.c_str());
        snprintf(temp_procData->identifier, IDENTIFIER_SIZE, "%s", my_identifier.c_str());
        snprintf(temp_procData->subidentifier, IDENTIFIER_SIZE, "%s", my_subidentifier.c_str());
        for (size_t l_fdidx = start_fdidx; l_fdidx < fdidx; l_fdidx++) {
            snprintf(global_procFD[l_fdidx].identifier, IDENTIFIER_SIZE, "%s", my_identifier.c_str());
            snprintf(global_procFD[l_fdidx].subidentifier, IDENTIFIER_SIZE, "%s", my_subidentifier.c_str());
        }
    }
    global_procFD.resize(fdidx);
    search_procfs_count = ntargets;

    /* sort by identifier/subidentifier/pid */
    sort(global_procStat.begin(), global_procStat.end(), cmp_procstat_ident);
    sort(global_procData.begin(), global_procData.end(), cmp_procdata_ident);
    sort(global_procFD.begin(), global_procFD.end(), cmp_procfd_ident);

    // get machine-level stats
    if (global_procFD.size() > 0) {
        global_netstat.clear();
        parseNetstat("tcp", global_netstat);
        parseNetstat("udp", global_netstat);
        for (vector<procfd>::iterator it = global_procFD.begin(); it != global_procFD.end(); ++it) {
            procfd *fd = &*it;
            if (strncmp(fd->path, "socket:[", 8) == 0) {
                char *ptr = fd->path + 8;
                char *eptr = fd->path + strlen(fd->path) - 1; // -1 to consume trailing ']'
                size_t inode = strtoul(ptr, &eptr, 10);
                auto mapSocket = global_netstat.find(inode);
                if (mapSocket != global_netstat.end()) {
                    netstat *net = &(mapSocket->second);
                    format_socket_connection(fd->path, BUFFER_SIZE, net);
                }
            }
        }
    }

    syslog(LOG_DEBUG, "ntargets: %d, ps: %lu, pd: %lu, fd: %lu, netstat: %lu\n", ntargets, global_procStat.size(), global_procData.size(), global_procFD.size(), global_netstat.size());

    return ntargets;
}

static void craylock() {
    int fd = open("/tmp/procmon", O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);
    int lock;
    int rand_pause = 0;
    if (fd < 0) {
        exit(0);
    }
    /*srand(getpid() | time(NULL));
    rand_pause = rand() % 30;
    sleep(rand_pause);*/
    
    lock = flock(fd, LOCK_EX);
    if (lock < 0) {
        exit(0);
    }
}

static void daemonize() {
    pid_t pid, sid;

    if (getppid() == 1) {
        return; // already daemonized
    }
    pid = fork();
    if (pid < 0) {
        exit(1); // failed to fork
    }
    if (pid > 0) {
        exit(0); // this is the parent, so exit
    }
    umask(077);

#ifndef NOSETSID
    sid = setsid();
    if (sid < 0) {
        exit(1);
    }
#endif

    if ((chdir("/")) < 0) {
        exit(1);
    }

    freopen("/dev/null", "r", stdin);
    freopen("/dev/null", "w", stdout);
    freopen("/dev/null", "w", stderr);
}

const string ProcmonConfig::getContext() {
    string context;
    context = hostname + "." + identifier + "." + subidentifier + ".*";
    return context;
}

void ProcmonConfig::setSyslogFacility(const string& _facility) {
    string facility(_facility);
    boost::to_upper(facility);
    int value = -1;
    if (facility == "DAEMON") {
        value = LOG_DAEMON;
    } else if (facility == "USER") {
        value = LOG_USER;
    } else if (facility.substr(0, 5) == "LOCAL") {
        int number = atoi(facility.substr(5).c_str());
        switch (number) {
            case 0: value = LOG_LOCAL0; break;
            case 1: value = LOG_LOCAL1; break;
            case 2: value = LOG_LOCAL2; break;
            case 3: value = LOG_LOCAL3; break;
            case 4: value = LOG_LOCAL4; break;
            case 5: value = LOG_LOCAL5; break;
            case 6: value = LOG_LOCAL6; break;
            case 7: value = LOG_LOCAL7; break;
            default: value = -1; break;
        }
    }
    syslog_facility = value;
}

void ProcmonConfig::setSyslogPriorityMin(const string& _priority) {
    string priority(_priority);
    boost::to_upper(priority);
    int value = -1;
    if (priority == "EMERG") {
        value = LOG_EMERG;
    } else if (priority == "ALERT") {
        value = LOG_ALERT;
    } else if (priority == "CRIT") {
        value = LOG_CRIT;
    } else if (priority == "ERR") {
        value = LOG_ERR;
    } else if (priority == "WARNING") {
        value = LOG_WARNING;
    } else if (priority == "NOTICE") {
        value = LOG_NOTICE;
    } else if (priority == "INFO") {
        value = LOG_INFO;
    } else if (priority == "DEBUG") {
        value = LOG_DEBUG;
    }
    syslog_priority_min = value;
}

void setupSyslog(ProcmonConfig &pc) {
    const int facility = pc.getSyslogFacility();
    if (facility < 0) return;
    int options = LOG_PID;
    if (pc.verbose) {
        options |= LOG_PERROR;
    }
    openlog("procmon", options, facility);

    const int level = pc.getSyslogPriorityMin();
    if (level >= 0) {
        setlogmask(LOG_UPTO(level));
    }
}

#ifdef SECURED
void display_perms_ownership(const char *thread_id) {
    gid_t curr_groups[512];
    uid_t curr_ruid, curr_euid, curr_suid;
    gid_t curr_rgid, curr_egid, curr_sgid;
    int cnt = getgroups(512, curr_groups);
    fprintf(stderr, "[%s] group list: ", thread_id);
    for (int i = 0; i < cnt; i++) {
        fprintf(stderr, "%s%d", i != 0 ? " ," : "", curr_groups[i]);
    }
    fprintf(stderr, "\n");
    if (getresuid(&curr_ruid, &curr_euid, &curr_suid) == 0) {
        fprintf(stderr, "[%s] real_uid: %d, eff_uid: %d, saved_uid: %d\n", thread_id, curr_ruid, curr_euid, curr_suid);
    } else {
        fprintf(stderr, "[%s] WARNING: failed to getresuid()\n", thread_id);
    }
    if (getresgid(&curr_rgid, &curr_egid, &curr_sgid) == 0) {
        fprintf(stderr, "[%s] real_gid: %d, eff_gid: %d, saved_gid: %d\n", thread_id, curr_rgid, curr_egid, curr_sgid);
    }
    cap_t capabilities = cap_get_proc();
    if (capabilities != NULL) {
        char *capstr = cap_to_text(capabilities, NULL);
        if (capstr != NULL) {
            fprintf(stderr, "[%s] Capabilities: %s\n", thread_id, capstr);
            //free(capstr);
        } else {
            fprintf(stderr, "[%s] WARNING: failed to cap_to_text()\n");
        }
        cap_free(capabilities);
    } else {
        fprintf(stderr, "[%s] WARNING: failed to get capabilities\n");
    }
}

bool perform_setuid(ProcmonConfig *config, const char *id) {
    bool dropped_privs = false;
    uid_t tgt_uid = getuid();
    gid_t tgt_gid = getgid();
    if (config->target_uid > 0) {
        tgt_uid = config->target_uid;
    }
    if (config->target_gid <= 0) {
        struct passwd *tgt_user = getpwuid(tgt_uid);
        if (tgt_user != NULL) {
            tgt_gid = tgt_user->pw_gid;
        }
    } else {
        tgt_gid = config->target_gid;
    }

    if (setgroups(1, &tgt_gid) != 0) {
        fprintf(stderr, "[%s] WARNING: Failed to trim groups.  Will continue.\n", id);
    }
    if (setresgid(tgt_gid, tgt_gid, tgt_gid) != 0) {
        fprintf(stderr, "[%s] WARNING: Failed to setresgid.  Will continue.\n", id);
    }
    if (tgt_uid > 0) {
        if (setresuid(tgt_uid, tgt_uid, tgt_uid) == 0) {
            dropped_privs = true;
        } else {
            fprintf(stderr, "[%s] WARNING: FAILED To setresuid.  Will attempt to drop capabilities instead.\n", id);
        }
    }
    return dropped_privs;
}

pthread_mutex_t token_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_barrier_t rbarrier;
static void *reader_thread_start(void *t_config) {
    int retCode = 0;
    int err = 0;
    ProcmonConfig *config = (ProcmonConfig *) t_config;

    if (config->verbose) display_perms_ownership("R, initial");

    /* read current capabilities */
    cap_t capabilities = cap_get_proc();
    if (capabilities == NULL) fatal_error("[R] failed to cap_get_proc()", errno);
    if ((err = cap_clear(capabilities)) != 0) fatal_error("[R] Couldn't clear capabilities data structure.", err);

    /* reset capabilities to just CAP_SYS_PTRACE */
    cap_value_t capability[] = { CAP_SYS_PTRACE };
    if ((err = cap_set_flag(capabilities, CAP_EFFECTIVE, 1, capability, CAP_SET)) != 0)
        fatal_error("[R] Couldn't set capability flags", err);
    if ((err = cap_set_flag(capabilities, CAP_PERMITTED, 1, capability, CAP_SET)) != 0)
        fatal_error("[R] Couldn't set capability flags", err);
    if ((err = cap_set_proc(capabilities)) != 0)
        fatal_error("[R] Couldn't set capbilities, secured procmon must be run as root or with cap_sys_ptrace permitted.", err);
    cap_free(capabilities);

    if (config->verbose) display_perms_ownership("R, after R perms-set");
    /* initial barrier-wait is used to synchronize capability/setuid activities
     * of the secured procmon. Without it there is a race between this thread's
     * cap_set_proc, and the other thread's setresuid */
    if ((err = pthread_barrier_wait(&rbarrier)) == EINVAL) fatal_error("Reader failed to barrier wait", err);
    if ((err = pthread_barrier_wait(&rbarrier)) == EINVAL) fatal_error("Reader failed to barrier wait", err);
    if (config->verbose) display_perms_ownership("R, after W perms-set");

    for ( ; ; ) {
        if ((err = pthread_mutex_lock(&token_lock)) != 0) fatal_error("Reader failed to lock token.", err);
        if ((err = pthread_barrier_wait(&rbarrier)) == EINVAL) fatal_error("Reader failed to barrier wait", err);
        if (cleanUpFlag == 0) {
            retCode = searchProcFs(config);
        }
        if ((err = pthread_mutex_unlock(&token_lock)) != 0) fatal_error("Reader failed to unlock token.", err);
        if ((err = pthread_barrier_wait(&rbarrier)) == EINVAL) fatal_error("Reader failed to barrier wait in late-loop", err);
        if (cleanUpFlag != 0) {
            pthread_exit(NULL);
        }
    }
}
#endif

/* pidfile start-up routine.
   Should be called after daemonizing, but before any privilege reduction.

   Will check if an existing pid file exists, if it does it will read that
   pid file, check to see if that pid is still running.

   If the pid isn't running, then the existing pidfile will be unlinked
  
   If the pid is running, then exit()

   Finally, the results of getpid() will be written into the pidfile. If the
   pid file fails to write, then exit()
*/
void pidfile(const string& pidfilename) {
    if (pidfilename.length() == 0) return;

    FILE *pidfile = NULL;
    char buffer[BUFFER_SIZE];
    pid_t my_pid = getpid();

    /* try to open existing pidfile */
    if ((pidfile = fopen(pidfilename.c_str(), "r")) != NULL) {

        /* try to read the pid */
        int readBytes = fread(buffer, sizeof(char), BUFFER_SIZE, pidfile);
        fclose(pidfile);

        if (readBytes > 0) {
            pid_t pid = atoi(buffer);

            if (pid != 0 && pid != my_pid) {
                struct stat st_stat;
                snprintf(buffer, BUFFER_SIZE, "/proc/%d/status", pid);
                if (stat(buffer, &st_stat) == 0) {
                    /* the process still exists! */
                    fprintf(stderr, "Process %d is still running, exiting.\n", pid);
                    exit(0);
                } else {
                    unlink(pidfilename.c_str());
                }
            }
        }
    }
    if ((pidfile = fopen(pidfilename.c_str(), "w")) != NULL) {
        fprintf(pidfile, "%d\n", my_pid);
        fclose(pidfile);
        return;
    }
    fprintf(stderr, "FAILED to write pidfile %s, exiting.", pidfilename.c_str());
    exit(1);
}

unsigned int write_procstat(ProcIO* output, procstat *data, int count) {
    return output->write_procstat(data, count);
}
unsigned int write_procdata(ProcIO *output, procdata *data, int count) {
    return output->write_procdata(data, count);
}
unsigned int write_procfd(ProcIO *output, procfd *data, int count) {
    return output->write_procfd(data, count);
}
bool set_context(ProcIO *output, const std::string& host, const std::string& identifier, const std::string &subidentifier) {
    return output->set_context(host, identifier, subidentifier);
}

template<typename T>
void writeOutput(ProcmonConfig *config, ProcIO *output, vector<T>& data, unsigned int (*write_data)(ProcIO *, T*, int))  {
    const char *last_ident = NULL;
    const char *last_subident = NULL;
    size_t i = 0;
    int sidx = 0;
    for (i = 0; i < data.size(); i++) {
        T *datum = &(data[i]);
        if (last_ident == NULL || last_subident == NULL
            || strncmp(datum->identifier, last_ident, IDENTIFIER_SIZE) != 0
            || strncmp(datum->subidentifier, last_subident, IDENTIFIER_SIZE) != 0)
        {
            last_ident = datum->identifier;
            last_subident = datum->subidentifier;
            if (i != 0) {
                set_context(output, config->hostname, data[sidx].identifier, data[sidx].subidentifier);
                write_data(output, &(data[sidx]), i - sidx);
            }
            sidx = i;
        }
    }
    if (i > 0) {
        set_context(output, config->hostname, data[sidx].identifier, data[sidx].subidentifier);
        write_data(output, &(data[sidx]), i - sidx);
    }
}

int main(int argc, char** argv) {
    int retCode = 0;
    struct timeval startTime;
    ProcmonConfig *config = new ProcmonConfig();
    config->parseOptions(argc, argv);
    setupSyslog(*config);
    syslog(LOG_NOTICE, "procmon starting with context %s",
            config->getContext().c_str());
    if (config->verbose) {
        const ProcmonConfig &pc = *config;
        cout << pc << endl;
    }
    if (getuid() == 0) {
#ifdef SECURED
        if (config->target_uid <= 0) {
            cerr << "WARNING: Executing (secured) procmon as root; capabilities will be dropped!" << endl;
        }
#else
        cerr << "ERROR: Do not run non-secured procmon with root privileges.  Build with SECURED=1.  Exiting." << endl;
        exit(1);
#endif
    }

    /* initialize global variables */
    cleanUpFlag = 0;

    /* setup signal handlers */
    signal(SIGINT, sig_handler);
    signal(SIGTERM, sig_handler);
    signal(SIGXCPU, sig_handler);
    signal(SIGUSR1, sig_handler);
    signal(SIGUSR2, sig_handler);

    if (config->craylock) {
        craylock();
    }
    if (config->daemonize) {
        daemonize();
    }
    if (config->pidfile.length() > 0) {
        pidfile(config->pidfile);
    }

#ifdef SECURED
    pthread_t reader_thread;
    int err;
    bool dropped_privs = false;
    if ( (err = pthread_barrier_init(&rbarrier, NULL, 2)) != 0) fatal_error("Failed to initialize barrier", err);
    if (pthread_mutex_lock(&token_lock) != 0) {
        /* handle error */
    }
    retCode = pthread_create(&reader_thread, NULL, reader_thread_start, config);
    if (retCode != 0) {
        errno = retCode;
        perror("Failed to start reader thread. Bailing out.");
        exit(1);
    }

    /* if the target_uid or target_gid are > 0, switch users
     * change gid then uid, the barrier is to ensure that the other thread has
     * had time to acquire CAP_SYS_PTRACE, and drop the rest  */
    if ((err = pthread_barrier_wait(&rbarrier)) == EINVAL) fatal_error("Writer failed to barrier wait", err);
    if (config->verbose) display_perms_ownership("W, before W perms-set");

    dropped_privs = perform_setuid(config, "R");

    if (!dropped_privs) {
        cap_t empty = cap_init();
        if (empty == NULL) fatal_error("[W] couldn't cap_init()", errno);
        if ((err = cap_set_proc(empty)) != 0) fatal_error("[W] Couldn't set capbilities.", err);
        dropped_privs = true;
        cap_free(empty);
    }

    if (config->verbose) display_perms_ownership("W, after W perms-set");
    if ((err = pthread_barrier_wait(&rbarrier)) == EINVAL) fatal_error("Writer failed to barrier wait", err);

#endif

    std::vector<ProcIO*> outputMethods;
    if (config->outputFlags & OUTPUT_TYPE_TEXT) {
        ProcIO* out = new ProcTextIO(config->outputTextFilename, FILE_MODE_WRITE);
        out->set_context(config->hostname, config->identifier, config->subidentifier);
        outputMethods.push_back(out);
    }
#ifdef USE_HDF5
    if (config->outputFlags & OUTPUT_TYPE_HDF5) {
        ProcIO* out = new ProcHDF5IO(config->outputHDF5Filename, FILE_MODE_WRITE);
        out->set_context(config->hostname, config->identifier, config->subidentifier);
        outputMethods.push_back(out);
    }
#endif
#ifdef USE_AMQP
    if (config->outputFlags & OUTPUT_TYPE_AMQP) {
        ProcIO* out = new ProcAMQPIO(config->mqServer, config->mqPort, config->mqVHost, config->mqUser, config->mqPassword, config->mqExchangeName, config->mqFrameSize, FILE_MODE_WRITE);
        out->set_context(config->hostname, config->identifier, config->subidentifier);
        outputMethods.push_back(out);
    }
#endif

    if (gettimeofday(&startTime, NULL) != 0) {
        fprintf(stderr, "FAILED to get start time\n");
        return 4;
    }

    if (config->verbose) {
        std::cout << "targetPPid      : " << config->targetPPid << std::endl;
        std::cout << "tgtGid          : " << config->tgtGid << std::endl;
        std::cout << "clockTicksPerSec: " << config->clockTicksPerSec << std::endl;
        std::cout << "boottime        : " << config->boottime << std::endl;
    }

    int iterations = 0;

    for ( ; ; ) {
        struct timeval cycleTime;
        gettimeofday(&cycleTime, NULL);
        retCode = 0;

        /* set the global state variables */
        search_procfs_count = 0;

#ifdef SECURED
        int err = 0;
        if ((err = pthread_mutex_unlock(&token_lock)) != 0) fatal_error("Writer failed to unlock token.", err);
        if ((err = pthread_barrier_wait(&rbarrier)) == EINVAL) fatal_error("Writer failed to barrier wait", err);
        if ((err = pthread_mutex_lock(&token_lock)) != 0) fatal_error("Writer failed to lock token.", err);
        retCode = search_procfs_count;
#else
        if (cleanUpFlag == 0) {
            retCode = searchProcFs(config);
        }
#endif
        if (retCode <= 0) {
            retCode *= -1;
            cleanUpFlag = 1;
        } else  {
            for (std::vector<ProcIO*>::iterator iter = outputMethods.begin(), end = outputMethods.end(); iter != end; iter++) {
                writeOutput(config, *iter, global_procStat, write_procstat);
                writeOutput(config, *iter, global_procData, write_procdata);
                writeOutput(config, *iter, global_procFD, write_procfd);
            }
        }

#ifdef SECURED
        if ((err = pthread_barrier_wait(&rbarrier)) == EINVAL) fatal_error("Writer failed to barrier wait in late-loop", err);
#endif

        if (cleanUpFlag == 0) {
            int sleepInterval = config->frequency;

            if (config->initialPhase > 0) {
                struct timeval currTime;
                double timeDelta;
                if (gettimeofday(&currTime, NULL) == 0) {
                    timeDelta = (currTime.tv_sec - startTime.tv_sec) + (double)((currTime.tv_usec - startTime.tv_usec))*1e-06;
                    if (timeDelta > config->initialPhase) {
                        config->initialPhase = 0;
                    } else {
                        sleepInterval = config->initialFrequency;
                    }
                    timeDelta = (currTime.tv_sec - cycleTime.tv_sec) + (double)((currTime.tv_usec - cycleTime.tv_usec))*1e-06;
                    sleepInterval -= floor(timeDelta);
                }
            }
            sleep(sleepInterval);
        } else {
            break;
        }
        if (++iterations == config->getMaxIterations()) {
            // maxIterations defaults to 0, so look for exact
            // match
            break;
        }
    }
    for (std::vector<ProcIO*>::iterator ptr = outputMethods.begin(), end = outputMethods.end(); ptr != end; ptr++) {
        delete *ptr;
    }
    outputMethods.resize(0);
    if (config->pidfile.length() > 0) {
        unlink(config->pidfile.c_str());
    }
    delete config;
#ifdef SECURED
    pthread_mutex_destroy(&token_lock);
    pthread_barrier_destroy(&rbarrier);
#endif
    exit(retCode);
}
