/*
 * procmon.c
 *
 * Author: Douglas Jacobsen <dmjacobsen@lbl.gov>, NERSC User Services Group
 * 2013/02/17
 * Copyright (C) 2012, The Regents of the University of California
 *
 * The purpose of the procmon is to read data from /proc for an entire process tree
 * and save that data at intervals longitudinally
 */


#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
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

#ifdef SECURED
#include <pthread.h>
#include <sys/capability.h>
#include <sys/prctl.h>
#include <errno.h>
#endif

#include "ProcData.hh"
#include "ProcIO.hh"
#include "procmon.hh"

template <class T>
class mempool {
public:
    mempool(size_t init_capacity = 128) {
        data = NULL;
        count = 0;
        capacity = 0;
        if (resize(init_capacity) != 0) {
            zero();
        }
    }
    ~mempool() {
        if (data != NULL) {
            free(data);
        }
        count = 0;
        capacity = 0;
        data = NULL;
    }
    size_t resize(size_t new_capacity) {
        size_t talloc = new_capacity > 511 ? new_capacity * 2 : 512;
        data = (T*) realloc(data, sizeof(T) * talloc);
        if (data == NULL) {
            fprintf(stderr, "FAILED to allocate memory for %d items (%lu bytes)\n", talloc, sizeof(T) * talloc);
            capacity = 0;
            count = 0;
            return 0;
        }
        memset(&(data[capacity]), 0, sizeof(T)*(talloc-capacity));
        capacity = talloc;
        return capacity;
    }

    inline size_t check_capacity(size_t t_capacity) {
        if (data == NULL || t_capacity > capacity) {
            resize(t_capacity);
        }
        return capacity;
    }

    void zero() {
        bzero(data, sizeof(T) * capacity);
        count = 0;
    }

    T *data;
    size_t count;
    size_t capacity;
};

inline void fatal_error(const char *error, int err) {
    fprintf(stderr, "Failed: %s; %d; bailing out.\n", error, err);
    exit(1);
}

/* global variables - these are global for signal handling, and inter-thread communication */
int cleanUpFlag = 0;
int search_procfs_count = 0;
mempool<int> pids;
mempool<int> keepflag;
mempool<procstat> tmp_procStat;
mempool<procdata> tmp_procData;
mempool<procstat> global_procStat;
mempool<procdata> global_procData;
mempool<procfd>   global_procFD;

void sig_handler(int signum) {
	/* if we receive any trapped signal, just set the cleanUpFlag
	 * this will break the infinite loop and cause the message
	 * buffer to get written out
	 */
	cleanUpFlag = 1;
}

static int cmp_procstat_ident(const void *p1, const void *p2) {
    procstat *a = (procstat *) p1;
    procstat *b = (procstat *) p2;
    int cmp = 0;
    if ((cmp = strncmp(a->identifier, b->identifier, IDENTIFIER_SIZE)) != 0) {
        return cmp;
    }
    if ((cmp = strncmp(a->subidentifier, b->subidentifier, IDENTIFIER_SIZE)) != 0) {
        return cmp;
    }
    return a->pid - b->pid;
}

static int cmp_procdata_ident(const void *p1, const void *p2) {
    procdata *a = (procdata *) p1;
    procdata *b = (procdata *) p2;
    int cmp = 0;
    if ((cmp = strncmp(a->identifier, b->identifier, IDENTIFIER_SIZE)) != 0) {
        return cmp;
    }
    if ((cmp = strncmp(a->subidentifier, b->subidentifier, IDENTIFIER_SIZE)) != 0) {
        return cmp;
    }
    return a->pid - b->pid;
}

static int cmp_procfd_ident(const void *p1, const void *p2) {
    procfd *a = (procfd *) p1;
    procfd *b = (procfd *) p2;
    int cmp = 0;
    if ((cmp = strncmp(a->identifier, b->identifier, IDENTIFIER_SIZE)) != 0) {
        return cmp;
    }
    if ((cmp = strncmp(a->subidentifier, b->subidentifier, IDENTIFIER_SIZE)) != 0) {
        return cmp;
    }
    return a->pid - b->pid;
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
				case 0:		statData->pid = atoi(sptr);  break;
				case 1:		{
                    int n = ((ptr - 1) - (sptr+1));
                    n = n > EXEBUFFER_SIZE ? EXEBUFFER_SIZE : n;
                    memcpy(procData->execName, sptr+1, n);
                    procData->execName[n] = 0;
                }
				case 2:		statData->state = *sptr; break;
				case 3:		statData->ppid = atoi(sptr); break;
				case 4:		statData->pgrp = atoi(sptr); break;
				case 5:		statData->session = atoi(sptr); break;
				case 6:		statData->tty = atoi(sptr); break;
				case 7:		statData->tpgid = atoi(sptr); break;
				case 8:		statData->flags = atoi(sptr); break;
				//case 9:	statData->minorFaults = strtoul(sptr, &ptr, 10); break;
				//case 10:	statData->cminorFaults = strtoul(sptr, &ptr, 10); break;
				//case 11:	statData->majorFaults = strtoul(sptr, &ptr, 10); break;
				//case 12:	statData->cmajorFaults = strtoul(sptr, &ptr, 10); break;
				case 13:	statData->utime = strtoul(sptr, &ptr, 10); break;
				case 14:	statData->stime = strtoul(sptr, &ptr, 10); break;
				//case 15:	statData->cutime = atol(sptr); break;
				//case 16:	statData->cstime = atol(sptr); break;
				case 17:	statData->priority = atol(sptr); break;
				case 18:	statData->nice = atol(sptr); break;
				case 19:	statData->numThreads = atol(sptr); break;
				//case 20:	statData->itrealvalue = atol(sptr); break;
				case 21:
                    starttime = strtoull(sptr, &ptr, 10);
                    temp_time = boottime + starttime / (double)clockTicksPerSec;
                    statData->startTime = (time_t) floor(temp_time);
                    statData->startTimeUSec = (time_t) floor( (temp_time - statData->startTime) * 1e6);
                    break;
				case 22:	statData->vsize = strtoul(sptr, &ptr, 10); break;
				case 23:	statData->rss = strtoul(sptr, &ptr, 10); break;
				case 24:	statData->rsslim = strtoul(sptr, &ptr, 10); break;
				//case 25:	statData->startcode = strtoul(sptr, &ptr, 10); break;
				//case 26:	statData->endcode = strtoul(sptr, &ptr, 10); break;
				//case 27:	statData->startstack = strtoul(sptr, &ptr, 10); break;
				//case 28:	statData->kstkesp = strtoul(sptr, &ptr, 10); break;
				//case 29:	statData->kstkeip = strtoul(sptr, &ptr, 10); break;
				case 30:	statData->signal = strtoul(sptr, &ptr, 10); break;
				case 31:	statData->blocked = strtoul(sptr, &ptr, 10); break;
				case 32:	statData->sigignore = strtoul(sptr, &ptr, 10); break;
				case 33:	statData->sigcatch = strtoul(sptr, &ptr, 10); break;
				//case 34:	statData->wchan = strtoul(sptr, &ptr, 10); break;
				//case 35:	statData->nswap = strtoul(sptr, &ptr, 10); break;
				//case 36:	statData->cnswap = strtoul(sptr, &ptr, 10); break;
				//case 37:	statData->exitSignal = atoi(sptr); break;
				//case 38:	statData->processor = atoi(sptr); break;
				case 39:	statData->rtPriority = atoi(sptr); break;
				case 40:	statData->policy = atoi(sptr); break;
				case 41:	statData->delayacctBlkIOTicks = strtoull(sptr, &ptr, 10); break;
				case 42:	statData->guestTime = strtoul(sptr, &ptr, 10); break;
				//case 43:	statData->cguestTime = strtoul(sptr, &ptr, 10); break;
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

int parse_fds(int pid, int maxfd, procfd *all_procfd, int *p_idx, procstat *statData) {
    char buffer[BUFFER_SIZE];
    struct stat link;
    int start_idx = *p_idx;
    int rbytes = 0;
    for (int fd = 3; fd < maxfd+3; fd++) {
		snprintf(buffer, BUFFER_SIZE, "/proc/%d/fd/%d", pid, fd);
        if (lstat(buffer, &link) != 0) {
            break;
        }
        int idx = (*p_idx)++;
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

    if (config->check_mpi != NULL || config->identifier_env != NULL || config->subidentifier_env != NULL) {
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

	while( (dptr = readdir(procDir)) != NULL) {
		tgt_pid = atoi(dptr->d_name);
		if (tgt_pid <= 0) {
			continue;
		}
        if (pids.check_capacity(npids) == 0) {
            return -1;
        }
		pids.data[npids++] = tgt_pid;
	}
	closedir(procDir);

    if (keepflag.check_capacity(npids) == 0) {
        return -1;
    }  
    if (tmp_procStat.check_capacity(npids) == 0) {
        return -1;
    }
    if (readStatFirst) {
        if (tmp_procData.check_capacity(npids) == 0) {
            return -1;
        }
        tmp_procData.zero();
    }
    procstat *procStats = tmp_procStat.data;
    procdata  *procDatas = tmp_procData.data;
    keepflag.zero();
    tmp_procStat.zero();

    if (readStatFirst) {
        for (idx = 0; idx < npids; idx++) {
            tgt_pid = pids.data[idx];	
		    parseProcStat(tgt_pid, &(procStats[idx]), &(procDatas[idx]), config->boottime, config->clockTicksPerSec);
        }
    } else {
        for (idx = 0; idx < npids; idx++) {
            tgt_pid = pids.data[idx];	
            keepflag.data[idx] = parseProcStatus(tgt_pid, config->tgtGid, &(procStats[idx]));
        }
    }

	/* === Discover processes of interest === 
	 * Phase 1:  find target parent process, and all the processes with the target
	 * gid (if applicable).  Mark each process that is found by setting keepflags.
	 * store interesting pids in the pids array, and their procstat indices in the
	 * indices array */
	int indices[npids];
	pids.data[0] = config->targetPPid;
	foundParent = 0;
    ntargets = 0;
	for (idx = 0; idx < npids; idx++) {
		if (procStats[idx].pid == config->targetPPid ||
            keepflag.data[idx] > 0 ||
            (config->tgtSid > 0 && procStats[idx].session == config->tgtSid) ||
            (config->tgtPgid > 0 && procStats[idx].pgrp == config->tgtPgid)
        ) {
			keepflag.data[idx] = 1;
			pids.data[ntargets] = procStats[idx].pid;
			indices[ntargets++] = idx;
		}
		if (procStats[idx].pid == config->targetPPid) {
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
				if (procStats[idx].ppid == pids.data[innerIdx] && keepflag.data[idx] == 0) {
					pids.data[nNewTargets] = procStats[idx].pid;
					keepflag.data[idx] = 1;
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
        if (global_procStat.check_capacity(ntargets) == 0) {
            fprintf(stderr, "Failed to allocate memory; exiting...\n");
            exit(1);
        }
            
        if (global_procData.check_capacity(ntargets) == 0) {
            fprintf(stderr, "Failed to allocate memory; exiting...\n");
            exit(1);
        }
        if (config->maxfd > 0) {
            if (global_procFD.check_capacity(ntargets * config->maxfd) == 0) {
                fprintf(stderr, "Failed to allocate memory; exiting...\n");
                exit(1);
            }
            global_procFD.zero();
        }
        global_procStat.zero();
        global_procData.zero();
    }
    int fdidx = 0;

    /* copy data from procStats to global_procStat, but in order by idx */
    for (idx = 0; idx < ntargets; idx++) {
        memcpy(&(global_procStat.data[idx]), &(procStats[indices[idx]]), sizeof(procstat));
    }
    /* if using session id matching, copy procDatas as well */
    if (readStatFirst) {
        for (idx = 0; idx < ntargets; idx++) {
            memcpy(&(global_procData.data[idx]), &(procDatas[indices[idx]]), sizeof(procdata));
        }
    }

	/* for each pid, capture:
	 *   io data, stat values, exe, cwd
	 */
	for (idx = 0; idx < ntargets; idx++) {
		ssize_t rbytes = 0;
        tgt_pid = pids.data[idx];
		procstat* statData = &(global_procStat.data[idx]);
        procdata* temp_procData = &(global_procData.data[idx]); 

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
        int start_fdidx = fdidx;
        int n_fd = global_procFD.capacity - fdidx;
        if (config->maxfd < n_fd) {
            n_fd = config->maxfd;
        }

        if (n_fd > 0) {
            parse_fds(tgt_pid, n_fd, global_procFD.data, &fdidx, statData);
        }
        std::map<std::string, std::string> env;
        std::map<std::string, std::string>::iterator it;
        std::string my_identifier(config->identifier);
        std::string my_subidentifier(config->subidentifier);

        if (procEnv) {
            parseProcEnvironment(tgt_pid, env);
        }

        if (config->identifier_env != NULL && (it = env.find(config->identifier_env)) != env.end()) {
            my_identifier = (*it).second;
        }
        if (config->subidentifier_env != NULL && (it = env.find(config->subidentifier_env)) != env.end()) {
            my_subidentifier = (*it).second;
        }
        if (config->check_mpi != NULL) {
            int mpi_rank = -1;
            if ((it = env.find(config->check_mpi)) != env.end()) {
                const char *rank_str = (*it).second.c_str();
                mpi_rank = atoi(rank_str);
            }
            statData->rtPriority = mpi_rank;
        }

        snprintf(statData->identifier, IDENTIFIER_SIZE, "%s", my_identifier.c_str());
        snprintf(statData->subidentifier, IDENTIFIER_SIZE, "%s", my_subidentifier.c_str());
        snprintf(temp_procData->identifier, IDENTIFIER_SIZE, "%s", my_identifier.c_str());
        snprintf(temp_procData->subidentifier, IDENTIFIER_SIZE, "%s", my_subidentifier.c_str());
        for (int l_fdidx = start_fdidx; l_fdidx < fdidx; l_fdidx++) {
            snprintf(global_procFD.data[l_fdidx].identifier, IDENTIFIER_SIZE, "%s", my_identifier.c_str());
            snprintf(global_procFD.data[l_fdidx].subidentifier, IDENTIFIER_SIZE, "%s", my_subidentifier.c_str());
        }
	}

    /* save data in global space */
    global_procStat.count = ntargets;
    global_procData.count = ntargets;
    global_procFD.count = fdidx;
    search_procfs_count = ntargets;

    /* sort by identifier/subidentifier/pid */
    qsort(global_procStat.data, global_procStat.count, sizeof(procstat), cmp_procstat_ident);
    qsort(global_procData.data, global_procData.count, sizeof(procdata), cmp_procdata_ident);
    qsort(global_procFD.data, global_procFD.count, sizeof(procfd), cmp_procfd_ident);

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
void writeOutput(ProcmonConfig *config, ProcIO *output, T *data, int count, unsigned int (*write_data)(ProcIO *, T*, int))  {
    const char *last_ident = NULL;
    const char *last_subident = NULL;
    int sidx = 0;
    int i = 0;
    for (i = 0; i < count; i++) {
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
    ProcmonConfig *config = new ProcmonConfig(argc, argv);
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

    dropped_privs = perform_setuid("R");

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
        ProcIO* out = new ProcHDF5IO(config->outputTextFilename, FILE_MODE_WRITE);
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
                /*
                const char *last_ident = NULL;
                const char *last_subident = NULL;
                int sidx = 0;
                int i = 0;
                for (i = 0; i < global_procStat.count; i++) {
                    procstat *temp_ps = &(global_procStat.data[i]);
                    if (last_ident == NULL || last_subident == NULL
                        || strncmp(temp_ps->identifier, last_ident, IDENTIFIER_SIZE) != 0
                        || strncmp(temp_ps->subidentifier, last_subident, IDENTIFIER_SIZE) != 0)
                    {
                        last_ident = temp_ps->identifier;
                        last_subident = temp_ps->subidentifier;
                        if (i != 0) {
                            (*iter)->set_context(config->hostname, global_procStat.data[sidx].identifier, global_procStat.data[sidx].subidentifier);
                            (*iter)->write_procstat(&(global_procStat.data[sidx]), i - sidx);
                        }
                        sidx = i;
                    }
                }
                if (i > 0) {
                    (*iter)->set_context(config->hostname, global_procStat.data[sidx].identifier, global_procStat.data[sidx].subidentifier);
                    (*iter)->write_procstat(&(global_procStat.data[sidx]), i - sidx);
                }

                if (global_procData.count > 0) {
                    (*iter)->write_procdata(global_procData.data, global_procData.count);
                }
                if (global_procFD.count > 0) {
                    (*iter)->write_procfd(global_procFD.data, global_procFD.count);
                }
                */
                writeOutput(config, *iter, global_procStat.data, global_procStat.count, write_procstat);
                writeOutput(config, *iter, global_procData.data, global_procData.count, write_procdata);
                writeOutput(config, *iter, global_procFD.data, global_procFD.count, write_procfd);
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
	}
    for (std::vector<ProcIO*>::iterator ptr = outputMethods.begin(), end = outputMethods.end(); ptr != end; ptr++) {
        delete *ptr;
    }
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
