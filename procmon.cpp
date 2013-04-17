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
#include <sys/time.h>
#include <sys/stat.h>
#include <math.h>

#include "procfmt.hh"

#define DEFAULT_FREQUENCY 120
#define DEFAULT_INITFREQ 1

/* global variables - these are global for signal handling */
int cleanUpFlag;

void sig_handler(int signum) {
	/* if we receive any trapped signal, just set the cleanUpFlag
	 * this will break the infinite loop and cause the message
	 * buffer to get written out
	 */
	cleanUpFlag = 1;
}

void usage(int exitStatus) {
	printf("procmon [-d] [-f <secs>] [-i <secs>] [-if <secs>] [-p <ppid>] [-I <identifier>] -o <outputfile>\n");
	printf("  -d: daemonize\n  -f: steady-state polling frequency\n  -i: duration of initial phase\n  -if: initial-phase polling frequency\n  -p: root of process-tracking hierarchy\n  -I: identifier used in output file (/<hostname>/<identifier>)\n  -o: output file (hdf5)\n\n");
	printf("Output format is almost csv (though text fields may include NULL-termination bytes and delimiters) containing: timestamp,timedelta,pid,state,ppid,pgrp,session,tty,ttygid,flags,utimeTicks,stimeTicks,priority,nice,numThreads,vsize,rss,rsslim,vpeak,rsspeak,signal,blocked,sigignore,sigcatch,cpusAllowed,rtpriority,policy,guestTimeTicks,blockIODelayTicks,io_rchar,io_wchar,io_syscr,io_syscw,io_readBytes,io_writeBytes,io_cancelledWriteBytes,m_size,m_resident,m_share,m_text,m_data,ticksPerSec,realUid,effUid,realGid,effGid\n(2) execNameBytes, execName, cmdArgBytes, cmdArgs, exePathBytes, exePath, cwdPathBytes, cwdPath, recordTime, recordTimeUSec, startTime, startTimeUSec, pid, ppid\n");
	exit(exitStatus);
}

int parseProcStat(char *buffer, int bufferLen, procstat* statData, time_t boottime, long clockTicksPerSec) {
	char *ptr, *sptr, *eptr;
	ptr = buffer;
	sptr = buffer;
	eptr = buffer + bufferLen;
	int idx = 0;
    unsigned long long starttime;
    double temp_time;
	for ( ; ptr != eptr; ptr++) {
		if (*ptr == ' ' || *ptr == 0) {
			*ptr = 0;
			switch (idx) {
				case 0:		statData->pid = atoi(sptr);  break;
				//case 1:		break; // do nothing with execName -- pick it up later from status
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
	}
	return 0;
}

int parseProcIO(char *buffer, int bufferLen, procdata* procData, procstat* statData) {
	char *ptr, *sptr, *eptr;
	char* label;
	int idx = 0;
	int stage = 0;  /* 0 = parsing Label; >1 parsing values */

	ptr = buffer;
	sptr = buffer;
	eptr = buffer + bufferLen;
	for ( ; ptr != eptr; ptr++) {
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
				continue;
			}
		}
	}
	return 0;
}

time_t getBootTime() {
	char *ptr, *sptr, *eptr;
	char* label;
	int idx = 0;
	int rbytes = 0;
	char lbuffer[LBUFFER_SIZE];
	time_t timestamp;
	int stage = 0;
	FILE* fp = fopen("/proc/stat", "r");
	if (fp != NULL) {
		rbytes = fread(lbuffer, sizeof(char), LBUFFER_SIZE, fp);
		fclose(fp);
	} else {
		return 0;
	}

	ptr = lbuffer;
	sptr = lbuffer;
	eptr = lbuffer + rbytes;

	for ( ; ptr != eptr; ptr++) {
		if (stage <= 0) {
			if (*ptr == ' ' || *ptr == '\t') {
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
	return timestamp;
}

int parseProcStatM(char* buffer, int bufferLen, procdata* procData, procstat* statData) {
	char *ptr, *sptr, *eptr;
	int idx = 0;

	ptr = buffer;
	sptr = buffer;
	eptr = buffer + bufferLen;
	for ( ; ptr != eptr; ptr++) {
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
	}
	return 0;
}

int parseProcStatus(char *buffer, int bufferLen, procdata* procData, procstat* statData) {
	char *ptr, *sptr, *eptr;
	char* label;
	int idx = 0;
	int stage = 0;  /* 0 = parsing Label; >1 parsing values */

	ptr = buffer;
	sptr = buffer;
	eptr = buffer + bufferLen;
	for ( ; ptr != eptr; ptr++) {
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
					stage = -1;
				}
				*ptr = 0;
				if (ptr != sptr) {
					/* got a real value here */
                    if (stage == 1 && strcmp(label, "Name") == 0) {
                        snprintf(procData->execName, EXEBUFFER_SIZE, "%s", sptr);
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
					}
					stage++;
				}
				sptr = ptr + 1;
				continue;
			}
		}
	}
	return 0;
}

/* on first pass:
 *   1) read /proc/<pid>/stat and
 *   save all contents in-memory
 */
int searchProcFs(int ppid, long clockTicksPerSec, long pageSize, time_t boottime, ProcFile* output) {
	DIR* procDir;
	struct dirent* dptr;
	char timebuffer[BUFFER_SIZE];
	char buffer[BUFFER_SIZE];
	char lbuffer[LBUFFER_SIZE];
	FILE* fp;
	int tgt_pid;
	size_t rbytes;
	int *pids = (int*) malloc(sizeof(int)*8192);
	int allocPids = 8192;
	int npids = 0;
	int ntargets = 0;
	int idx = 0;
	int nchange = 0;
	int nNewTargets = ntargets;
	int nstart = 0;
	int groupid = 0;
	procstat* procData;
	struct timeval before;
	struct timeval after;
	struct tm datetime;
	double timeDelta;
	int found;

	if (pids == NULL) {
		fprintf(stderr, "FAILED to allocate memory for procid cache for %d pids (%lu bytes)\n", allocPids, sizeof(int)*allocPids);
		return -1;
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
		procstat statData;
		tgt_pid = atoi(dptr->d_name);
		if (tgt_pid <= 0) {
			continue;
		}
		while (npids > allocPids) {
			int talloc = allocPids*2;
			pids = (int*) realloc(pids, sizeof(int)*talloc);
			if (pids == NULL) {
				fprintf(stderr, "FAILED to allocate memory for procid cache for %d pids (%lu bytes)\n", talloc, sizeof(int)*talloc);
				return -1;
			}
			allocPids = talloc;
		}
		pids[npids++] = tgt_pid;
	}
	closedir(procDir);

	procData = (procstat*) malloc(sizeof(procstat) * npids);
	memset(procData, 0, sizeof(procstat)*npids);
	if (procData == NULL) {
		fprintf(stderr, "FAILED to allocate memory for proc stat data for %d pids (%lu bytes)\n", npids, sizeof(procstat)*npids);
		return -1;
	}
	for (idx = 0; idx < npids; idx++) {
		tgt_pid = pids[idx];	
		snprintf(buffer, BUFFER_SIZE, "/proc/%d/stat", tgt_pid);
		fp = fopen(buffer, "r");
		if (fp == NULL) {
			continue;
		}
		rbytes = fread(lbuffer, sizeof(char), LBUFFER_SIZE, fp);
		if (rbytes == 0) {
			continue;
		}

		parseProcStat(lbuffer, rbytes, &(procData[idx]), boottime, clockTicksPerSec);
		fclose(fp);
	}

	/* explicitly re-using the pids buffer at this point; npids is now only needed
	 * for knowing the limits of procData; now ntargets will hold the limit for
	 * pids */
	int indices[allocPids];
	pids[0] = ppid;
	found = 0;
	for (idx = 0; idx < npids; idx++) {
		if (procData[idx].pid == ppid) {
			indices[0] = idx;
			groupid = procData[idx].pgrp;
			found = 1;
			break;
		}
	}
	if (found == 0) {
		return 0;
	}
	ntargets = 1;
	nstart = 0;
	do {
		int innerIdx = 0;
		nchange = 0;
		nNewTargets = ntargets;
		for (idx = 0; idx < npids; idx++) {
			for (innerIdx = nstart; innerIdx < ntargets; innerIdx++) {
				if (procData[idx].ppid == pids[innerIdx]) {
					pids[nNewTargets] = procData[idx].pid;
					indices[nNewTargets] = idx;
					nNewTargets++;
					nchange++;
				}
			}
		}
		nstart = ntargets;
		ntargets = nNewTargets;
	} while (nchange > 0);

	if (gettimeofday(&after, NULL) != 0) {
		fprintf(stderr, "FAILED to get time (before)\n");
		return -4;
	}

	timeDelta = (after.tv_sec - before.tv_sec) + (double)((after.tv_usec - before.tv_usec))*1e-06;
	localtime_r(&before.tv_sec, &datetime);
	strftime(buffer, BUFFER_SIZE, "%Y-%m-%d %H:%M:%S", &datetime);
	snprintf(timebuffer, BUFFER_SIZE, "%s.%03u,%f", buffer, before.tv_usec/1000, timeDelta);

    procstat all_procstat[ntargets];
    procdata all_procdata[ntargets];
    bzero(all_procdata, sizeof(procdata)*ntargets);

	/* for each pid, capture:
	 *   io data, peak rss/vmem values, exe, cwd
	 */
	for (idx = 0; idx < ntargets; idx++) {
		ssize_t rbytes = 0;
        double temp_time = 0;
		procstat* statData = &(procData[indices[idx]]);
        procdata* temp_procData = &(all_procdata[idx]); 

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
		snprintf(buffer, BUFFER_SIZE, "/proc/%d/io", pids[idx]);
		fp = fopen(buffer, "r");
		if (fp != NULL) {
			rbytes = fread(lbuffer, sizeof(char), LBUFFER_SIZE, fp);
			if (rbytes > 0) {
				parseProcIO(lbuffer, rbytes, temp_procData, statData);
			}
			fclose(fp);
		}

		/* read status */
		snprintf(buffer, BUFFER_SIZE, "/proc/%d/status", pids[idx]);
		fp = fopen(buffer, "r");
		if (fp != NULL) {
			rbytes = fread(lbuffer, sizeof(char), LBUFFER_SIZE, fp);
			if (rbytes > 0) {
				parseProcStatus(lbuffer, rbytes, temp_procData, statData);
			}
			fclose(fp);
		}

		/* read statm */
		snprintf(buffer, BUFFER_SIZE, "/proc/%d/statm", pids[idx]);
		fp = fopen(buffer, "r");
		if (fp != NULL) {
			rbytes = fread(lbuffer, sizeof(char), LBUFFER_SIZE, fp);
			if (rbytes > 0) {
				parseProcStatM(lbuffer, rbytes, temp_procData, statData);
			}
			fclose(fp);
		}

		/* fix the units of each field */
		statData->vmpeak *= 1024; // convert from kb to bytes
		statData->rsspeak *= 1024;
		statData->rss *= pageSize; // convert from pages to bytes
		statData->m_size *= pageSize;
		statData->m_resident *= pageSize;
		statData->m_share *= pageSize;
		statData->m_text *= pageSize;
		statData->m_data *= pageSize;

		snprintf(buffer, BUFFER_SIZE, "/proc/%d/exe", pids[idx]);
		if ((rbytes = readlink(buffer, temp_procData->exePath, BUFFER_SIZE)) <= 0) {
			snprintf(temp_procData->exePath, BUFFER_SIZE, "Unknown");
		} else {
			temp_procData->exePath[rbytes] = 0;
		}
		snprintf(buffer, BUFFER_SIZE, "/proc/%d/cwd", pids[idx]);
		if ((rbytes = readlink(buffer, temp_procData->cwdPath, BUFFER_SIZE)) <= 0) {
			snprintf(temp_procData->cwdPath, BUFFER_SIZE, "Unknown");
		} else {
			temp_procData->cwdPath[rbytes] = 0;
		}
        snprintf(buffer, BUFFER_SIZE, "/proc/%d/cmdline", pids[idx]);
        fp = fopen(buffer, "r");
        if (fp != NULL) {
            rbytes = fread(temp_procData->cmdArgs, sizeof(char), BUFFER_SIZE, fp);
            temp_procData->cmdArgBytes = rbytes;
            if (rbytes == BUFFER_SIZE) {
                temp_procData->cmdArgs[rbytes] = 0;
            }
            fclose(fp);
        } else {
            snprintf(temp_procData->cmdArgs, BUFFER_SIZE, "Unknown");
            temp_procData->cmdArgBytes = 0;
        }
        memcpy(&(all_procstat[idx]), statData, sizeof(procstat));
	}

    output->write_procstat(all_procstat, ntargets);
    output->write_procdata(all_procdata, ntargets);

	free(pids);
	free(procData);

	return ntargets;
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
	umask(0);

	sid = setsid();
	if (sid < 0) {
		exit(1);
	}

	if ((chdir("/")) < 0) {
		exit(1);
	}

	freopen("/dev/null", "r", stdin);
	freopen("/dev/null", "w", stdout);
	freopen("/dev/null", "w", stderr);
}

int main(int argc, char** argv) {
	int parentProcessID = 1; //monitor all processes by default
	int retCode = 0;
	int frequency = DEFAULT_FREQUENCY;
	int initialFrequency = DEFAULT_INITFREQ;
	int initialWait = 0;
	long clockTicksPerSec = 0;
	long pageSize = 0;
	int daemon = 0;
	int i = 0;
	struct timeval startTime;
	time_t boottime;
    char outputFilename[BUFFER_SIZE];
    char hostname[BUFFER_SIZE];
    char identifier[BUFFER_SIZE];
    hid_t file_id, hgroup_id, collection_id;
    herr_t status;

	/* initialize global variables */
	cleanUpFlag = 0;

    if (gethostname(hostname, BUFFER_SIZE) != 0) {
        snprintf(hostname, BUFFER_SIZE, "Unknown");
    }

	/* setup signal handlers */
	signal(SIGINT, sig_handler);
	signal(SIGTERM, sig_handler);
	signal(SIGXCPU, sig_handler);
	signal(SIGUSR1, sig_handler);
	signal(SIGUSR2, sig_handler);

	/* parse command line arguments */
	for (i = 1; i < argc; i++) {
		if (strcmp(argv[i], "-d") == 0) {
			daemon = 1;
		}
		if (strcmp(argv[i], "-f") == 0) {
			if (i + 1 >= argc) {
				fprintf(stderr, "Not enough arguments for frequency\n");
				usage(3);
			}
			frequency = atoi(argv[++i]);
			if (frequency <= 0) {
				fprintf(stderr, "Frequency is invalid\n");
				usage(3);
			}
		}
		if (strcmp(argv[i], "-i") == 0) {
			if (i + 1 >= argc) {
				fprintf(stderr, "Not enough arguments for initial freq period\n");
				usage(3);
			}
			initialWait = atoi(argv[++i]);
			if (frequency < 0) {
				fprintf(stderr, "initial freq period is invalid\n");
				usage(3);
			}
		}
		if (strcmp(argv[i], "-if") == 0) {
			if (i + 1 >= argc) {
				fprintf(stderr, "Not enough arguments for initial frequency\n");
				usage(3);
			}
			initialFrequency = atoi(argv[++i]);
			if (frequency <= 0) {
				fprintf(stderr, "initial frequency is invalid\n");
				usage(3);
			}
		}
		if (strcmp(argv[i], "-p") == 0) {
			if (i + 1 >= argc) {
				fprintf(stderr, "Not enough arguments for parent process id\n");
				usage(3);
			}
			parentProcessID = atoi(argv[++i]);
			if (parentProcessID <= 0) {
				fprintf(stderr, "Invalid parent process id\n");
				usage(3);
			}
		}
		if (strcmp(argv[i], "-o") == 0) {
			if (i + 1 >= argc) {
				fprintf(stderr, "Not enough arguments for output file\n");
				usage(3);
			}
			strncpy(outputFilename, argv[++i], BUFFER_SIZE);
		}
        if (strcmp(argv[i], "-I") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "Not enought arguments for identifier\n");
                usage(3);
            }
            strncpy(identifier, argv[++i], BUFFER_SIZE);
        }
	}
	if (strlen(outputFilename) == 0) {
		fprintf(stderr, "output filename has 0 length\n");
		usage(3);
	}
			
	clockTicksPerSec = sysconf(_SC_CLK_TCK);
	pageSize = sysconf(_SC_PAGESIZE);
	boottime = getBootTime();
	
	if (daemon) {
		daemonize();
	}

    ProcFile outputFile(outputFilename, hostname, identifier, FILE_FORMAT_TEXT, FILE_MODE_WRITE);

	if (gettimeofday(&startTime, NULL) != 0) {
		fprintf(stderr, "FAILED to get start time\n");
		return 4;
	}

	while (cleanUpFlag == 0) {
		retCode = searchProcFs(parentProcessID, clockTicksPerSec, pageSize, boottime, &outputFile);
		if (retCode <= 0) {
			exit(-1*retCode);
		}
		if (cleanUpFlag == 0) {
			int sleepInterval = frequency;
			if (initialWait > 0) {
				struct timeval currTime;
				double timeDelta;
				if (gettimeofday(&currTime, NULL) == 0) {
					timeDelta = (currTime.tv_sec - startTime.tv_sec) + (double)((currTime.tv_usec - startTime.tv_usec))*1e-06;
					if (timeDelta > initialWait) {
						initialWait = 0;
					} else {
						sleepInterval = initialFrequency;
					}
				}
			}
			sleep(sleepInterval);
		}
	}

	exit(0);
}


