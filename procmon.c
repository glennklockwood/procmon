/*
 * procmon.c
 *
 *  Created on: Feb 16, 2013
 *      Author: dmj
 */


#include <stdlib.h>
#include <stdio.h>
#include <dirent.h>
#include <unistd.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>

#include "procfmt.h"

void usage(int exitStatus) {
	printf("procmon <ppid>\n");
	exit(exitStatus);
}

int parseProcStat(char *buffer, int bufferLen, procstat* statData) {
	char *ptr, *sptr, *eptr;
	ptr = buffer;
	sptr = buffer;
	eptr = buffer + bufferLen;
	int idx = 0;
	for ( ; ptr != eptr; ptr++) {
		if (*ptr == ' ' || *ptr == 0) {
			*ptr = 0;
			switch (idx) {
				case 0:		statData->pid = atoi(sptr);  break;
				case 1:		snprintf(statData->execName, 257, "%s", sptr); break;
				case 2:		statData->state = *sptr; break;
				case 3:		statData->ppid = atoi(sptr); break;
				case 4:		statData->pgrp = atoi(sptr); break;
				case 5:		statData->session = atoi(sptr); break;
				case 6:		statData->tty = atoi(sptr); break;
				case 7:		statData->tpgid = atoi(sptr); break;
				case 8:		statData->flags = atoi(sptr); break;
				case 9:		statData->minorFaults = strtoul(sptr, &ptr, 10); break;
				case 10:	statData->cminorFaults = strtoul(sptr, &ptr, 10); break;
				case 11:	statData->majorFaults = strtoul(sptr, &ptr, 10); break;
				case 12:	statData->cmajorFaults = strtoul(sptr, &ptr, 10); break;
				case 13:	statData->utime = strtoul(sptr, &ptr, 10); break;
				case 14:	statData->stime = strtoul(sptr, &ptr, 10); break;
				case 15:	statData->cutime = atol(sptr); break;
				case 16:	statData->cstime = atol(sptr); break;
				case 17:	statData->priority = atol(sptr); break;
				case 18:	statData->nice = atol(sptr); break;
				case 19:	statData->numThreads = atol(sptr); break;
				case 20:	statData->itrealvalue = atol(sptr); break;
				case 21:	statData->starttime = strtoull(sptr, &ptr, 10); break;
				case 22:	statData->vsize = strtoul(sptr, &ptr, 10); break;
				case 23:	statData->rss = strtoul(sptr, &ptr, 10); break;
				case 24:	statData->rsslim = strtoul(sptr, &ptr, 10); break;
				case 25:	statData->startcode = strtoul(sptr, &ptr, 10); break;
				case 26:	statData->endcode = strtoul(sptr, &ptr, 10); break;
				case 27:	statData->startstack = strtoul(sptr, &ptr, 10); break;
				case 28:	statData->kstkesp = strtoul(sptr, &ptr, 10); break;
				case 29:	statData->kstkeip = strtoul(sptr, &ptr, 10); break;
				case 30:	statData->signal = strtoul(sptr, &ptr, 10); break;
				case 31:	statData->blocked = strtoul(sptr, &ptr, 10); break;
				case 32:	statData->sigignore = strtoul(sptr, &ptr, 10); break;
				case 33:	statData->sigcatch = strtoul(sptr, &ptr, 10); break;
				case 34:	statData->wchan = strtoul(sptr, &ptr, 10); break;
				case 35:	statData->nswap = strtoul(sptr, &ptr, 10); break;
				case 36:	statData->cnswap = strtoul(sptr, &ptr, 10); break;
				case 37:	statData->exitSignal = atoi(sptr); break;
				case 38:	statData->processor = atoi(sptr); break;
				case 39:	statData->rtPriority = atoi(sptr); break;
				case 40:	statData->policy = atoi(sptr); break;
				case 41:	statData->delayacctBlkIOTicks = strtoull(sptr, &ptr, 10); break;
				case 42:	statData->guestTime = strtoul(sptr, &ptr, 10); break;
				case 43:	statData->cguestTime = strtoul(sptr, &ptr, 10); break;
			}
			idx++;
			sptr = ptr+1;
		}
	}
	return 0;
}

int parseProcIO(char *buffer, int bufferLen, procstat* statData) {
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

int parseProcStatus(char *buffer, int bufferLen, procstat* statData) {
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
					if (stage == 1 && strcmp(label, "VmPeak") == 0) {
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
#define BUFFER_SIZE 1024
#define LBUFFER_SIZE 8192
int searchProcFs(int ppid, long clockTicksPerSec, long pageSize) {
	DIR* procDir;
	struct dirent* dptr;
	char timebuffer[BUFFER_SIZE];
	char buffer[BUFFER_SIZE];
	char lbuffer[LBUFFER_SIZE];
	FILE* fp;
	int tgt_pid;
	size_t rbytes;
	int* pids = (int*) malloc(sizeof(int)*8192);
	int* indices;
	int allocPids = 8192;
	int npids = 0;
	int ntargets = 0;
	int idx = 0;
	int nchange = 0;
	int nNewTargets = ntargets;
	int nstart = 0;
	procstat* procData;
	struct timeval before;
	struct timeval after;
	struct tm datetime;
	double timeDelta;

	if (pids == NULL) {
		fprintf(stderr, "FAILED to allocate memory for procid cache for %d pids (%lu bytes)\n", allocPids, sizeof(int)*allocPids);
		return 1;
	}

	if (gettimeofday(&before, NULL) != 0) {
		fprintf(stderr, "FAILED to get time (before)\n");
		return 2;
	}

	if ( (procDir=opendir("/proc")) == NULL) {
		fprintf(stderr, "FAILED to open /proc\n");
		return 3;
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
				return 1;
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
		return 1;
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

		parseProcStat(lbuffer, rbytes, &(procData[idx]));
		fclose(fp);
	}

	/* explicitly re-using the pids buffer at this point; npids is now only needed
	 * for knowing the limits of procData; now ntargets will hold the limit for
	 * pids */
	pids[0] = ppid;
	ntargets = 1;
	nstart = 0;
	indices = (int*) malloc(sizeof(int)*allocPids);
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
		return 2;
	}

	timeDelta = (after.tv_sec - before.tv_sec) + (double)((after.tv_usec - before.tv_usec))*1e-06;
	localtime_r(&before.tv_sec, &datetime);
	strftime(buffer, BUFFER_SIZE, "%Y-%m-%d %H:%M:%S", &datetime);
	snprintf(timebuffer, BUFFER_SIZE, "%s.%03u,%f", buffer, before.tv_usec/1000, timeDelta);

	/* for each pid, capture:
	 *   io data, peak rss/vmem values, exe, cwd
	 */
	for (idx = 0; idx < ntargets; idx++) {
		ssize_t rbytes = 0;
		procstat* l_procData = &(procData[indices[idx]]);
		
		/* read io */
		snprintf(buffer, BUFFER_SIZE, "/proc/%d/io", pids[idx]);
		fp = fopen(buffer, "r");
		if (fp != NULL) {
			rbytes = fread(lbuffer, sizeof(char), LBUFFER_SIZE, fp);
			if (rbytes > 0) {
				parseProcIO(lbuffer, rbytes, l_procData);
			}
			fclose(fp);
		}

		/* read status */
		snprintf(buffer, BUFFER_SIZE, "/proc/%d/status", pids[idx]);
		fp = fopen(buffer, "r");
		if (fp != NULL) {
			rbytes = fread(lbuffer, sizeof(char), LBUFFER_SIZE, fp);
			if (rbytes > 0) {
				parseProcStatus(lbuffer, rbytes, l_procData);
			}
			fclose(fp);
		}

		/* fix the units of each field */
		l_procData->vmpeak *= 1024; // convert from kb to bytes
		l_procData->rsspeak *= 1024;
		l_procData->rss *= pageSize; // convert from pages to bytes

		/* start writing output */
		printf("%s,%d,%c,%d,%d,%d,%d,%d,%u,%lu,%lu,%lu,%lu,%lu,%lu,%ld,%ld,%ld,%ld,%ld,%ld,%llu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%d,%d,%d,%u,%u,%lu,%lu,%llu,%llu,%llu,%llu,%llu,%llu,%llu,%llu,%ld,%s", timebuffer,l_procData->pid,l_procData->state,l_procData->ppid,l_procData->pgrp,l_procData->session,l_procData->tty,l_procData->tpgid,l_procData->flags,l_procData->minorFaults,l_procData->cminorFaults,l_procData->majorFaults,l_procData->cmajorFaults,l_procData->utime,l_procData->stime,l_procData->cutime,l_procData->cstime,l_procData->priority,l_procData->nice,l_procData->numThreads,l_procData->itrealvalue,l_procData->starttime,l_procData->vsize,l_procData->rss,l_procData->rsslim,l_procData->vmpeak,l_procData->rsspeak,l_procData->startcode,l_procData->endcode,l_procData->startstack,l_procData->kstkesp,l_procData->kstkeip,l_procData->signal,l_procData->blocked,l_procData->sigignore,l_procData->sigcatch,l_procData->wchan,l_procData->nswap,l_procData->cnswap,l_procData->exitSignal,l_procData->processor,l_procData->cpusAllowed,l_procData->rtPriority,l_procData->policy,l_procData->guestTime,l_procData->cguestTime,l_procData->delayacctBlkIOTicks,l_procData->io_rchar,l_procData->io_wchar,l_procData->io_syscr,l_procData->io_syscw,l_procData->io_readBytes,l_procData->io_writeBytes,l_procData->io_cancelledWriteBytes, clockTicksPerSec, l_procData->execName);

		snprintf(buffer, BUFFER_SIZE, "/proc/%d/exe", pids[idx]);
		if ((rbytes = readlink(buffer, lbuffer, LBUFFER_SIZE)) <= 0) {
			snprintf(lbuffer, LBUFFER_SIZE, "Unknown");
		} else {
			lbuffer[rbytes] = 0;
		}
		printf(",%s",lbuffer);
		snprintf(buffer, BUFFER_SIZE, "/proc/%d/cwd", pids[idx]);
		if ((rbytes = readlink(buffer, lbuffer, LBUFFER_SIZE)) <= 0) {
			snprintf(lbuffer, LBUFFER_SIZE, "Unknown");
		} else {
			lbuffer[rbytes] = 0;
		}
		printf(",%s\n",lbuffer);
	}
	free(pids);
	free(indices);
	free(procData);
}

int main(int argc, char** argv) {
	int parentProcessID;
	int retCode = 0;
	long clockTicksPerSec = 0;
	long pageSize = 0;
	if (argc < 2) {
		usage(3);
	}
	parentProcessID = atoi(argv[1]);
	if (parentProcessID == 0) {
		usage(3);
	}
	clockTicksPerSec = sysconf(_SC_CLK_TCK);
	pageSize = sysconf(_SC_PAGESIZE);
	
	/* print header */
	printf("timestamp,timedelta,pid,state,ppid,pgrp,session,tty,ttygid,flags,minorFaults,cminorFaults,majorFaults,cmajorFaults,utimeTicks,stimeTicks,cutimeTicks,cstimeTicks,priority,nice,numThreads,itrealvalue,starttime,vsize,rss,rsslim,vpeak,rsspeak,startcode,endcode,startstack,kesp,keip,signal,blocked,sigignore,sigcatch,wchan,nswap,cnswap,exitSignal,processor,cpusAllowed,rtpriority,policy,guestTimeTicks,cguestTimeTicks,blockIODelayTicks,io_rchar,io_wchar,io_syscr,io_syscw,io_readBytes,io_writeBytes,io_cancelledWriteBytes,ticksPerSec,execName,execPath,cwd\n");
	retCode = searchProcFs(parentProcessID, clockTicksPerSec, pageSize);
	exit(retCode);
}

