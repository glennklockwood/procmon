/*
 * procfmt.h
 *
 * Author: Douglas Jacobsen <dmjacobsen@lbl.gov>, NERSC User Services Group
 * 2013/02/17
 * Copyright (C) 2012, The Regents of the University of California
 *
 * The purpose of the procmon is to read data from /proc for an entire process tree
 * and save that data at intervals longitudinally
 */

#ifndef PROCFMT_H_
#define PROCFMT_H_

typedef struct _procstat {
	int pid;
	char execName[258];
	char state;
	int ppid;
	int pgrp;
	int session;
	int tty;
	int tpgid;
	unsigned int flags;
	unsigned long minorFaults;
	unsigned long cminorFaults;
	unsigned long majorFaults;
	unsigned long cmajorFaults;
	unsigned long utime;
	unsigned long stime;
	long cutime;
	long cstime;
	long priority;
	long nice;
	long numThreads;
	long itrealvalue; /* likely zero in all modern kernels */
	unsigned long long starttime;
	unsigned long vsize; /* virtual mem in bytes */
	unsigned long rss;   /* number of pages in physical memory */
	unsigned long rsslim;/* limit of rss bytes */
	unsigned long startcode;
	unsigned long endcode;
	unsigned long startstack;
	unsigned long kstkesp;
	unsigned long kstkeip;
	unsigned long signal;
	unsigned long blocked;
	unsigned long sigignore;
	unsigned long sigcatch;
	unsigned long wchan;
	unsigned long nswap;
	unsigned long cnswap;
	int exitSignal;
	int processor;
	unsigned int rtPriority;
	unsigned int policy;
	unsigned long long delayacctBlkIOTicks;
	unsigned long guestTime;
	unsigned long cguestTime;

	/* fields from /proc/[pid]/status */
	unsigned long vmpeak;  /* kB */
	unsigned long rsspeak; /* kB */
	int cpusAllowed;

	/* fields from /proc/[pid]/io */
	unsigned long long io_rchar;
	unsigned long long io_wchar;
	unsigned long long io_syscr;
	unsigned long long io_syscw;
	unsigned long long io_readBytes;
	unsigned long long io_writeBytes;
	unsigned long long io_cancelledWriteBytes;

	/* fields from /proc/[pid]/statm */
	unsigned long m_size;
	unsigned long m_resident;
	unsigned long m_share;
	unsigned long m_text;
	unsigned long m_data;
} procstat;


#endif /* PROCFMT_H_ */
