#include <algorithm>
#include "ProcReducerData.hh"
#include <string.h>
#include <iostream>

using namespace std;

ProcessRecord::ProcessRecord() {
    expire();
}

int procdatacmp(const procdata& a, const procdata& b) {
	if (a.pid != b.pid) return 1;
	if (a.ppid != b.ppid) return 2;
	if (a.startTime != b.startTime) return 3;
	if (a.startTimeUSec != b.startTimeUSec) return 4;
	if (a.cmdArgBytes != b.cmdArgBytes) return 5;
	if (memcmp(a.cmdArgs, b.cmdArgs, a.cmdArgBytes) != 0) return 6;
	if (strcmp(a.exePath, b.exePath) != 0) return 7;
	if (strcmp(a.cwdPath, b.cwdPath) != 0) return 8;
	return 0;
}

int procfdcmp(const procfd& a, const procfd& b) {
    if (a.pid != b.pid) return 1;
	if (a.ppid != b.ppid) return 2;
	if (a.startTime != b.startTime) return 3;
	if (a.startTimeUSec != b.startTimeUSec) return 4;
	if (strcmp(a.path, b.path) != 0) return 5;
	if (a.fd != b.fd) return 6;
	if (a.mode != b.mode) return 7;
    return 0;
}

int procstatcmp(const procstat& a, const procstat& b) {
	if (a.pid != b.pid) return 1;
	if (a.ppid != b.ppid) return 2;
	if (a.state != b.state) return 3;
	if (a.realUid != b.realUid) return 4;
	if (a.effUid != b.effUid) return 5;
	if (a.realGid != b.realGid) return 6;
	if (a.effGid != b.effGid) return 7;
	if (a.utime != b.utime) return 8;
	if (a.stime != b.stime) return 9;
	if (a.priority != b.priority) return 10;
	if (a.vsize != b.vsize) return 11;
	if (a.rss != b.rss) return 12;
	if (a.rsslim != b.rsslim) return 13;
	if (a.delayacctBlkIOTicks != b.delayacctBlkIOTicks) return 14;
	if (a.vmpeak != b.vmpeak) return 15;
	if (a.rsspeak != b.rsspeak) return 16;
	if (a.guestTime != b.guestTime) return 17;
	if (a.io_rchar != b.io_rchar) return 18;
	if (a.io_wchar != b.io_wchar) return 19;
	if (a.io_syscr != b.io_syscr) return 20;
	if (a.io_syscw != b.io_syscw) return 21;
	if (a.io_readBytes != b.io_readBytes) return 22;
	if (a.io_writeBytes != b.io_writeBytes) return 23;
	return 0;
}

bool ProcessRecord::operator==(const unsigned int pid) const {
    /* only looks at currRecord */
    if (!active) {
        return false;
    }
    if (currRecord.dataSet && currRecord.data.pid == pid) {
        return true;
    }
    if (currRecord.statSet && currRecord.stat.pid == pid) {
        return true;
    }
    return false;
}

time_t ProcessRecord::getAge(const time_t &currTime) {
    if (!active) {
        return 0;
    }
    if (currRecord.dataSet) {
        return currTime - currRecord.data.recTime;
    }
    if (currRecord.statSet) {
        return currTime - currRecord.stat.recTime;
    }
    for (int i = 0; i < REDUCER_MAX_FDS; i++) {
        if (currRecord.fdSet[i]) {
            return currTime - currRecord.fd[i].recTime;
        }
    }

    return 0;
}

void ProcessRecord::expire() {
    active = false;
    currRecord.dataSet = false;
    currRecord.statSet = false;
    for (int i = 0; i < REDUCER_MAX_FDS; i++) {
        currRecord.fdSet[i] = false;
    }
}

void ProcessRecord::set_procdata_id(unsigned int id) {
	currRecord.dataRecord = id;
}

void ProcessRecord::set_procstat_id(unsigned int id) {
	currRecord.statRecord = id;
}

void ProcessRecord::set_procfd_id(unsigned int id, procfd *procFD) {
    int effective_fd = procFD->fd - 3;
    if (effective_fd < 0 || effective_fd >= REDUCER_MAX_FDS) {
        throw ReducerInvalidFDException(effective_fd);
    }
    currRecord.fdRecord[effective_fd] = id;
}

unsigned int ProcessRecord::set_procstat(procstat *procStat, bool newRecord) {
	unsigned int recId = 0;
	active = true;
	if (newRecord || !currRecord.statSet) {
		currRecord.statSet = true;
	} else {
		recId = currRecord.statRecord;
		if (procstatcmp(currRecord.stat, *procStat) != 0) {
			recId = 0;
		}
	}
	memcpy(&(currRecord.stat), procStat, sizeof(procstat));
	return recId;
}

unsigned int ProcessRecord::set_procdata(procdata *procData, bool newRecord) {
	unsigned int recId = 0;
	active = true;
	if (newRecord || !currRecord.dataSet) {
		currRecord.dataSet = true;
	} else {
		recId = currRecord.dataRecord;
		if (procdatacmp(currRecord.data, *procData) != 0) {
			recId = 0;
		}
	}
	memcpy(&(currRecord.data), procData, sizeof(procdata));
	return recId;
}

unsigned int ProcessRecord::set_procfd(procfd *procFD, bool newRecord) {
    unsigned int recId = 0;
    active = true;
    int effective_fd = procFD->fd - 3; //never measure 0,1,2
    if (effective_fd < 0 || effective_fd >= REDUCER_MAX_FDS) {
        throw ReducerInvalidFDException(effective_fd);
    }
    if (newRecord || !currRecord.fdSet[effective_fd]) {
        currRecord.fdSet[effective_fd] = true;
    } else {
        recId = currRecord.fdRecord[effective_fd];
        if (procfdcmp(currRecord.fd[effective_fd], *procFD) != 0) {
            recId = 0;
        }
    }
    memcpy(&(currRecord.fd[effective_fd]), procFD, sizeof(procfd));
    return recId;
}

ProcessList::ProcessList(const time_t& _maxAge): maxAge(_maxAge) {
    add_new_process_list();
}

bool ProcessList::add_new_process_list() {
    ProcessRecord *new_list = new ProcessRecord[PROCESSES_PER_LIST];
    memset(new_list, 0, sizeof(ProcessRecord)*PROCESSES_PER_LIST);
    if (new_list == NULL) {
        return false;
    }
    processLists.push_back(new_list);

    /* put all new pointers in the unusedProcessQueue vector lowest order
     * lowest order pointers first so that they are used first - this is a true queue! */
    for (int i = 0; i < PROCESSES_PER_LIST; ++i) {
        ProcessRecord *ptr = &(new_list[i]);
        unusedProcessQueue.push_back(ptr);
    }
    return true;
}

ProcessRecord * ProcessList::find_process_record(const unsigned int pid) {
    for (auto& list: processLists) {
        for (int i = 0; i < PROCESSES_PER_LIST; ++i) {
            if (list[i] == pid) {
                return &(list[i]);
            }
        }
    }
    return NULL;
}

ProcessRecord * ProcessList::new_process_record() {
    ProcessRecord *retPtr = NULL;
    if (unusedProcessQueue.size() > 0) {
		retPtr = unusedProcessQueue[0];
        unusedProcessQueue.pop_front();
    }
    if (retPtr == NULL) {
        add_new_process_list();
        if (unusedProcessQueue.size() > 0) {
			retPtr = unusedProcessQueue[0];
            unusedProcessQueue.pop_front();
        }
    }
    return retPtr;
}

unsigned int ProcessList::get_process_count() {
	unsigned int ret = processLists.size() * PROCESSES_PER_LIST;
    std::cerr << "have " << processLists.size() * PROCESSES_PER_LIST << " possible processes" << std::endl;
	ret -= unusedProcessQueue.size();
    std::cerr << "have " <<  unusedProcessQueue.size() << " unused processes" << std::endl;
	return ret;
}

bool ProcessList::find_expired_processes() {
    int nFound = 0;
    time_t currTime = time(NULL);
    for (auto& list: processLists) {
        ProcessRecord *ptr = list;
        ProcessRecord *end = ptr + PROCESSES_PER_LIST;
        while (ptr < end) {
            if (ptr->getAge(currTime) > maxAge) {
                ptr->expire();
                unusedProcessQueue.push_back(ptr);
            }
            ptr++;
        }
    }
    return unusedProcessQueue.size() > 0;

	/*
    int nExtraLists = (unusedProcessQueue.size() / PROCESSES_PER_LIST);
    if (nExtraLists > 0) {
        vector<ProcessRecord*> keepRecords;
        keepRecords.reserve((processLists.size() - nExtraLists)*PROCESSES_PER_LIST);
        auto rangeStartIter = termMemRange.begin();
        rangeStartIter += (termMemRange.size() - nExtraLists);
        for (auto& record: unusedProcessQueue) {
            bool found = false;
            for (auto& range = rangeStartIter;
                 range >= termMemRange.begin() && range != termMemRange.end();
                 ++range)
            {
				ProcessRecord *sPtr = range->first;
				ProcessRecord *ePtr = range->second;
                if (record >= sPtr && record < ePtr) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                keepRecords.push_back(record);
            }
        }
        // dump everything in the unusedProcessQueue, and re-populate with keepRecords
        unusedProcessQueue.clear();
        copy(keepRecords.begin(), keepRecords.end(), unusedProcessQueue.begin());

        // copy remaining records in the about-to-be-removed lists to empty records
        while (nExtraLists > 0) {
            ProcessRecord *ptr = processLists.back();
			processLists.pop_back();
            ProcessRecord *end = ptr + PROCESSES_PER_LIST;

            while (ptr < end) {
                if (ptr->active) {
                    ProcessRecord *tgt = unusedProcessQueue[0];
					unusedProcessQueue.pop_front();
                    if (tgt != NULL) {
                        *tgt = *ptr;
                    }
                }
                ++ptr;
            }
            nExtraLists--;
        }
    }
	*/
    return unusedProcessQueue.size() > 0;
}

void ProcessList::expire_all_processes() {
	unusedProcessQueue.clear();
    for (auto& list: processLists) {
        ProcessRecord *ptr = list;
        ProcessRecord *end = ptr + PROCESSES_PER_LIST;
        while (ptr < end) {
            ptr->expire();
            unusedProcessQueue.push_back(ptr);
            ptr++;
        }
    }
}
