#include <algorithm>

ProcessRecord::ProcessRecord() {
    active = false;
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
    return 0;
}

ProcessList::ProcessList(const unsigned time_t &_maxAge): maxAge(_maxAge) {
    add_new_process_list();
}

bool ProcessList::add_new_process_list() {
    ProcessList *new_list = new ProcessList[PROCESSES_PER_LIST];
    if (new_list == NULL) {
        return false;
    }
    processLists.push_back(new_list);

    /* put all new pointers in the unusedProcessQueue vector lowest order
     * lowest order pointers first so that they are used first - this is a true queue! */
    for (int i = 0; i < PROCESSES_PER_LIST; ++i) {
        ProcessList *ptr = &(new_list[i]);
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
        retPtr =  unusedProcessQueue.pop_front();
    }
    if (retPtr == NULL) {
        add_new_process_list();
        if (unusedProcessQueue.size() > 0) {
            retPtr = unusedProcessQueue.pop_front();
        }
    }
    return retPtr;
}

bool ProcessList::find_expired_processes() {
    int nFound = 0;
    time_t currTime = time(NULL);
    vector<pair<ProcessRecord*,ProcessRecord*> > termMemRange;
    for (auto& list: processLists) {
        ProcessRecord *ptr = list;
        ProcessRecord *end = ptr + PROCESSES_PER_LIST;
        termMemRange.push_back({ptr,end});
        while (ptr < end) {
            if (ptr->getAge(currTime) > maxAge) {
                ptr->active = false;
                unusedProcessQueue.push_back(ptr);
            }
            ptr++;
        }
    }
    int nExtraLists = (unusedProcessQueue.size() / PROCESSES_PER_LIST);
    if (nExtraLists > 0) {
        vector<ProcessRecord*> keepRecords;
        excessRecords.reserve((processLists.size() - nExtraLists)*PROCESSES_PER_LIST);
        auto& rangeStartIter = termMemRange.begin();
        rangeStartIter += (termMemRange.size() - nExtraLists);
        for (auto& record: unusedProcessQueue) {
            bool found = false;
            for (auto& range = rangeStartIter;
                 range >= termMemRange.begin() && range != termMemRange.end();
                 ++range)
            {
                if (record >= range->first && range < range->end) {
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
            ProcessRecord *ptr = processLists.pop_back();
            ProcessRecord *end = ptr + PROCESSES_PER_LIST;

            while (ptr < end) {
                if (ptr->active) {
                    ProcessRecord *tgt = unusedProcessQueue.pop_front();
                    if (tgt != NULL) {
                        *tgt = *ptr;
                    }
                }
                ++ptr;
            }
            nExtraLists--;
        }
    }
    return unusedProcessQueue.size() > 0;
}
