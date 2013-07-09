#ifndef __PROC_REDUCER_DATA_
#define __PROC_REDUCER_DATA_

#include "ProcData.hh"
#include <vector>
#include <deque>

#define PROCESSES_PER_LIST 1200

using namespace std;

typedef struct _SingleProcessRecord {
    bool statSet;
    bool dataSet;
    procstat stat;
    procdata data;
    unsigned int statRecord;
    unsigned int dataRecord;
} SingleProcessRecord;

class ProcessRecord {
    friend class ProcessList;
public:
    ProcessRecord();
    bool operator==(const unsigned int pid) const;
    time_t getAge(const time_t &currTime);
	unsigned int set_procdata(procdata*, bool newRecord);
	unsigned int set_procstat(procstat*, bool newRecord);
	void set_procdata_id(unsigned int id);
	void set_procstat_id(unsigned int id);
private:
    SingleProcessRecord currRecord;
    SingleProcessRecord prevRecord;
    bool active;
	int nData;
	int nStat;
};

/* ProcessList class
 *   manage large blocks of processes to conserve memory by preventing many
 *   small allocations as new processes arrive; blocks of 800 ProcessRecords
 *   will be allocated (about 3.2M)
 */
class ProcessList {
public:
    ProcessList(const time_t &maxAge);
    ProcessRecord *find_process_record(const unsigned int pid);
    ProcessRecord *new_process_record();
    bool find_expired_processes();
	void expire_all_processes();
	unsigned int get_process_count();
private:
    bool add_new_process_list();

    vector<ProcessRecord*> processLists;
    deque<ProcessRecord*> unusedProcessQueue;
    time_t maxAge;
};


#endif
