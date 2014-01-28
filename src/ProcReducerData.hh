#ifndef __PROC_REDUCER_DATA_
#define __PROC_REDUCER_DATA_

#include "ProcData.hh"
#include "config.h"
#include <vector>
#include <deque>

#define PROCESSES_PER_LIST 50

using namespace std;

int procstatbad(const procstat *a);
int procdatabad(const procdata *a);
int procfdbad(const procfd *a);

int procstatcmp(const procstat& a, const procstat& b);
int procdatacmp(const procdata& a, const procdata& b);
int procfdcmp(const procfd& a, const procfd& b);

typedef struct _SingleProcessRecord {
    bool statSet;
    bool dataSet;
    bool fdSet[REDUCER_MAX_FDS];
    procstat stat;
    procdata data;
    procfd fd[REDUCER_MAX_FDS];
    unsigned int statRecord;
    unsigned int dataRecord;
    unsigned int fdRecord[REDUCER_MAX_FDS];
} SingleProcessRecord;

class ProcessRecord {
    friend class ProcessList;
public:
    ProcessRecord();
    bool operator==(const unsigned int pid) const;
    time_t getAge(const time_t &currTime);
    void expire();
	unsigned int set_procdata(procdata*, bool newRecord);
	unsigned int set_procstat(procstat*, bool newRecord);
    unsigned int set_procfd(procfd*, bool newRecord);
	void set_procdata_id(unsigned int id);
	void set_procstat_id(unsigned int id);
    void set_procfd_id(unsigned int id, procfd*);
private:
    SingleProcessRecord currRecord;
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
    ProcessRecord *new_process_record(ProcessList *spare_deck);
    bool find_expired_processes(ProcessList *spare_deck);
	void expire_all_processes();
	unsigned int get_process_count();
	unsigned int get_process_capacity();
private:
    bool add_new_process_list(ProcessList *spare_deck);

    vector<ProcessRecord*> processLists;
    deque<ProcessRecord*> unusedProcessQueue;
    time_t maxAge;
};

class ReducerInvalidFDException : public exception {
public:
	ReducerInvalidFDException(int attempted_fds) {
        error = "Attempted to load " + to_string(attempted_fds) + " fds - out of bounds! (MAX: " + to_string(REDUCER_MAX_FDS);
	}

	virtual const char* what() const throw() {
		return error.c_str();
	}

	~ReducerInvalidFDException() throw() { }
private:
    string error;
};


#endif
