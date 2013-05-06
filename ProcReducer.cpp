#include "config.h"
#include "ProcData.hh"
#include "ProcIO.hh"
#include <signal.h>
#include <string.h>
#include <iostream>
#include <map>

/* ProcReducer
    * Takes in procmon data from all sources and only writes the minimal record
    * Writes an HDF5 file for all procdata foreach day
*/

/* TODO: write FILE_MODE_READ setup for ProcAMPQIO */

constexpr int cleanFreq = 600;
constexpr int cleanAge = 3600;

/* global variables - these are global for signal handling */
int cleanUpExitFlag = 0;
int resetOutputFileFlag = 0;

void sig_handler(int signum) {
	/* if we receive any trapped signal, just set the cleanUpFlag
     * this will break the infinite loop and cause the message
     * buffer to get written out
     */
     cleanUpExitFlag = 1;
}

void sighup_handler(int signum) {
	/* if we receive any trapped signal, just set the cleanUpFlag
     * this will break the infinite loop and cause the message
     * buffer to get written out
     */
     resetOutputFileFlag = 1;
}

class proc_t {
public:
	proc_t(): statSet(false),dataSet(false) { }
	bool statSet;
	bool dataSet;
	procstat stat;
	procdata data;
	unsigned int statRecord;
	unsigned int dataRecord;
	int nData;
	int nStat;
};

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

int main(int argc, char **argv) {
	cleanUpExitFlag = 0;
	resetOutputFileFlag = 0;

    //ProcReducerConfig config(argc, argv);
    //ProcAMPQIO conn(config.mqServer, config.mqPort, config.mqVHost, config.mqUser, config.mqPassword, config.mqExchangeName, config.mqFrameSize, FILE_MODE_READ);
    ProcAMQPIO conn(DEFAULT_AMQP_HOST, DEFAULT_AMQP_PORT, DEFAULT_AMQP_VHOST, DEFAULT_AMQP_USER, DEFAULT_AMQP_PASSWORD, DEFAULT_AMQP_EXCHANGE_NAME, DEFAULT_AMQP_FRAMESIZE, FILE_MODE_READ);
    conn.set_context("*","*","*");
    ProcHDF5IO* outputFile = NULL;

    time_t currTimestamp = time(NULL);
	time_t lastClean = 0;
    struct tm currTm;
    memset(&currTm, 0, sizeof(struct tm));
    currTm.tm_isdst = -1;
    localtime_r(&currTimestamp, &currTm);
    int last_day = currTm.tm_mday;
    string hostname, identifier, subidentifier;

	procstat* procstat_ptr = NULL;
	procdata* procdata_ptr = NULL;
	int saveCnt = 0;
	int nRecords = 0;
	const char* filenameBuffer = "test";
	string outputFilename;

	char buffer[1024];
	map<string,proc_t*> pidMap;

	signal(SIGINT, sig_handler);
	signal(SIGTERM, sig_handler);
	signal(SIGXCPU, sig_handler);
	signal(SIGUSR1, sig_handler);
	signal(SIGUSR2, sig_handler);
	signal(SIGHUP, sighup_handler);

    while (cleanUpExitFlag == 0) {
        currTimestamp = time(NULL);
        localtime_r(&currTimestamp, &currTm);

        if (currTm.tm_mday != last_day || outputFile == NULL || resetOutputFileFlag == 2) {
			/* flush the file buffers and clean up the old references to the old file */
            if (outputFile != NULL) {
				outputFile->flush();
                delete outputFile;
                outputFile = NULL;
				resetOutputFileFlag = 0;
            }

			/* dump the hash table - we need a fresh start for a fresh file */
			for (auto iter = pidMap.begin(), end = pidMap.end(); iter != end; ) {
				proc_t* record = iter->second;
				delete record;
				pidMap.erase(iter++);
			}

			/* use the current date and time as the suffix for this file */
			strftime(buffer, 1024, "%Y%m%d%H%M%S", &currTm);
			outputFilename = filenameBuffer + string(buffer) + ".h5";

            outputFile = new ProcHDF5IO(outputFilename, FILE_MODE_WRITE);
        }
        last_day = currTm.tm_mday;

		void* data = NULL;
        ProcRecordType recordType = conn.read_stream_record(&data, &nRecords);
		conn.get_frame_context(hostname, identifier, subidentifier);
		cout << "hostname: " << hostname << "; identifier: " << identifier << "; subidentifier: " << subidentifier << endl;

		if (data == NULL) {
			continue;
		}
		if (recordType == TYPE_PROCDATA) {
			procdata_ptr = (procdata*) data;
		} else if (recordType == TYPE_PROCSTAT) {
			procstat_ptr = (procstat*) data;
		}

		outputFile->set_context(hostname, identifier, subidentifier);
        for (int i = 0; i < nRecords; i++) {
			int pid = 0;
			unsigned int recId = 0;
			bool saveNewRec = false;
			unsigned int recID = 0;
			proc_t* record = NULL;
			if (recordType == TYPE_PROCDATA) {
				pid = procdata_ptr[i].pid;
			} else if (recordType == TYPE_PROCSTAT) {
				pid = procstat_ptr[i].pid;
			}
			snprintf(buffer, 1024, "%s.%s.%s.%d", hostname.c_str(), identifier.c_str(), subidentifier.c_str(), pid);
			const char* key = buffer;
			map<string,proc_t*>::iterator pidRec_iter = pidMap.find(key);

			if (pidRec_iter == pidMap.end()) {
				/* new pid! */
				record = new proc_t;
				record->stat.recTime = 0;
				record->data.recTime = 0;
				bzero(record, sizeof(proc_t));
				if (recordType == TYPE_PROCDATA) {
					memcpy(&(record->data), &(procdata_ptr[i]), sizeof(procdata));
					record->dataSet = true;
				} else if (recordType == TYPE_PROCSTAT) {
					memcpy(&(record->stat), &(procstat_ptr[i]), sizeof(procstat));
					record->statSet = true;
				}
				saveNewRec = true;
				auto insertVal = pidMap.insert({string(key), record});
				if (insertVal.second) {
					pidRec_iter = insertVal.first;
				}
			} else {
				record = pidRec_iter->second;
				int cmpVal = 0;
				if (recordType == TYPE_PROCDATA) {
					recId = record->dataRecord;
					if (record->nData <= 1 || (cmpVal = procdatacmp(record->data, procdata_ptr[i])) != 0) {
						saveNewRec = true;
							cout << "procdatacmp: " << cmpVal << "; nData: " << record->nData << endl;
					}
					memcpy(&(record->data), &(procdata_ptr[i]), sizeof(procdata));
					record->nData++;
				} else if (recordType == TYPE_PROCSTAT) {
					recId = record->statRecord;
					if (record->nStat <= 1 || (cmpVal = procstatcmp(record->stat, procstat_ptr[i])) != 0) {
							cout << "procstatcmp: " << cmpVal << "; nStat: " << record->nStat << endl;
						saveNewRec = true;
					}
					memcpy(&(record->stat), (&procstat_ptr[i]), sizeof(procstat));
					record->nStat++;
				}
			}
			if (saveNewRec) { recId = 0; }
			if (record != NULL) {
				if (recordType == TYPE_PROCSTAT) {
					record->statRecord = outputFile->write_procstat(&(procstat_ptr[i]), recId, 1);
						cout << "wrote procstat: " << recId << "; " << record->statRecord << endl;
				} else if (recordType == TYPE_PROCDATA) {
					record->dataRecord = outputFile->write_procdata(&(procdata_ptr[i]), recId, 1);
						cout << "wrote procdata: " << recId << "; " << record->dataRecord << endl;
				}
			}
		}
		free(data);

		outputFile->flush();
		time_t currTime = time(NULL);
		if (currTime - lastClean > cleanFreq || cleanUpExitFlag != 0 || resetOutputFileFlag == 1) {
			cout << "Presently tracking " << pidMap.size() << " processes" << std::endl;
			cout << "Flushing data to disk..." << endl;
			outputFile->flush();
			cout << "Complete" << endl;
			cout << "Starting hash cleanup" << endl;
			/* first trim out local hash */
			for (auto iter = pidMap.begin(), end = pidMap.end(); iter != end; ) {
				proc_t* record = iter->second;
				time_t maxTime = record->data.recTime > record->stat.recTime ? record->data.recTime : record->stat.recTime;
				if (currTime - maxTime > cleanAge) {
					delete record;
					pidMap.erase(iter++);
				} else {
					++iter;
				}
			}
			outputFile->trim_segments(currTime - cleanAge);
			time_t nowTime = time(NULL);
			time_t deltaTime = nowTime - currTime;
			cout << "Cleaning finished in " << deltaTime << " seconds" << endl;
			cout << "Presently tracking " << pidMap.size() << " processes" << std::endl;
			lastClean = currTime;
			if (resetOutputFileFlag == 1) {
				resetOutputFileFlag++;
			}
		}
    }
    return 0;
}
