#include "procfmt.hh"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <iostream>


char* parseProcStatRecord(char* buffer, char* endPtr, procstat* procData, char* pathBuffer, char* cwdBuffer) {
    char* ptr = buffer;
    char* sPtr = buffer;
    struct tm _tmpTime;
    int pos = 0;
    long clockTicksPerSec;
    while (ptr != endPtr) {
        if (*ptr == ',' || *ptr == '\n') {
            switch (pos) {
                case 0:
                    sPtr = strptime(sPtr, "%Y-%m-%d %H:%M:%S", &_tmpTime) + 2;
                    /*
                    RecordTime->time = mktime(&_tmpTime);
                    RecordTime->usec = strtoul(sPtr, &ptr, 10);
                    */
                    break;
                case 1:
                    procData->pid = atoi(sPtr);
                    break;
                case 2:
                    procData->state = *sPtr;
                    break;
                case 3:
                    procData->ppid = atoi(sPtr);
                    break;
                case 4:
                    procData->pgrp = atoi(sPtr);
                    break;
                case 5:
                    procData->session = atoi(sPtr);
                    break;
                case 6:
                    procData->tty = atoi(sPtr);
                    break;
                case 7:
                    procData->tpgid = atoi(sPtr);
                    break;
                case 8:
                    procData->flags = (unsigned int) strtoul(sPtr, &ptr, 10);
                    break;
                case 9:
                    procData->minorFaults = strtoul(sPtr, &ptr, 10);
                    break;
                case 10:
                    procData->cminorFaults = strtoul(sPtr, &ptr, 10);
                    break;
                case 11:
                    procData->majorFaults = strtoul(sPtr, &ptr, 10);
                    break;
                case 12:
                    procData->cmajorFaults = strtoul(sPtr, &ptr, 10);
                    break;
                case 13:
                    procData->utime = strtoul(sPtr, &ptr, 10);
                    break;
                case 14:
                    procData->stime = strtoul(sPtr, &ptr, 10);
                    break;
                case 15:
                    procData->cutime = strtol(sPtr, &ptr, 10);
                    break;
                case 16:
                    procData->cstime = strtol(sPtr, &ptr, 10);
                    break;
                case 17:
                    procData->priority = strtol(sPtr, &ptr, 10);
                    break;
                case 18:
                    procData->nice = strtol(sPtr, &ptr, 10);
                    break;
                case 19:
                    procData->numThreads = strtol(sPtr, &ptr, 10);
                    break;
                case 20:
                    procData->itrealvalue = strtol(sPtr, &ptr, 10);
                    break;
                case 21:
                    //procData->startTimestamp = atof(sPtr);
                    break;
                case 22:
                    procData->vsize = strtoul(sPtr, &ptr, 10);
                    break;
                case 23:
                    procData->rss = strtoul(sPtr, &ptr, 10);
                    break;
                case 24:
                    procData->rsslim = strtoul(sPtr, &ptr, 10);
                    break;
                case 25:
                    procData->vmpeak = strtoul(sPtr, &ptr, 10);
                    break;
                case 26:
                    procData->rsspeak = strtoul(sPtr, &ptr, 10);
                    break;
                case 27:
                    procData->startcode = strtoul(sPtr, &ptr, 10);
                    break;
                case 28:
                    procData->endcode = strtoul(sPtr, &ptr, 10);
                    break;
                case 29:
                    procData->startstack = strtoul(sPtr, &ptr, 10);
                    break;
                case 30:
                    procData->kstkesp = strtoul(sPtr, &ptr, 10);
                    break;
                case 31:
                    procData->kstkeip = strtoul(sPtr, &ptr, 10);
                    break;
                case 32:
                    procData->signal = strtoul(sPtr, &ptr, 10);
                    break;
                case 33:
                    procData->blocked = strtoul(sPtr, &ptr, 10);
                    break;
                case 34:
                    procData->sigignore = strtoul(sPtr, &ptr, 10);
                    break;
                case 35:
                    procData->sigcatch = strtoul(sPtr, &ptr, 10);
                    break;
                case 36:
                    procData->wchan = strtoul(sPtr, &ptr, 10);
                    break;
                case 37:
                    procData->nswap = strtoul(sPtr, &ptr, 10);
                    break;
                case 38:
                    procData->cnswap = strtoul(sPtr, &ptr, 10);
                    break;
                case 39:
                    procData->exitSignal = atoi(sPtr);
                    break;
                case 40:
                    procData->processor = atoi(sPtr);
                    break;
                case 41:
                    procData->cpusAllowed = atoi(sPtr);
                    break;
                case 42:
                    procData->rtPriority = (unsigned int) strtoul(sPtr, &ptr, 10);
                    break;
                case 43:
                    procData->policy = (unsigned int) strtoul(sPtr, &ptr, 10);
                    break;
                case 44:
                    procData->guestTime = strtoul(sPtr, &ptr, 10);
                    break;
                case 45:
                    procData->cguestTime = strtoul(sPtr, &ptr, 10);
                    break;
                case 46:
                    procData->delayacctBlkIOTicks = strtoull(sPtr, &ptr, 10);
                    break;
                case 47:
                    procData->io_rchar = strtoull(sPtr, &ptr, 10);
                    break;
                case 48:
                    procData->io_wchar = strtoull(sPtr, &ptr, 10);
                    break;
                case 49:
                    procData->io_syscr = strtoull(sPtr, &ptr, 10);
                    break;
                case 50:
                    procData->io_syscw = strtoull(sPtr, &ptr, 10);
                    break;
                case 51:
                    procData->io_readBytes = strtoull(sPtr, &ptr, 10);
                    break;
                case 52:
                    procData->io_writeBytes = strtoull(sPtr, &ptr, 10);
                    break;
                case 53:
                    procData->io_cancelledWriteBytes = strtoull(sPtr, &ptr, 10);
                    break;
                case 54:
                    procData->m_size = strtoul(sPtr, &ptr, 10);
                    break;
                case 55:
                    procData->m_resident = strtoul(sPtr, &ptr, 10);
                    break;
                case 56:
                    procData->m_share = strtoul(sPtr, &ptr, 10);
                    break;
                case 57:
                    procData->m_text = strtoul(sPtr, &ptr, 10);
                    break;
                case 58:
                    procData->m_data = strtoul(sPtr, &ptr, 10);
                    break;
                case 59:
                    clockTicksPerSec = strtol(sPtr, &ptr, 10);
                    break;
                case 60:
                    procData->realUid = strtoul(sPtr, &ptr, 10);
                    break;
                case 61:
                    procData->effUid = strtoul(sPtr, &ptr, 10);
                    break;
                case 62:
                    procData->realGid = strtoul(sPtr, &ptr, 10);
                    break;
                case 63:
                    procData->effGid = strtoul(sPtr, &ptr, 10);
                    break;
                case 64:
                    //snprintf(procData->execName, (ptr - sPtr < 258 ? (ptr - sPtr) + 1 : 258), "%s", sPtr);
                    break;
                case 65:
                    snprintf(pathBuffer, (ptr - sPtr < LBUFFER_SIZE ? (ptr - sPtr) + 1 : LBUFFER_SIZE), "%s", sPtr);
                    break;
                case 66:
                    snprintf(cwdBuffer, (ptr - sPtr < LBUFFER_SIZE ? (ptr - sPtr) + 1 : LBUFFER_SIZE), "%s", sPtr);
                    break;
            }

            pos++;
            if (*ptr == '\n') break;
            sPtr = ptr + 1;
        }
        ptr++;
    }
    return ++ptr;
}

int writeProcStatRecord(FILE* output, procstat* l_procData, long clockTicksPerSec, char* timeBuffer, char* pathBuffer, char* cwdBuffer) {
    //return fprintf(output,"%s,%d,%c,%d,%d,%d,%d,%d,%u,%lu,%lu,%lu,%lu,%lu,%lu,%ld,%ld,%ld,%ld,%ld,%ld,%0.3f,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%lu,%d,%d,%d,%u,%u,%lu,%lu,%llu,%llu,%llu,%llu,%llu,%llu,%llu,%llu,%lu,%lu,%lu,%lu,%lu,%ld,%lu,%lu,%lu,%lu,%s,%s,%s\n", timeBuffer,l_procData->pid,l_procData->state,l_procData->ppid,l_procData->pgrp,l_procData->session,l_procData->tty,l_procData->tpgid,l_procData->flags,l_procData->minorFaults,l_procData->cminorFaults,l_procData->majorFaults,l_procData->cmajorFaults,l_procData->utime,l_procData->stime,l_procData->cutime,l_procData->cstime,l_procData->priority,l_procData->nice,l_procData->numThreads,l_procData->itrealvalue,l_procData->startTimestamp,l_procData->vsize,l_procData->rss,l_procData->rsslim,l_procData->vmpeak,l_procData->rsspeak,l_procData->startcode,l_procData->endcode,l_procData->startstack,l_procData->kstkesp,l_procData->kstkeip,l_procData->signal,l_procData->blocked,l_procData->sigignore,l_procData->sigcatch,l_procData->wchan,l_procData->nswap,l_procData->cnswap,l_procData->exitSignal,l_procData->processor,l_procData->cpusAllowed,l_procData->rtPriority,l_procData->policy,l_procData->guestTime,l_procData->cguestTime,l_procData->delayacctBlkIOTicks,l_procData->io_rchar,l_procData->io_wchar,l_procData->io_syscr,l_procData->io_syscw,l_procData->io_readBytes,l_procData->io_writeBytes,l_procData->io_cancelledWriteBytes, l_procData->m_size,l_procData->m_resident,l_procData->m_share,l_procData->m_text,l_procData->m_data, clockTicksPerSec, l_procData->realUid, l_procData->effUid, l_procData->realGid, l_procData->effGid, l_procData->execName, pathBuffer, cwdBuffer);
    return 0;
}

int compareProcData(procdata* a, procdata* b) {
    int i;
    if (a == NULL && b == NULL) {
        return 0;
    }
    if (a == NULL || b == NULL) {
        return 1;
    }
    if (a->pid != b->pid) {
        return 1;
    }
    if (a->ppid != b->ppid) {
        return 1;
    }
    if (a->startTime != b->startTime) {
        return 1;
    }
    if (strcmp(a->execName, b->execName) != 0) {
        return 1;
    }
    if (a->cmdArgBytes != b->cmdArgBytes) {
        return 1;
    }
    for (i = 0; i < a->cmdArgBytes; i++) {
        if (a->cmdArgs[i] != b->cmdArgs[i]) return 1;
    }
    if (strcmp(a->exePath, b->exePath) !=0 ) {
        return 1;
    }
    if (strcmp(a->cwdPath, b->cwdPath) != 0) {
        return 1;
    }
    return 0;
}

ProcFile::ProcFile(const char* filename, const char* hostname, const char* identifier) {
    herr_t status;

    file = H5Fopen(filename, H5F_ACC_CREAT | H5F_ACC_RDWR, H5P_DEFAULT);
    if (file < 0) {
        throw ProcFileException("Failed to open file");
    }

    if (H5Lexists(file, hostname, H5P_DEFAULT) == 1) {
        hostGroup = H5Gopen2(file, hostname, H5P_DEFAULT);
    } else {
        hostGroup = H5Gcreate(file, hostname, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    }
    if (hostGroup < 0) {
        throw ProcFileException("Failed to access hostname group");
    }

    if (H5Lexists(hostGroup, identifier, H5P_DEFAULT) == 1) {
        idGroup = H5Gopen2(hostGroup, identifier, H5P_DEFAULT);
    } else {
        idGroup = H5Gcreate(hostGroup, identifier, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    }
    if (idGroup < 0) {
        throw ProcFileException("Failed to access identifier group");
    }

    /* setup data structure types */
    strType_exeBuffer = H5Tcopy(H5T_C_S1);
    status = H5Tset_size(strType_exeBuffer, EXEBUFFER_SIZE);
    if (status < 0) {
        throw ProcFileException("Failed to set strType_exeBuffer size");
    }
    
    strType_buffer = H5Tcopy(H5T_C_S1);
    status = H5Tset_size(strType_buffer, BUFFER_SIZE);
    if (status < 0) {
        throw ProcFileException("Failed to set strType_buffer size");
    }

    type_procdata = H5Tcreate(H5T_COMPOUND, sizeof(procdata));
    if (type_procdata < 0) throw ProcFileException("Failed to create type_procdata");
    H5Tinsert(type_procdata, "execName", HOFFSET(procdata, execName), strType_exeBuffer);
    H5Tinsert(type_procdata, "cmdArgBytes", HOFFSET(procdata, cmdArgBytes), H5T_NATIVE_SHORT);
    H5Tinsert(type_procdata, "cmdArgs", HOFFSET(procdata, cmdArgs), strType_buffer);
    H5Tinsert(type_procdata, "exePath", HOFFSET(procdata, exePath), strType_buffer);
    H5Tinsert(type_procdata, "cwdPath", HOFFSET(procdata, cwdPath), strType_buffer);
    H5Tinsert(type_procdata, "recTime", HOFFSET(procdata, recTime), H5T_NATIVE_ULONG);
    H5Tinsert(type_procdata, "recTimeUSec", HOFFSET(procdata, recTimeUSec), H5T_NATIVE_ULONG);
    H5Tinsert(type_procdata, "startTime", HOFFSET(procdata, startTime), H5T_NATIVE_ULONG);
    H5Tinsert(type_procdata, "startTimeUSec", HOFFSET(procdata, startTimeUSec), H5T_NATIVE_ULONG);
    H5Tinsert(type_procdata, "pid", HOFFSET(procdata, pid), H5T_NATIVE_UINT);
    H5Tinsert(type_procdata, "ppid", HOFFSET(procdata, ppid), H5T_NATIVE_UINT);

    type_procstat = H5Tcreate(H5T_COMPOUND, sizeof(procstat));
    if (type_procstat < 0) throw ProcFileException("Failed to create type_procstat");
    H5Tinsert(type_procstat, "pid", HOFFSET(procstat, pid), H5T_NATIVE_UINT);
    H5Tinsert(type_procstat, "recTime", HOFFSET(procstat, recTime), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "recTimeUSec", HOFFSET(procstat, recTimeUSec), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "state", HOFFSET(procstat, state), H5T_NATIVE_CHAR);
    H5Tinsert(type_procstat, "ppid", HOFFSET(procstat, ppid), H5T_NATIVE_INT);
    H5Tinsert(type_procstat, "pgrp", HOFFSET(procstat, pgrp), H5T_NATIVE_INT);
    H5Tinsert(type_procstat, "session", HOFFSET(procstat, session), H5T_NATIVE_INT);
    H5Tinsert(type_procstat, "tty", HOFFSET(procstat, tty), H5T_NATIVE_INT);
    H5Tinsert(type_procstat, "tpgid", HOFFSET(procstat, tpgid), H5T_NATIVE_INT);
    H5Tinsert(type_procstat, "realUid", HOFFSET(procstat, realUid), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "effUid", HOFFSET(procstat, effUid), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "realGid", HOFFSET(procstat, realGid), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "effGid", HOFFSET(procstat, effGid), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "flags", HOFFSET(procstat, flags), H5T_NATIVE_UINT);
    H5Tinsert(type_procstat, "minorFaults", HOFFSET(procstat, minorFaults), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "cminorFaults", HOFFSET(procstat, cminorFaults), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "majorFaults", HOFFSET(procstat, majorFaults), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "cmajorFaults", HOFFSET(procstat, cmajorFaults), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "utime", HOFFSET(procstat, utime), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "stime", HOFFSET(procstat, stime), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "cutime", HOFFSET(procstat, cutime), H5T_NATIVE_LONG);
    H5Tinsert(type_procstat, "cstime", HOFFSET(procstat, cstime), H5T_NATIVE_LONG);
    H5Tinsert(type_procstat, "priority", HOFFSET(procstat, priority), H5T_NATIVE_LONG);
    H5Tinsert(type_procstat, "nice", HOFFSET(procstat, nice), H5T_NATIVE_LONG);
    H5Tinsert(type_procstat, "numThreads", HOFFSET(procstat, numThreads), H5T_NATIVE_LONG);
    H5Tinsert(type_procstat, "itrealvalue", HOFFSET(procstat, itrealvalue), H5T_NATIVE_LONG);
    H5Tinsert(type_procstat, "starttime", HOFFSET(procstat, starttime), H5T_NATIVE_ULLONG);
    H5Tinsert(type_procstat, "vsize", HOFFSET(procstat, vsize), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "rss", HOFFSET(procstat, rss), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "rsslim", HOFFSET(procstat, rsslim), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "startcode", HOFFSET(procstat, startcode), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "endcode", HOFFSET(procstat, endcode), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "startstack", HOFFSET(procstat, startstack), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "kstkesp", HOFFSET(procstat, kstkesp), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "kstkeip", HOFFSET(procstat, kstkeip), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "signal", HOFFSET(procstat, signal), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "blocked", HOFFSET(procstat, blocked), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "sigignore", HOFFSET(procstat, sigignore), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "sigcatch", HOFFSET(procstat, sigcatch), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "wchan", HOFFSET(procstat, wchan), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "nswap", HOFFSET(procstat, nswap), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "cnswap", HOFFSET(procstat, cnswap), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "exitSignal", HOFFSET(procstat, exitSignal), H5T_NATIVE_INT);
    H5Tinsert(type_procstat, "processor", HOFFSET(procstat, processor), H5T_NATIVE_INT);
    H5Tinsert(type_procstat, "rtPriority", HOFFSET(procstat, rtPriority), H5T_NATIVE_UINT);
    H5Tinsert(type_procstat, "policy", HOFFSET(procstat, policy), H5T_NATIVE_UINT);
    H5Tinsert(type_procstat, "delayacctBlkIOTicks", HOFFSET(procstat, delayacctBlkIOTicks), H5T_NATIVE_ULLONG);
    H5Tinsert(type_procstat, "guestTime", HOFFSET(procstat, guestTime), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "cguestTime", HOFFSET(procstat, cguestTime), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "vmpeak", HOFFSET(procstat, vmpeak), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "rsspeak", HOFFSET(procstat, rsspeak), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "cpusAllowed", HOFFSET(procstat, cpusAllowed), H5T_NATIVE_INT);
    H5Tinsert(type_procstat, "io_rchar", HOFFSET(procstat, io_rchar), H5T_NATIVE_ULLONG);
    H5Tinsert(type_procstat, "io_wchar", HOFFSET(procstat, io_wchar), H5T_NATIVE_ULLONG);
    H5Tinsert(type_procstat, "io_syscr", HOFFSET(procstat, io_syscr), H5T_NATIVE_ULLONG);
    H5Tinsert(type_procstat, "io_syscw", HOFFSET(procstat, io_syscw), H5T_NATIVE_ULLONG);
    H5Tinsert(type_procstat, "io_readBytes", HOFFSET(procstat, io_readBytes), H5T_NATIVE_ULLONG);
    H5Tinsert(type_procstat, "io_writeBytes", HOFFSET(procstat, io_writeBytes), H5T_NATIVE_ULLONG);
    H5Tinsert(type_procstat, "io_cancelledWriteBytes", HOFFSET(procstat, io_cancelledWriteBytes), H5T_NATIVE_ULLONG);
    H5Tinsert(type_procstat, "m_size", HOFFSET(procstat, m_size), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "m_resident", HOFFSET(procstat, m_resident), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "m_share", HOFFSET(procstat, m_share), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "m_text", HOFFSET(procstat, m_text), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "m_data", HOFFSET(procstat, m_data), H5T_NATIVE_ULONG);

}

bool ProcFile::write_procstat(procstat* start_pointer, int count) {
    return write_dataset("procstat", type_procstat, (void*) start_pointer, count, 2048);
}

bool ProcFile::write_procdata(procdata* start_pointer, int count) {
    return write_dataset("procdata", type_procdata, (void*) start_pointer, count, 512);
}

bool ProcFile::write_dataset(const char* dsName, hid_t type, void* start_pointer, int count, int chunkSize) {
    hsize_t chunk_dims = chunkSize;
    hsize_t rank = 1;
    hsize_t nRecords = 0;
    hsize_t targetRecords = 0;
    hsize_t newRecords = count;

    hid_t dataset;
    hid_t filespace;
    hid_t dataspace;
    herr_t status;

    if (H5Lexists(idGroup, dsName, H5P_DEFAULT) == 1) {
        dataset = H5Dopen2(idGroup, dsName, H5P_DEFAULT);
        dataspace = H5Dget_space(dataset);
        H5Sget_simple_extent_dims(dataspace, &nRecords, NULL);
        status = H5Sclose(dataspace);

    } else {
        hid_t param;
        hsize_t initial_dims = count;
        hsize_t maximal_dims = H5S_UNLIMITED;
        dataspace = H5Screate_simple(rank, &initial_dims, &maximal_dims);

        param = H5Pcreate(H5P_DATASET_CREATE);
        H5Pset_chunk(param, rank, &chunk_dims);
        dataset = H5Dcreate(idGroup, dsName, type, dataspace, H5P_DEFAULT, param, H5P_DEFAULT);
        H5Pclose(param);
        H5Sclose(dataspace);
    }
    targetRecords = nRecords + count;
    status = H5Dset_extent(dataset, &targetRecords);
    filespace = H5Dget_space(dataset);

    status = H5Sselect_hyperslab(filespace, H5S_SELECT_SET, &nRecords, NULL, &newRecords, NULL);
    dataspace = H5Screate_simple(rank, &newRecords, NULL);

    H5Dwrite(dataset, type, dataspace, filespace, H5P_DEFAULT, start_pointer);

    H5Sclose(filespace);
    H5Sclose(dataspace);
    H5Dclose(dataset);
    return true;
}

ProcFile::~ProcFile() {
    herr_t status;
    if (type_procdata > 0) status = H5Tclose(type_procdata);
    if (type_procstat > 0) status = H5Tclose(type_procstat);
    if (strType_exeBuffer > 0) status = H5Tclose(strType_exeBuffer);
    if (strType_buffer > 0) status = H5Tclose(strType_buffer);
    if (idGroup > 0) status = H5Gclose(idGroup);
    if (hostGroup > 0) status = H5Gclose(hostGroup);
    if (file > 0) status = H5Fclose(file);
}
