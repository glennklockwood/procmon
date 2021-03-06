#include "procfmt.hh"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <iostream>


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

ProcFile::ProcFile(const char* t_filename, const char* t_hostname, const char* t_identifier, ProcFileFormat t_format, ProcFileMode t_mode) {
    filename = t_filename;
    hostname = t_hostname;
    identifier = t_identifier;
    format = t_format;
    mode = t_mode;

    filePtr = NULL;
    ptr = NULL;
    sPtr = NULL;
    ePtr = NULL;
    file = 0;
    hostGroup = 0;
    idGroup = 0;
    strType_exeBuffer = 0;
    strType_buffer = 0;
    type_procdata = 0;
    type_procstat = 0;

    if (format == FILE_FORMAT_HDF5 && mode == FILE_MODE_READ) {
        hdf5_open_read();
        hdf5_initialize_types();
    } else if (format == FILE_FORMAT_HDF5 && mode == FILE_MODE_WRITE) {
        hdf5_open_write();
        hdf5_initialize_types();
    } else if (format == FILE_FORMAT_TEXT && mode == FILE_MODE_READ) {
        text_open_read();
    } else if (format == FILE_FORMAT_TEXT && mode == FILE_MODE_WRITE) {
        text_open_write();
    }
}

bool ProcFile::write_procdata(procdata* start_ptr, int count) {
    if (mode != FILE_MODE_WRITE) return false;

    if (format == FILE_FORMAT_HDF5) {
        return hdf5_write_procdata(start_ptr, count);
    } else if (format == FILE_FORMAT_TEXT) {
        bool success = true;
        for (int i = 0; i < count; i++) {
            success &= text_write_procdata(&(start_ptr[i]));
        }
        return success;
    }
    return false;
}

bool ProcFile::write_procstat(procstat* start_ptr, int count) {
    if (mode != FILE_MODE_WRITE) return false;

    if (format == FILE_FORMAT_HDF5) {
        return hdf5_write_procstat(start_ptr, count);
    } else if (format == FILE_FORMAT_TEXT) {
        bool success = true;
        for (int i = 0; i < count; i++) {
            success &= text_write_procstat(&(start_ptr[i]));
        }
        return success;
    }
    return false;
}

void ProcFile::text_open_read() {
    filePtr = fopen(filename.c_str(), "r");
    if (filePtr == NULL) throw ProcFileException("Couldn't open text file for reading");
}

void ProcFile::text_open_write() {
    filePtr = fopen(filename.c_str(), "a");
    if (filePtr == NULL) throw ProcFileException("Couldn't open text file for writing");
}

unsigned int ProcFile::read_procdata(procdata* procData, unsigned int id) {
    return read_procdata(procData, id, 1);
}

unsigned int ProcFile::read_procstat(procstat* procStat, unsigned int id) {
    return read_procstat(procStat, id, 1);
}

unsigned int ProcFile::read_procdata(procdata* start_ptr, unsigned int start_id, unsigned int count) {
    if (format == FILE_FORMAT_HDF5) {
        return hdf5_read_procdata(start_ptr, start_id, count);
    }
    return false;
}

unsigned int ProcFile::read_procstat(procstat* start_ptr, unsigned int start_id, unsigned int count) {
    if (format == FILE_FORMAT_HDF5) {
        return hdf5_read_procstat(start_ptr, start_id, count);
    }
    return false;
}

void ProcFile::hdf5_open_read() {
    herr_t status;

    file = H5Fopen(filename.c_str(), H5F_ACC_RDONLY, H5P_DEFAULT);
    if (file < 0) {
        throw ProcFileException("Failed to open file");
    }

    if (H5Lexists(file, hostname.c_str(), H5P_DEFAULT) == 1) {
        hostGroup = H5Gopen2(file, hostname.c_str(), H5P_DEFAULT);
    }
    if (hostGroup < 0) {
        throw ProcFileException("Failed to access hostname group");
    }

    if (H5Lexists(hostGroup, identifier.c_str(), H5P_DEFAULT) == 1) {
        idGroup = H5Gopen2(hostGroup, identifier.c_str(), H5P_DEFAULT);
    }
    if (idGroup < 0) {
        throw ProcFileException("Failed to access identifier group");
    }
}

void ProcFile::hdf5_open_write() {
    herr_t status;

    file = H5Fopen(filename.c_str(), H5F_ACC_CREAT | H5F_ACC_RDWR, H5P_DEFAULT);
    if (file < 0) {
        throw ProcFileException("Failed to open file");
    }

    if (H5Lexists(file, hostname.c_str(), H5P_DEFAULT) == 1) {
        hostGroup = H5Gopen2(file, hostname.c_str(), H5P_DEFAULT);
    } else {
        hostGroup = H5Gcreate(file, hostname.c_str(), H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    }
    if (hostGroup < 0) {
        throw ProcFileException("Failed to access hostname group");
    }

    if (H5Lexists(hostGroup, identifier.c_str(), H5P_DEFAULT) == 1) {
        idGroup = H5Gopen2(hostGroup, identifier.c_str(), H5P_DEFAULT);
    } else {
        idGroup = H5Gcreate(hostGroup, identifier.c_str(), H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    }
    if (idGroup < 0) {
        throw ProcFileException("Failed to access identifier group");
    }
}

void ProcFile::hdf5_initialize_types() {
    herr_t status; 

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
    H5Tinsert(type_procdata, "cmdArgBytes", HOFFSET(procdata, cmdArgBytes), H5T_NATIVE_ULONG);
    H5Tinsert(type_procdata, "cmdArgs", HOFFSET(procdata, cmdArgs), strType_buffer);
    H5Tinsert(type_procdata, "exePath", HOFFSET(procdata, exePath), strType_buffer);
    H5Tinsert(type_procdata, "cwdPath", HOFFSET(procdata, cwdPath), strType_buffer);
    H5Tinsert(type_procdata, "recTime", HOFFSET(procdata, recTime), H5T_NATIVE_ULONG);
    H5Tinsert(type_procdata, "recTimeUSec", HOFFSET(procdata, recTimeUSec), H5T_NATIVE_ULONG);
    H5Tinsert(type_procdata, "startTime", HOFFSET(procdata, startTime), H5T_NATIVE_ULONG);
    H5Tinsert(type_procdata, "startTimeUSec", HOFFSET(procdata, startTimeUSec), H5T_NATIVE_ULONG);
    H5Tinsert(type_procdata, "pid", HOFFSET(procdata, pid), H5T_NATIVE_UINT);
    H5Tinsert(type_procdata, "ppid", HOFFSET(procdata, ppid), H5T_NATIVE_UINT);
    H5Tinsert(type_procdata, "nextRec", HOFFSET(procdata, nextRec), H5T_NATIVE_UINT);
    H5Tinsert(type_procdata, "prevRec", HOFFSET(procdata, prevRec), H5T_NATIVE_UINT);

    type_procstat = H5Tcreate(H5T_COMPOUND, sizeof(procstat));
    if (type_procstat < 0) throw ProcFileException("Failed to create type_procstat");
    H5Tinsert(type_procstat, "pid", HOFFSET(procstat, pid), H5T_NATIVE_UINT);
    H5Tinsert(type_procstat, "nextRec", HOFFSET(procstat, nextRec), H5T_NATIVE_UINT);
    H5Tinsert(type_procstat, "prevRec", HOFFSET(procstat, prevRec), H5T_NATIVE_UINT);
    H5Tinsert(type_procstat, "recTime", HOFFSET(procstat, recTime), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "recTimeUSec", HOFFSET(procstat, recTimeUSec), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "startTime", HOFFSET(procstat, startTime), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "startTimeUSec", HOFFSET(procstat, startTimeUSec), H5T_NATIVE_ULONG);
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
    H5Tinsert(type_procstat, "utime", HOFFSET(procstat, utime), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "stime", HOFFSET(procstat, stime), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "priority", HOFFSET(procstat, priority), H5T_NATIVE_LONG);
    H5Tinsert(type_procstat, "nice", HOFFSET(procstat, nice), H5T_NATIVE_LONG);
    H5Tinsert(type_procstat, "numThreads", HOFFSET(procstat, numThreads), H5T_NATIVE_LONG);
    H5Tinsert(type_procstat, "vsize", HOFFSET(procstat, vsize), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "rss", HOFFSET(procstat, rss), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "rsslim", HOFFSET(procstat, rsslim), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "signal", HOFFSET(procstat, signal), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "blocked", HOFFSET(procstat, blocked), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "sigignore", HOFFSET(procstat, sigignore), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "sigcatch", HOFFSET(procstat, sigcatch), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "rtPriority", HOFFSET(procstat, rtPriority), H5T_NATIVE_UINT);
    H5Tinsert(type_procstat, "policy", HOFFSET(procstat, policy), H5T_NATIVE_UINT);
    H5Tinsert(type_procstat, "delayacctBlkIOTicks", HOFFSET(procstat, delayacctBlkIOTicks), H5T_NATIVE_ULLONG);
    H5Tinsert(type_procstat, "guestTime", HOFFSET(procstat, guestTime), H5T_NATIVE_ULONG);
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

unsigned int ProcFile::hdf5_write_procstat(procstat* start_pointer, unsigned int count) {
    return hdf5_write_dataset("procstat", type_procstat, (void*) start_pointer, count, 128);
}

unsigned int ProcFile::hdf5_write_procdata(procdata* start_pointer, unsigned int count) {
    return hdf5_write_dataset("procdata", type_procdata, (void*) start_pointer, count, 4);
}

unsigned int ProcFile::hdf5_read_procstat(procstat* start_pointer, unsigned int start_id, unsigned int count) {
    return hdf5_read_dataset("procstat", type_procstat, (void*) start_pointer, start_id, count);
}

unsigned int ProcFile::hdf5_read_procdata(procdata* start_pointer, unsigned int start_id, unsigned int count) {
    return hdf5_read_dataset("procdata", type_procdata, (void*) start_pointer, start_id, count);
}

unsigned int ProcFile::hdf5_read_dataset(const char* dsName, hid_t type, void* start_pointer, unsigned int start_id, unsigned int count) {
    if (H5Lexists(idGroup, dsName, H5P_DEFAULT) == 0) {
        return 0;
    }

    hsize_t targetRecords = 0;
    hsize_t localRecords = count;
    hsize_t remoteStart = start_id;
    hsize_t localStart = 0;
    hsize_t nRecords = 0;

    hid_t dataset = H5Dopen2(idGroup, dsName, H5P_DEFAULT);
    hid_t dataspace = H5Dget_space(dataset);
    hid_t memspace = H5Screate_simple(1, &targetRecords, NULL);

    herr_t status = 0;

    int rank = H5Sget_simple_extent_ndims(dataspace);
    status = H5Sget_simple_extent_dims(dataspace, &nRecords, NULL);
    if (remoteStart < nRecords) {
        targetRecords = count < (nRecords - remoteStart) ? count : (nRecords - remoteStart);

        status = H5Sselect_hyperslab(dataspace, H5S_SELECT_SET, &remoteStart, H5P_DEFAULT, &targetRecords, H5P_DEFAULT);
        status = H5Sselect_hyperslab(memspace, H5S_SELECT_SET, &localStart, H5P_DEFAULT, &localRecords, H5P_DEFAULT);
        status = H5Dread(dataset, type, H5S_ALL, dataspace, H5P_DEFAULT, start_pointer);
    }

    H5Sclose(dataspace);
    H5Sclose(memspace);
    H5Dclose(dataset);

    return (int) targetRecords;
}

unsigned int ProcFile::hdf5_write_dataset(const char* dsName, hid_t type, void* start_pointer, int count, int chunkSize) {
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
    return (int) newRecords;
}

ProcFileRecordType ProcFile::read_stream_record(procdata* procData, procstat* procStat) {
    if (format == FILE_FORMAT_HDF5) return TYPE_INVALID;
    if (format == FILE_FORMAT_TEXT) {
        return text_read_record(procData, procStat);
    }
    return TYPE_INVALID;
}

int ProcFile::text_fill_buffer() {
    if (format != FILE_FORMAT_TEXT) return -1;
    if (sPtr != NULL) {
        bcopy(buffer, sPtr, sizeof(char)*(ptr-sPtr));
        ptr = buffer + (ptr - sPtr);
        sPtr = buffer;
    } else {
        sPtr = buffer;
        ptr = buffer;
    }
    int readBytes = fread(ptr, sizeof(char), LBUFFER_SIZE - (ptr-sPtr), filePtr);
    if (readBytes == 0) {
        return 0;
    }
    if (sPtr != NULL) {
        ptr = buffer + (ptr - sPtr);
        sPtr = buffer;
    }
    ePtr = ptr + readBytes;
    return readBytes;
}


ProcFileRecordType ProcFile::text_read_record(procdata* procData, procstat* procStat) {
    while (true) {
        if (sPtr == NULL || ptr == ePtr) {
            int bytes = text_fill_buffer();
            if (bytes == 0) return TYPE_INVALID;
        }
        if (*ptr == ',') {
            *ptr = 0;
            if (strcmp(sPtr, "procstat") == 0) {
                sPtr = ptr + 1;
                return text_read_procstat(procStat) ? TYPE_PROCSTAT : TYPE_INVALID;
            } else if (strcmp(sPtr, "procdata") == 0) {
                sPtr = ptr + 1;
                return text_read_procdata(procData) ? TYPE_PROCDATA : TYPE_INVALID;
            }
        }
        ptr++;
    }
}

bool ProcFile::text_read_procstat(procstat* procStat) {
    int pos = 0;
    int readBytes = -1;
    bool done = false;
    while (!done) {
        if (sPtr == NULL || ptr == ePtr) {
            int bytes = text_fill_buffer();
            if (bytes == 0) return true;
        }
        if ((*ptr == ',' || *ptr == '\n') && (readBytes < 0 || readBytes == (ptr-sPtr))) {
            if (*ptr == '\n') done = true;
            *ptr = 0;
            switch (pos) {
                case 0: procStat->pid = atoi(sPtr); break;
                case 1: procStat->recTime = strtoul(sPtr, &ptr, 10); break;
                case 2: procStat->recTimeUSec = strtoul(sPtr, &ptr, 10); break;
                case 3: procStat->startTime = strtoul(sPtr, &ptr, 10); break;
                case 4: procStat->startTimeUSec = strtoul(sPtr, &ptr, 10); break;
                case 5: procStat->state = *sPtr; break;
                case 6: procStat->ppid = atoi(sPtr); break;
                case 7: procStat->pgrp = atoi(sPtr); break;
                case 8: procStat->session = atoi(sPtr); break;
                case 9: procStat->tty = atoi(sPtr); break;
                case 10: procStat->tpgid = atoi(sPtr); break;
                case 11: procStat->flags = (unsigned int) strtoul(sPtr, &ptr, 10); break;
                case 12: procStat->utime = strtoul(sPtr, &ptr, 10); break;
                case 13: procStat->stime = strtoul(sPtr, &ptr, 10); break;
                case 14: procStat->priority = strtol(sPtr, &ptr, 10); break;
                case 15: procStat->nice = strtol(sPtr, &ptr, 10); break;
                case 16: procStat->numThreads = strtol(sPtr, &ptr, 10); break;
                case 17: procStat->vsize = strtoul(sPtr, &ptr, 10); break;
                case 18: procStat->rss = strtoul(sPtr, &ptr, 10); break;
                case 19: procStat->rsslim = strtoul(sPtr, &ptr, 10); break;
                case 20: procStat->vmpeak = strtoul(sPtr, &ptr, 10); break;
                case 21: procStat->rsspeak = strtoul(sPtr, &ptr, 10); break;
                case 22: procStat->signal = strtoul(sPtr, &ptr, 10); break;
                case 23: procStat->blocked = strtoul(sPtr, &ptr, 10); break;
                case 24: procStat->sigignore = strtoul(sPtr, &ptr, 10); break;
                case 25: procStat->sigcatch = strtoul(sPtr, &ptr, 10); break;
                case 26: procStat->cpusAllowed = atoi(sPtr); break;
                case 27: procStat->rtPriority = (unsigned int) strtoul(sPtr, &ptr, 10); break;
                case 28: procStat->policy = (unsigned int) strtoul(sPtr, &ptr, 10); break;
                case 29: procStat->guestTime = strtoul(sPtr, &ptr, 10); break;
                case 30: procStat->delayacctBlkIOTicks = strtoull(sPtr, &ptr, 10); break;
                case 31: procStat->io_rchar = strtoull(sPtr, &ptr, 10); break;
                case 32: procStat->io_wchar = strtoull(sPtr, &ptr, 10); break;
                case 33: procStat->io_syscr = strtoull(sPtr, &ptr, 10); break;
                case 34: procStat->io_syscw = strtoull(sPtr, &ptr, 10); break;
                case 35: procStat->io_readBytes = strtoull(sPtr, &ptr, 10); break;
                case 36: procStat->io_writeBytes = strtoull(sPtr, &ptr, 10); break;
                case 37: procStat->io_cancelledWriteBytes = strtoull(sPtr, &ptr, 10); break;
                case 38: procStat->m_size = strtoul(sPtr, &ptr, 10); break;
                case 39: procStat->m_resident = strtoul(sPtr, &ptr, 10); break;
                case 40: procStat->m_share = strtoul(sPtr, &ptr, 10); break;
                case 41: procStat->m_text = strtoul(sPtr, &ptr, 10); break;
                case 42: procStat->m_data = strtoul(sPtr, &ptr, 10); break;
                case 43: procStat->realUid = strtoul(sPtr, &ptr, 10); break;
                case 44: procStat->effUid = strtoul(sPtr, &ptr, 10); break;
                case 45: procStat->realGid = strtoul(sPtr, &ptr, 10); break;
                case 46: procStat->effGid = strtoul(sPtr, &ptr, 10); done = true; break;
            }
            pos++;
            sPtr = ptr + 1;
        }
        ptr++;
    }
    return true;
}

bool ProcFile::text_read_procdata(procdata* procData) {
    int pos = 0;
    int readBytes = -1;
    bool done = false;
    while (!done) {
        if (sPtr == NULL || ptr == ePtr) {
            int bytes = text_fill_buffer();
            if (bytes == 0) return true;
        }
        if ((*ptr == ',' || *ptr == '\n') && (readBytes < 0 || readBytes == (ptr-sPtr))) {
            if (*ptr == '\n') done = true;
            *ptr = 0;
            switch (pos) {
                case 0: procData->pid = atoi(sPtr); break;
                case 1: procData->ppid = atoi(sPtr); break;
                case 2: procData->recTime = strtoul(sPtr, &ptr, 10); break;
                case 3: procData->recTimeUSec = strtoul(sPtr, &ptr, 10); break;
                case 4: procData->startTime = strtoul(sPtr, &ptr, 10); break;
                case 5: procData->startTimeUSec = strtoul(sPtr, &ptr, 10); break;
                case 6: readBytes = atoi(sPtr); break;
                case 7: memcpy(procData->execName, sPtr, sizeof(char)*readBytes); readBytes = -1; break;
                case 8: readBytes = atoi(sPtr); break;
                case 9: memcpy(procData->cmdArgs, sPtr, sizeof(char)*readBytes); procData->cmdArgBytes = readBytes; readBytes = -1; break;
                case 10: readBytes = atoi(sPtr); break;
                case 11: memcpy(procData->exePath, sPtr, sizeof(char)*readBytes); readBytes = -1; break;
                case 12: readBytes = atoi(sPtr); break;
                case 13: memcpy(procData->cwdPath, sPtr, sizeof(char)*readBytes); readBytes = -1; break;
            }
            pos++;
            sPtr = ptr + 1;
        }
        ptr++;
    }
    return true;
}

int ProcFile::text_write_procdata(procdata* procData) {
    int nBytes = 0;
    nBytes += fprintf(filePtr, "procdata,%d,%d,%lu,%lu,%lu,%lu",procData->pid,procData->ppid,procData->recTime,procData->recTimeUSec,procData->startTime,procData->startTimeUSec);
    nBytes += fprintf(filePtr, ",%lu,%s", strlen(procData->execName), procData->execName);
    nBytes += fprintf(filePtr, ",%lu,%s", procData->cmdArgBytes, procData->cmdArgs);
    nBytes += fprintf(filePtr, ",%lu,%s", strlen(procData->exePath), procData->exePath);
    nBytes += fprintf(filePtr, ",%lu,%s\n", strlen(procData->cwdPath), procData->cwdPath);
    return nBytes;
}

int ProcFile::text_write_procstat(procstat* procStat) {
    int nBytes = 0;
    nBytes += fprintf(filePtr, "procstat,%d,%lu,%lu,%lu,%lu",procStat->pid,procStat->recTime,procStat->recTimeUSec,procStat->startTime,procStat->startTimeUSec);
    nBytes += fprintf(filePtr, ",%c,%d,%d,%d",procStat->state,procStat->ppid,procStat->pgrp,procStat->session);
    nBytes += fprintf(filePtr, ",%d,%d,%u,%lu,%lu",procStat->tty,procStat->tpgid,procStat->flags,procStat->utime,procStat->stime);
    nBytes += fprintf(filePtr, ",%ld,%ld,%ld,%lu,%lu",procStat->priority,procStat->nice,procStat->numThreads,procStat->vsize,procStat->rss);
    nBytes += fprintf(filePtr, ",%lu,%lu,%lu,%lu,%lu",procStat->rsslim,procStat->vmpeak,procStat->rsspeak,procStat->signal,procStat->blocked);
    nBytes += fprintf(filePtr, ",%lu,%lu,%d,%u,%u",procStat->sigignore,procStat->sigcatch,procStat->cpusAllowed,procStat->rtPriority,procStat->policy);
    nBytes += fprintf(filePtr, ",%lu,%llu,%llu,%llu,%llu",procStat->guestTime,procStat->delayacctBlkIOTicks,procStat->io_rchar,procStat->io_wchar,procStat->io_syscr);
    nBytes += fprintf(filePtr, ",%llu,%llu,%llu,%llu,%lu",procStat->io_syscw,procStat->io_readBytes,procStat->io_writeBytes,procStat->io_cancelledWriteBytes, procStat->m_size);
    nBytes += fprintf(filePtr, ",%lu,%lu,%lu,%lu,%lu",procStat->m_resident,procStat->m_share,procStat->m_text,procStat->m_data, procStat->realUid); 
    nBytes += fprintf(filePtr, ",%lu,%lu,%lu\n",procStat->effUid, procStat->realGid, procStat->effGid);
    return nBytes;
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
    if (filePtr != NULL) fclose(filePtr);
}
