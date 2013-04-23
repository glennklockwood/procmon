#include "procfmt.hh"
#include <iostream>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <strings.h>

int main(int argc, char** argv) {
    int n = 2;
    try {
        ProcFile file("test.h5", "localhost", "procmonDriver", FILE_FORMAT_HDF5, FILE_MODE_WRITE);

        procstat stat[n];
        procdata data[n];
        bzero(&stat, sizeof(procstat)*n);
        bzero(&data, sizeof(procdata)*n);
        for (int i = 0; i < n; i++) {
            stat[i].pid = i;
            data[i].pid = i;
            if (i > 0) {
                stat[i].ppid = i-1;
                data[i].ppid = i-1;
            } else {
                stat[i].ppid = 0;
                data[i].ppid = 0;
            }
            stat[i].realGid = i+5;
            stat[i].effGid = i+1;
            stat[i].state = 'R';
            snprintf(data[i].execName, EXEBUFFER_SIZE, "MyExec%d", i);
            snprintf(data[i].cmdArgs, BUFFER_SIZE, "MyExec%d", i);
            data[i].cmdArgBytes = strlen(data[i].cmdArgs);
            snprintf(data[i].exePath, BUFFER_SIZE, "/path/to/MyExec%d", i);
            snprintf(data[i].cwdPath, BUFFER_SIZE, "/path/to/My/Home/%d", i);
        }
        file.write_procstat(stat, n);
        file.write_procdata(data, n);
    } catch (ProcFileException& e) {
        std::cout << e.what() << std::endl;
    }

    try {
        ProcFile file("test.h5", "localhost", "procmonDriver", FILE_FORMAT_HDF5, FILE_MODE_READ);
        procdata procData[n];
        procstat procStat[n];
        ProcFileRecordType type;
        std::cout << "read procdata: " << file.read_procdata(procData, 0, 2) << std::endl;
        std::cout << "read procstat: " << file.read_procstat(procStat, 0, 2) << std::endl;
        for (int i = 0; i < n; i++) {
                std::cout << "execName: " << procData[i].execName << std::endl;
                std::cout << "exe: " << procData[i].exePath << std::endl;
                std::cout << "cwd: " << procData[i].cwdPath << std::endl;
                std::cout << "effGid: " << procStat[i].effGid << std::endl;
                std::cout << "state: " << procStat[i].state << ";" << (int) procStat[i].state << std::endl;
        }
    } catch (ProcFileException& e) {
        std::cout << e.what() << std::endl;
        std::cout << "errno: " << errno << std::endl;
    }
    
}
