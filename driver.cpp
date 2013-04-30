#include "ProcData.hh"
#include "ProcIO.hh"
#include <iostream>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <strings.h>

int main(int argc, char** argv) {
    int n = 300;
    try {
        ProcAMQPIO output("genepool10.nersc.gov", 5672, "/", "guest", "guest", "procmon", 131072, FILE_MODE_WRITE);
		output.set_context("localhost", "procmon", "driver");

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
        output.write_procstat(stat, n);
        output.write_procdata(data, n);
    } catch (ProcIOException& e) {
        std::cout << e.what() << std::endl;
    }
}
