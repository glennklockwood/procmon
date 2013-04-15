#include "procfmt.hh"
#include <iostream>

int main(int argc, char** argv) {
    try {
        ProcFile file("test.h5", "localhost", "procmonDriver");

        int n = 5;
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
        }
        file.write_procstat(stat, n);
        file.write_procdata(data, n);
    } catch (ProcFileException& e) {
        std::cout << e.what() << std::endl;
    }
}
