#include "ProcData.hh"
#include "ProcIO.hh"
#include <iostream>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <strings.h>

using namespace std;

int main(int argc, char** argv) {
    int n = 300;
    try {
		ProcHDF5IO reader(argv[1], FILE_MODE_READ);
		reader.set_context(argv[2], "whatever", "whatever");

		int nStat = reader.get_nprocstat();
		int nData = reader.get_nprocdata();

		cout << "procstat: " << nStat << endl;
		cout << "procdata: " << nData << endl;

		procdata all[nData];
		procstat allStat[nStat];
		reader.read_procdata(all, 0, nData);
		reader.read_procstat(allStat, 0, nStat);

		for (int i = 0; i < nData; i++) {
			cout << "[" << all[i].pid << "] " << all[i].execName << endl;
			for (int j = 0; j < all[i].cmdArgBytes; j++ ){
				if (all[i].cmdArgs[j] == 0) {
					all[i].cmdArgs[j] = ' ';
				}
			}
			all[i].cmdArgs[all[i].cmdArgBytes] = 0;
			cout << "    " << all[i].cmdArgs << endl;
			cout << "    " << all[i].identifier << "." << all[i].subidentifier << endl;
		}

//		for (int i = 0 ; i < nStat; i++) {
//			cout << "[" << allStat[i].pid << "] " << all[i].

    } catch (ProcIOException& e) {
        std::cout << e.what() << std::endl;
    }
}
