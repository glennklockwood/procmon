#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>

#include <vector>
#include <string>
#include <sstream>

using namespace std;

char **convertVecToArgs(const vector<string>& args) {
    char **ret = (char **) malloc(sizeof(char *) * (args.size()+1));
    memset(ret, 0, sizeof(char *) * (args.size() + 1));

    size_t idx = 0;
    for (idx = 0; idx < args.size(); idx++) {
        ret[idx] = strdup(args[idx].c_str());
    }
    ret[idx] = NULL; //trailing null
    return ret;
}

int main(int argc, char **argv) {
    const string procmonPath = "/global/syscom/sc/nsg/opt/procmon/deploy/prod/bin/procmon";
    pid_t pid = 1;

    /* c++11 has better ways of doing this, but I don't want this wrapper to use
       c++11 features.... */ 
    stringstream ss;
    ss << pid;
    string pidstr = ss.str();

    string identifier;
    string subidentifier;
    char *ptr = NULL;

    /* put jobid in identifier, stopping at any special characters (.[, etc) */
    if ((ptr = getenv("JOB_ID")) != NULL) {
        identifier = ptr;
    } else if ((ptr = getenv("PBS_JOBID")) != NULL) {
        identifier = ptr;
    } else if ((ptr = getenv("SLURM_JOB_ID")) != NULL) {
        identifier = ptr;
    } else {
        identifier = "INTERACTIVE";
    }
    size_t idx = 0;
    for (idx = 0; idx < identifier.length(); idx++) {
        if (!isalnum(identifier[idx])) {
            break;
        }
    }
    identifier = identifier.substr(0, idx);

    /* put array task # in subidentifier */
    if ((ptr = getenv("PBS_ARRAYID")) != NULL) {
        subidentifier = ptr;
    } else {
        subidentifier = "1";
    }

    vector<string> procmonArgs;
    vector<string> realArgs;

    procmonArgs.push_back(procmonPath);
    procmonArgs.push_back("-p"); // all processes under my pid (ORd with other tracking options)
    procmonArgs.push_back(pidstr);
    procmonArgs.push_back("-s"); // all processes with my session id
    procmonArgs.push_back("-c"); // cray mode
    procmonArgs.push_back("-I"); // identifier
    procmonArgs.push_back(identifier);
    procmonArgs.push_back("-S"); // subidentifier
    procmonArgs.push_back(subidentifier);
    procmonArgs.push_back("-i"); // length of initial phase
    procmonArgs.push_back("60");
    procmonArgs.push_back("-F"); // frequency during initial phase
    procmonArgs.push_back("2");
    procmonArgs.push_back("-f"); // frequency after initial phase
    procmonArgs.push_back("10");
    procmonArgs.push_back("-W"); // read 100 file descriptors per process
    procmonArgs.push_back("100");

    for (int i = 1; i < argc; i++) {
        realArgs.push_back(argv[i]);
    }

    /* start double-forking and exec process */
    pid_t cpid = fork();
    if (cpid < 0) {
        exit(1); // failed to fork
    }
    if (cpid > 0) {
        int status = 0;
        char **execArgs = convertVecToArgs(realArgs);
        waitpid(cpid, &status, 0);
        execvp(execArgs[0], execArgs);
    } else {
        cpid = fork();
        if (cpid > 0) {
            _exit(0);
        } else {
            char **execArgs = convertVecToArgs(procmonArgs);
            execvp(execArgs[0], execArgs);
        }
    }
}
