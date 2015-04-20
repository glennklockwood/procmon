#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

int main(int argc, char **argv) {
    char procmonPath[1024];
    char pidstr[24];
    char identifier[24];
    char subidentifier[24];
    pid_t mypid;
    char **procmonArgs = (char **) malloc(sizeof(char *) * 16);
    char **realArgv = argv + 1;
    int pidx = 0;
    if (getenv("JOB_ID") != NULL) {
        snprintf(identifier, 24, "%s", getenv("JOB_ID"));
    } else if (getenv("PBS_JOBID") != NULL) {
        snprintf(identifier, 24, "%s", getenv("PBS_JOBID"));
    } else if (getenv("SLURM_JOB_ID") != NULL) {
        snprintf(identifier, 24, "%s", getenv("SLURM_JOB_ID"));
    } else {
        snprintf(identifier, 24, "%s", "INTERACTIVE");
    }
    snprintf(subidentifier, 24, "%d", 1);
    snprintf(procmonPath, 1024, "/global/syscom/sc/nsg/opt/procmon/deploy/prod/bin/procmon");
    snprintf(pidstr, 24, "%d", 1);//getpid());

    procmonArgs[pidx++] = procmonPath;
    procmonArgs[pidx++] = "-p";
    procmonArgs[pidx++] = pidstr;
    procmonArgs[pidx++] = "-c";
    procmonArgs[pidx++] = "-I";
    procmonArgs[pidx++] = identifier;
    procmonArgs[pidx++] = "-S";
    procmonArgs[pidx++] = subidentifier;
    procmonArgs[pidx++] = "-i";
    procmonArgs[pidx++] = "60";
    procmonArgs[pidx++] = "-F";
    procmonArgs[pidx++] = "2";
    procmonArgs[pidx++] = "-f";
    procmonArgs[pidx++] = "10";
    procmonArgs[pidx++] = NULL;
    mypid = fork();
    if (mypid < 0) {
        exit(1); // failed to fork
    }
    if (mypid > 0) {
        int status = 0;
        waitpid(mypid, &status, 0);
        execvp(realArgv[0], realArgv);
    } else {
        pid_t mypid2 = fork();
        if (mypid2 > 0) {
            _exit(0);
        } else {
            char **arg = procmonArgs;
            execvp(procmonArgs[0], procmonArgs);
        }
    }
}
