/* a simple program to attempt to wait no longer than a prespecified time for
   another process to finish.  kills the target process aggressively if the
   timer ticks over 

Author: Doug Jacobsen
Copyright (C) 2014 - The Regents of the University of California
*/

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <signal.h>

pid_t child = 0;

void sighandler(int signum) {
    if (signum == SIGCHLD) {
        int status;
        pid_t pid = waitpid(child, &status, WUNTRACED);
        if (WIFEXITED(status)) {
            exit(WEXITSTATUS(status));
        }
    }
}


int main(int argc, char **argv) {
    int timeout = 0;
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <timeout (s)> <command> [commands args]\n", argv[0]);
        exit(1);
    }
    timeout = atoi(argv[1]);
    if (timeout <= 0) {
        fprintf(stderr, "Timeout must be an integer > 0\n");
        exit(1);
    }

    child = fork();
    if (child == 0) { // this *is* the child -- exec to target
        execvp(argv[2], &(argv[2]));
    } else if (child > 0) {
        signal(SIGCHLD, sighandler);
        sleep(timeout);
        signal(SIGCHLD, SIG_IGN);
        kill(child, 9); // vicious termination
        exit(137);
    } else {
        fprintf(stderr, "Failed to frok the target process. Exiting...\n");
        exit(1);
    }
}
