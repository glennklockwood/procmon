#include "config.h"
#include "amqpLogger.h"
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <signal.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <getopt.h>
#include <pthread.h>
#include <pwd.h>
#include <grp.h>


#define BUFFER_SIZE 1024

pthread_mutex_t logger_mutex = PTHREAD_MUTEX_INITIALIZER;
amqpLogger logger;
int done = 0;
int verbose = 0;

void sig_handler(int signum) {
    amqpLogger_logf(&logger, 0, "amqpLogger", "amqpLogger server exiting, caught signal %d", signum);
    done = 1;
}



static void daemonize();
void run_server();
void *server_thread(void *args);

void usage(int exitstatus) {
    printf("%s %d.%d\n\n", AMQPLOGGER_NAME, AMQPLOGGER_VERSION_MAJOR, AMQPLOGGER_VERSION_MINOR);
    printf("amqpLogger <options> <message tag> strings... (will be sent delimiter separated)\n");
    printf("\nOPTIONS:\n");
    printf("  --help/-h                   Print this help\n");
    printf("  --verbose/-v                Verbose output (not used right now)\n");
    printf("  --version/-V                Print version\n");
    printf("  --daemon/-d                 Run as amqpLogger daemon\n");
    printf("  --delimiter/-t <char>       Use another delimiter instead of ','\n");
    printf("  --system/-s <string>        Override SYSTEM tag (avoid this)\n");
    printf("  --host/-H <string>          Override HOST tag (avoid this)\n");
    printf("  --identifier/-I <string>    Override IDENTIFIER tag, defaults to job id or INTERACTIVE if not in a job context\n");
    printf("  --subidentifier/-S <string> Override SUBIDENTIFIER tag, defaults to task id or NA if not in a job context\n");
    printf("  --pidfile/-p <path>         Use a pid file to control and record startup, placed in <path>\n");
    printf(" --user/-u <string>           If running as a service, drop priviliges to named user\n");
    printf(" --group/-g <string>          If running as a service, drop priviliges to named group\n");
    exit(exitstatus);
}

void pidfile(const char *pidfilename) {
    if (strlen(pidfilename) == 0) return;

    FILE *pidfile = NULL;
    char buffer[BUFFER_SIZE];
    pid_t my_pid = getpid();

    /* try to open existing pidfile */
    if ((pidfile = fopen(pidfilename, "r")) != NULL) {

        /* try to read the pid */
        int readBytes = fread(buffer, sizeof(char), BUFFER_SIZE, pidfile);
        fclose(pidfile);

        if (readBytes > 0) {
            pid_t pid = atoi(buffer);

            if (pid != 0 && pid != my_pid) {
                struct stat st_stat;
                snprintf(buffer, BUFFER_SIZE, "/proc/%d/status", pid);
                if (stat(buffer, &st_stat) == 0) {
                    /* the process still exists! */
                    fprintf(stderr, "Process %d is still running, exiting.\n", pid);
                    exit(0);
                } else {
                    unlink(pidfilename);
                }
            }
        }
    }
    if ((pidfile = fopen(pidfilename, "w")) != NULL) {
        fprintf(pidfile, "%d\n", my_pid);
        fclose(pidfile);
        return;
    }
    fprintf(stderr, "FAILED to write pidfile %s, exiting.", pidfilename);
    exit(1);
}

int main(int argc, char **argv) {

    /* configuration variables */
    int is_daemon = 0;
    char delim = ',';
    char pid_filename[1024];
    char system[HEADER_BUFFER_SIZE] = "";
    char host[HEADER_BUFFER_SIZE] = "";
    char identifier[HEADER_BUFFER_SIZE] = "";
    char subidentifier[HEADER_BUFFER_SIZE] = "";
    char tag[HEADER_BUFFER_SIZE] = "";
    char buffer[AMQP_BUFFER_SIZE];
    char *ptr = buffer;
    uid_t tgt_uid = 0;
    gid_t tgt_gid = 0;

    pid_filename[0] = 0;
    int c, idx, l_idx=0;
    int option_index = 0;
    static struct option long_options[] = {
        {"help", 0, 0, 'h'},
        {"verbose", 0, 0, 'v'},
        {"version", 0, 0, 'V'},
        {"daemon", 0, 0, 'd'},
        {"delimiter", 1, 0, 't'},
        {"system", 1, 0, 's'},
        {"host", 1, 0, 'H'},
        {"identifier", 1, 0, 'I'},
        {"subidentifier", 1, 0, 'S'},
        {"pidfile", 1, 0, 'p'},
		{"user",1, 0, 'u'},
		{"group",1,0,'g'},

    };
    for ( ; ; ) {
        c = getopt_long(argc, argv, "hvVdt:s:H:I:S:p:u:g:", long_options, &option_index);
        if (c == -1) {
            break;
        }
        switch (c) {
            case 'h':
                usage(0);
                break;
            case 'v':
                verbose = 1;
                break;
            case 'V':
                printf("%s version %d.%d\n", AMQPLOGGER_NAME, AMQPLOGGER_VERSION_MAJOR, AMQPLOGGER_VERSION_MINOR);
                exit(0);
                break;
            case 'd':
                is_daemon = 1;
                break;
            case 't':
                if (optarg == NULL) {
                    printf("-t/--delim requires an argument\n\n");
                    usage(1);
                }
                delim = optarg[0];
                break;
            case 's':
                if (optarg == NULL) {
                    printf("-s/--system requires an argument\n\n");
                    usage(1);
                }
                strncpy(system, optarg, HEADER_BUFFER_SIZE);
                system[HEADER_BUFFER_SIZE-1] = 0;
                break;
            case 'H':
                if (optarg == NULL) {
                    printf("-H/--host requires an argument\n\n");
                    usage(1);
                }
                strncpy(host, optarg, HEADER_BUFFER_SIZE);
                host[HEADER_BUFFER_SIZE-1] = 0;
                break;
            case 'I':
                if (optarg == NULL) {
                    printf("-I/--identifier requires an argument\n\n");
                    usage(1);
                }
                strncpy(identifier, optarg, HEADER_BUFFER_SIZE);
                identifier[HEADER_BUFFER_SIZE-1] = 0;
                break;
            case 'S':
                if (optarg == NULL) {
                    printf("-S/--subidentifier requires an argument\n\n");
                    usage(1);
                }
                strncpy(subidentifier, optarg, HEADER_BUFFER_SIZE);
                subidentifier[HEADER_BUFFER_SIZE-1] = 0;
                break;
            case 'p':
                if (optarg == NULL) {
                    printf("-p/--pidfile requires an argument\n\n");
                    usage(1);
                }
                strncpy(pid_filename, optarg, 1024);
                pid_filename[1023] = 0;
                break;
            case 'u':
                if (optarg == NULL) {
                    printf("-u/--user requires an argument!\n\n");
                    usage(1);
                } else {
                    struct passwd *user_data = getpwnam(optarg);
                    if (user_data == NULL) {
                        int uid = atoi(optarg);
                        if (uid > 0) {
                            user_data = getpwuid(uid);
                            if (user_data != NULL) {
                                tgt_uid = user_data->pw_uid;
                            }
                        }
                    } else {
                        tgt_uid = user_data->pw_uid;
                    }
                    if (tgt_uid == 0) {
                        printf("User %s is invalid user!\n\n", optarg);
                        usage(1);
                    }
                }
                break;
            case 'g':
                /* deal with GROUP arg */
                if (optarg != NULL && strlen(optarg) > 0) {
                    struct group *group_data = getgrnam(optarg);
                    if (group_data == NULL) {
                        int gid = atoi(optarg);
                        if (group_data != NULL) {
                            tgt_gid = group_data->gr_gid;
                        }
                    } else {
                        tgt_gid = group_data->gr_gid;
                    }
                } else {
                    printf("-g/--group requires an argument!\n\n");
                    usage(1);
                }
                break;
            default:
                printf("Invalid arguments!\n\n");
                usage(1);
        }
    }
    for (idx = optind, l_idx = 0; idx < argc; ++idx, ++l_idx) {
        if (l_idx == 0) {
            strncpy(tag, argv[idx], HEADER_BUFFER_SIZE);
            tag[HEADER_BUFFER_SIZE-1] = 0;
        } else {
            *ptr++ = delim;
            *ptr = 0;
        }
        ptr += snprintf(ptr, AMQP_BUFFER_SIZE - (ptr - buffer), "%s", argv[idx]);
    }   

    if (is_daemon) daemonize();
    if (strlen(pid_filename) > 0) {
        pidfile(pid_filename);
    }

    memset(&logger, 0, sizeof(amqpLogger));
    amqpLogger_initialize(&logger, is_daemon ? AMQP_SERVER_REMOTEONLY : AMQP_SERVER_AUTO, system, host, identifier, subidentifier, delim);

    if (is_daemon) {
        signal(SIGINT, sig_handler);
        signal(SIGTERM, sig_handler);
        signal(SIGXCPU, sig_handler);
        signal(SIGUSR1, sig_handler);
        signal(SIGUSR2, sig_handler);
        signal(SIGHUP, sig_handler);
        signal(SIGPIPE, SIG_IGN);
        amqpLogger_log(&logger, 0, "amqpLogger", "amqpLogger server starting");
        unlink(SOCKET_PATH);
        run_server();
        unlink(SOCKET_PATH);
    } else {
        int ret;
        if (verbose) printf("trying to send %s: %s\n", tag, buffer);
        ret = amqpLogger_log(&logger, 0, tag, buffer);
        if (verbose) printf("got return code: %d\n", ret);
    }
    return 0;
}

static void daemonize() {
    pid_t pid, sid;

    if (getppid() == 1) {
        return; // already daemonized
    }
    pid = fork();
    if (pid < 0) {
        exit(1); // failed to fork
    }
    if (pid > 0) {
        exit(0); // this is the parent, so exit
    }
    umask(0);

    sid = setsid();
    if (sid < 0) {
        exit(1);
    }

    if ((chdir("/")) < 0) {
        exit(1);
    }

    freopen("/dev/null", "r", stdin);
    freopen("/dev/null", "w", stdout);
    freopen("/dev/null", "w", stderr);
}

void run_server() {
    struct sockaddr_un local, remote;
    socklen_t remote_size;
    int errors = 0;
    int s_local = -1;
    int s_remote = -1;
    pthread_t thread_id;

    memset(&local, 0, sizeof(struct sockaddr_un));

    s_local = socket(AF_UNIX, SOCK_STREAM, 0);
    if (s_local == -1) {
        fprintf(stderr, "Failed to create socket, exiting.\n");
        exit(1);
    }
    local.sun_family = AF_UNIX;
    strncpy(local.sun_path, SOCKET_PATH, sizeof(local.sun_path) - 1);

    if (bind(s_local, (struct sockaddr *) &local, sizeof(struct sockaddr_un)) == -1) {
        fprintf(stderr, "Failed to bind to socket at %s, exiting.\n", SOCKET_PATH);
        exit(1);
    }

    if (listen(s_local, SOCKET_BACKLOG) == -1) {
        fprintf(stderr, "Failed to listen to socket at %s, exiting.\n", SOCKET_PATH);
        exit(1);
    }

    while (done == 0) {
        s_remote = accept(s_local, (struct sockaddr *) &remote, &remote_size);
        if (s_remote == -1) {
            if (++errors > 5) {
                fprintf(stderr, "Too many failed socket connections, exiting.\n");
                exit(1);
            }
            continue;
        }
        pthread_create(&thread_id, NULL, server_thread, &s_remote);
        errors = 0;
    }
    close(s_local);
}

void *server_thread(void *args) {
    int *s_remote_ptr = (int*) args;
    int s_remote = *s_remote_ptr;
    int len;
    char buffer[AMQP_BUFFER_SIZE];
    char routing_key[1024];
    char *ptr;
    ssize_t read_bytes;
    ssize_t l_read_bytes;

    /* read connection header */
    read_bytes = recv(s_remote, buffer, 1024, 0);
    if (strcmp(buffer, "AMQPLOGGER") != 0) {
        send(s_remote, "FAIL", 5, 0);
        close(s_remote);
        return NULL;
    }
    send(s_remote, "OK", 3, 0);

    /* get the routing key */
    read_bytes = recv(s_remote, routing_key, 1024, 0);
    if (s_remote < 0) return NULL; // sigpipe must have happened
    if (read_bytes <= 0) {
        send(s_remote, "FAIL", 5, 0);
        close(s_remote);
        return NULL;
    }
    send(s_remote, "OK", 3, 0);
    if (s_remote < 0) return NULL; // sigpipe must have happened

    /* get the # of bytes in the message */
    read_bytes = recv(s_remote, buffer, AMQP_BUFFER_SIZE, 0);
    if (s_remote < 0) return NULL; // sigpipe must have happened
    len = atoi(buffer);
    if (len <= 0 || len >= AMQP_BUFFER_SIZE) {
        send(s_remote, "FAIL", 3, 0);
        close(s_remote);
        return NULL;
    }
    if (len > 0) {
        send(s_remote, "OK", 3, 0);
        if (s_remote < 0) return NULL; // sigpipe must have happened
        ptr = buffer;
        while (ptr - buffer < len) {
            read_bytes = recv(s_remote, ptr, AMQP_BUFFER_SIZE - (ptr - buffer), 0);
            if (s_remote < 0) continue; // sigpipe must have happened
            ptr += read_bytes;
            if (read_bytes == 0) {
                break;
            }
        }
        if (ptr - buffer < len) {
            send(s_remote, "FAIL", 5, 0);
            close(s_remote);
            return NULL;
        }
        send(s_remote, "OK", 3, 0);
        close(s_remote);
        buffer[len] = 0;
        pthread_mutex_lock(&logger_mutex);
        amqpLogger_routingraw_lograw(&logger, routing_key, buffer, len);
        pthread_mutex_unlock(&logger_mutex);
    }
    return NULL;
}
