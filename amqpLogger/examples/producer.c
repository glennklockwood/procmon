#include <amqpLogger.h>
#include <stdio.h>
#include <stdlib.h>
#include <signal.h>

amqpLogger logger;

void sighandler(int signum) {
    fprintf(stderr, "GoodBye\n");
    amqpLogger_destruct(&logger);
    exit(1);
}

int main(int argc, char** argv) {
    struct timeval start, end;
    memset(&start, 0, sizeof(struct timeval));
    memset(&end, 0, sizeof(struct timeval));
    int i = 0;
    double timediff = 0.0;
    int message_size = atoi(argv[1]);
    int message_count = atoi(argv[2]);

    char message[message_size];
    signal(SIGTERM, sighandler);
    signal(SIGINT, sighandler);
    for (i = 0; i < message_size; i++) {
        message[i] = rand()/127 + 1;
    }
    message[message_size-1] = 0;

    gettimeofday(&start, NULL);


    amqpLogger_initialize(&logger, AMQP_SERVER_AUTO, "", "", "", "", '|');
    logger.amqp.output_warnings = 1;

    amqpLogger_setidentifiers(&logger, "test_id", "test_subid");
    for (i = 0; i < message_count; i++) {
        if (amqpLogger_lograw(&logger, "example", message, message_size)) {
            printf("failed to send.\n");
        }
        usleep(100000);
    }
    gettimeofday(&end, NULL);
    amqpLogger_lograw(&logger, "example", "end", 4);

    timediff = end.tv_sec - start.tv_sec + 1e-6*(end.tv_usec - start.tv_usec);
    printf("Took %0.2fs to send %d messages\n", timediff, message_count);

    amqpLogger_destruct(&logger);
}
