#include <amqpLogger.h>
#include <stdio.h>
#include <stdlib.h>

int main(int argc, char** argv) {
    amqpLogger logger;
    struct timeval start, end;
    memset(&start, 0, sizeof(struct timeval));
    memset(&end, 0, sizeof(struct timeval));
    int i = 0;
    double timediff = 0.0;
    int message_size = atoi(argv[1]);
    int message_count = atoi(argv[2]);

    char message[message_size];
    for (i = 0; i < message_size; i++) {
        message[i] = rand()/127 + 1;
    }
    message[message_size-1] = 0;

    gettimeofday(&start, NULL);

    amqpLogger_initialize(&logger, AMQP_SERVER_AUTO, "", "", "", "", '|');

    amqpLogger_setidentifiers(&logger, "test_id", "test_subid");
    for (i = 0; i < message_count; i++) {
        if (amqpLogger_lograw(&logger, "example", message, message_size)) {
            printf("failed to send.\n");
        }
        //usleep(100);
    }
    gettimeofday(&end, NULL);
    amqpLogger_lograw(&logger, "example", "end", 4);

    timediff = end.tv_sec - start.tv_sec + 1e-6*(end.tv_usec - start.tv_usec);
    printf("Took %0.2fs to send %d messages\n", timediff, message_count);
}
