#include <amqpLogger.h>
#include <stdlib.h>
#include <stdio.h>

int main(int argc, char** argv) {
    amqpReceiver receiver;
    char system[HEADER_BUFFER_SIZE];
    char hostname[HEADER_BUFFER_SIZE];
    char identifier[HEADER_BUFFER_SIZE];
    char subidentifier[HEADER_BUFFER_SIZE];
    char tag[HEADER_BUFFER_SIZE];
    char *message_buffer = NULL;
    size_t message_buffer_size = 0;
    int received = 0;
    size_t bytes_received = 0;
    size_t bytes = 0;

    amqpReceiver_initialize(&receiver);

    amqpReceiver_add_routing_key(&receiver, NULL, NULL, NULL, NULL, "example");

    while ((bytes = amqpReceiver_get_message(&receiver, system, hostname, identifier, subidentifier, tag, &message_buffer, &message_buffer_size)) > 0) {
        bytes_received += bytes;
        received++;
        if (strncmp(message_buffer, "end", 3) == 0) {
            break;
        }
    }
    free(message_buffer);

    printf("Received %d messages; %lu bytes\n", received, bytes_received);

    amqpReceiver_destruct(&receiver);
}
