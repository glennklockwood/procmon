/*
 * amqpLogger.h
 *
 * Author: Douglas Jacobsen <dmjacobsen@lbl.gov>, NERSC User Services Group
 * 2013/07/08
 * Copyright (C) 2013, The Regents of the University of California
 *
 */

#ifndef __AMQPLOGGER_H__
#define __AMQPLOGGER_H__

#include <amqp_tcp_socket.h>
#include <amqp_framing.h>

#ifdef __cplusplus
extern "C" {
#endif

#define AMQP_BUFFER_SIZE 3145728 /* 3 Mb */
#define TEXT_BUFFER_SIZE 8192    /* 8 kB */
#define HEADER_BUFFER_SIZE 512

/* public interface */
typedef enum _amqpServerSelection {
    AMQP_SERVER_REMOTEONLY = -1,
    AMQP_SERVER_AUTO =  0,
} amqpServerSelection;

#define AMQP_CONNECTED_NONE 0
#define AMQP_CONNECTED_LOCAL 1
#define AMQP_CONNECTED_REMOTE 2

typedef struct amqpState {
    /* amqp variables */
	amqp_connection_state_t conn;
	amqp_socket_t* socket;
	amqp_rpc_reply_t status;
    char amqp_error_message[HEADER_BUFFER_SIZE];
    unsigned int connected;
    int output_warnings;
} amqpState;


typedef struct amqpLogger {
    /* configurations & status */
    amqpServerSelection use_server;
    int retries;
    char delim;
    
    /* log routing headers */
    char system[HEADER_BUFFER_SIZE];
    char hostname[HEADER_BUFFER_SIZE];
    char identifier[HEADER_BUFFER_SIZE];
    char subidentifier[HEADER_BUFFER_SIZE];
    // fifth routing key, tag, is supplied at log-time

    // amqp variables
    amqpState amqp;


    /* local server variables */
} amqpLogger;

typedef struct amqpReceiver {
    amqp_bytes_t queue;
    int consuming;
    amqpState amqp;
} amqpReceiver;

int amqpLogger_initialize(amqpLogger *log, amqpServerSelection use_server, 
    char const *system, char const *hostname, char const *identifier, 
    char const *subidentifier, char delim);

void amqpLogger_destruct(amqpLogger *log);

int amqpLogger_setidentifiers(amqpLogger *log, char const *identifier, 
    char const *subidentifier);

int amqpLogger_log(amqpLogger *log, time_t logtime, char const *tag,
    char const *message);

int amqpLogger_logf(amqpLogger *log, time_t logtime, char const *tag,
    char const *format, ...);

int amqpLogger_lograw(amqpLogger *log, char const *tag, void *bytes,
    size_t n_bytes);

int amqpLogger_routingraw_lograw(amqpLogger *log, char const *routing, void *bytes,
    size_t n_bytes);

int amqpReceiver_initialize(amqpReceiver *recv);
void amqpReceiver_destruct(amqpReceiver *recv);
int amqpReceiver_add_routing_key(amqpReceiver*, const char *system, const char *hostname, const char *identifier, const char *subidentifier, const char *tag);
size_t amqpReceiver_get_message(amqpReceiver*, char *system, char *hostname, char *identifier, char *subidentifier, char *tag, char **message_buffer, size_t *buffer_size);


#ifdef __cplusplus
}
#endif

#endif /* __AMQPLOGGER_H__ */

