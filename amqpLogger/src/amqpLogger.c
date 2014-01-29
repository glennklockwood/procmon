#include "config.h"
#include "amqpLogger.h"
#include <amqp_tcp_socket.h>
#include <amqp_framing.h>
#include <string.h>
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <unistd.h>
#include "config.h"

/* private interface declarations */
int _amqp_open(amqpState *amqp);
int _amqp_eval_status(amqpState* amqp, amqp_rpc_reply_t status);
void _amqp_close(amqpState *amqp);
int _local_send(amqpLogger *log, char const *routing, char const *bytes, int n_bytes);

/* public interface */
int amqpLogger_initialize(amqpLogger *log, amqpServerSelection use_server, 
    char const *system, char const *hostname, char const *identifier, 
    char const *subidentifier, char delim) {

    char *dotptr;

    if (log == NULL) {
        return -1;
    }
    log->use_server = use_server;
    log->amqp.connected = AMQP_CONNECTED_NONE;
    log->delim = delim;
    log->amqp.output_warnings = 0;
    log->retries = AMQPLOGGER_RETRIES;

    /* zero out the amqp portions of the structure */
    memset((void *) &(log->amqp.conn), 0, sizeof(amqp_connection_state_t));
    log->amqp.socket = NULL;
    memset((void *) &(log->amqp.status), 0, sizeof(amqp_rpc_reply_t));

    if (log->use_server == AMQP_SERVER_REMOTEONLY) {
        _amqp_open(&(log->amqp));
        amqp_confirm_select(log->amqp.conn, 1);
    }

    /* set the system */
    if (system == NULL || strlen(system) == 0) {
        char *nersc_host = getenv("NERSC_HOST");
        if (nersc_host != NULL && strlen(nersc_host) != 0) {
            snprintf(log->system, HEADER_BUFFER_SIZE, "%s", nersc_host);
        } else {
            snprintf(log->system, HEADER_BUFFER_SIZE, "%s", "UNKNOWN_SYSTEM");
        }
    } else {
        snprintf(log->system, HEADER_BUFFER_SIZE, "%s", system);
    }

    /* set the hostname */
    if (hostname == NULL || strlen(hostname) == 0) {
        if (gethostname(log->hostname, HEADER_BUFFER_SIZE) != 0) {
            snprintf(log->hostname, HEADER_BUFFER_SIZE, "%s", "UNKNOWN_HOST");
        }
    } else {
        snprintf(log->hostname, HEADER_BUFFER_SIZE, "%s", hostname);
    }

    /* ensure that the set system and hostname do not overflow the buffer */
    log->system[HEADER_BUFFER_SIZE - 1] = 0;
    log->hostname[HEADER_BUFFER_SIZE - 1] = 0;

    /* replace the first '.' (if it exists) with a string termination; this
       ensures that the routing key is valid */
    dotptr = strchr(log->system, '.');
    if (dotptr != NULL) {
        *dotptr = 0;
    }
    dotptr = strchr(log->hostname, '.');
    if (dotptr != NULL) {
        *dotptr = 0;
    }
    amqpLogger_setidentifiers(log, identifier, subidentifier);
    return 0;
}

void amqpLogger_destruct(amqpLogger *log) {
    if (log->amqp.connected & AMQP_CONNECTED_REMOTE) {
        _amqp_close(&(log->amqp));
    }
}

int amqpLogger_setidentifiers(amqpLogger *log, char const *identifier, 
    char const *subidentifier) {

    char *dotptr;

    if (identifier == NULL || strlen(identifier) == 0) {
        char *job_id = getenv("JOB_ID"); // gridengine
        if (job_id == NULL) {
            job_id = getenv("PBS_JOBID");
        }
        if (job_id != NULL) {
            snprintf(log->identifier, HEADER_BUFFER_SIZE, "%s", job_id);
        } else {
            snprintf(log->identifier, HEADER_BUFFER_SIZE, "%s", "INTERACTIVE");
        }
    } else {
        snprintf(log->identifier, HEADER_BUFFER_SIZE, "%s", identifier);
    }
    if (subidentifier == NULL || strlen(subidentifier) == 0) {
        char *job_id = getenv("JOB_ID"); // gridengine
        if (job_id == NULL) {
            job_id = getenv("PBS_JOBID");
        }
        if (job_id != NULL) {
            char *task_id = getenv("SGE_TASK_ID");
            if (task_id == NULL) {
                task_id = getenv("PBS_ARRAYID");
            }
            if (task_id == NULL || strcmp(task_id, "undefined") == 0) {
                snprintf(log->subidentifier, HEADER_BUFFER_SIZE, "0");
            } else {
                snprintf(log->subidentifier, HEADER_BUFFER_SIZE, "%s", task_id);
            }
        } else {
            snprintf(log->subidentifier, HEADER_BUFFER_SIZE, "NA");
        }
    } else {
        snprintf(log->subidentifier, HEADER_BUFFER_SIZE, "%s", subidentifier);
    }

    /* ensure that the set identifier and subidentifier do not overflow the 
       buffer */
    log->identifier[HEADER_BUFFER_SIZE - 1] = 0;
    log->subidentifier[HEADER_BUFFER_SIZE - 1] = 0;

    /* replace the first '.' (if it exists) with a string termination; this
       ensures that the routing key is valid */
    dotptr = strchr(log->system, '.');
    if (dotptr != NULL) {
        *dotptr = 0;
    }
    dotptr = strchr(log->hostname, '.');
    if (dotptr != NULL) {
        *dotptr = 0;
    }
}

int amqpLogger_log(amqpLogger *log, time_t logtime, char const *tag,
    char const *message) {

    if (log == NULL || tag == NULL || message == NULL) {
        return 1;
    }

    return amqpLogger_logf(log, logtime, tag, "%s", message);
}

int amqpLogger_logf(amqpLogger *log, time_t logtime, char const *tag, char const *format, ...) {
    char buffer[AMQP_BUFFER_SIZE];
    char *ptr = buffer;
    va_list ap;
    int vbytes;

    if (log == NULL || tag == NULL || format == NULL
            || strlen(tag) == 0) {
        return 1;
    }

    if (logtime == 0) {
        logtime = time(NULL);
    }
    if (logtime != 0) {
        struct tm _logtime;
        bzero(&_logtime, sizeof(struct tm));
        _logtime.tm_isdst = -1;
        localtime_r(&logtime, &_logtime);
        ptr += strftime(ptr, AMQP_BUFFER_SIZE - (ptr - buffer),
            "%Y-%m-%d %H:%M:%S", &_logtime);
        *ptr++ = log->delim;
    }
    ptr += snprintf(ptr, AMQP_BUFFER_SIZE - (ptr - buffer),
        "%s%c%s%c%s%c%s%c%s%c", log->system, log->delim, log->hostname,
        log->delim, log->identifier, log->delim, log->subidentifier, log->delim,
        tag, log->delim);
    *ptr = 0;

    va_start(ap, format);
    vbytes = vsnprintf(ptr, AMQP_BUFFER_SIZE - (ptr - buffer), format, ap);
    va_end(ap);
    buffer[AMQP_BUFFER_SIZE - 1] = 0;

    if (vbytes < 0) {
        return 1;
    } else {
        ptr += vbytes;
    }

    return amqpLogger_lograw(log, tag, buffer, ptr - buffer);
}


int amqpLogger_lograw(amqpLogger *log, char const *tag, void *bytes,
    size_t n_bytes) {

    const int key_size = 5*HEADER_BUFFER_SIZE + 6;
	char routing_key[key_size];
    char *ptr = routing_key;
    char *tmp_ptr;

	ptr += snprintf(ptr, key_size, "%s.%s.%s.%s.", log->system, log->hostname,
        log->identifier, log->subidentifier);
    tmp_ptr = ptr + snprintf(ptr, key_size - (ptr - routing_key), "%s", tag);
    *tmp_ptr = 0;
    ptr = strchr(ptr, '.');
    if (ptr != NULL) {
        *ptr = 0;
    }
    return amqpLogger_routingraw_lograw(log, routing_key, bytes, n_bytes);
}

int amqpLogger_routingraw_lograw(amqpLogger *log, char const *routing,
    void *bytes, size_t n_bytes) {

    int cnt = 0;
    if (log == NULL || routing == NULL || bytes == NULL || n_bytes <= 0 || n_bytes >= AMQP_BUFFER_SIZE) {
        return 1;
    }

    if ( (log->amqp.connected & AMQP_CONNECTED_REMOTE) == 0 && log->use_server != AMQP_SERVER_REMOTEONLY) {
        if (_local_send(log, routing, bytes, n_bytes) == 0) {
            return 0;
        }
    }
    for (cnt = 0; cnt < log->retries; cnt++) {
        if ( (log->amqp.connected & AMQP_CONNECTED_REMOTE) == 0) {
            _amqp_open(&(log->amqp));
            amqp_confirm_select(log->amqp.conn, 1);
        }
        if (log->amqp.connected & AMQP_CONNECTED_REMOTE) {
            amqp_bytes_t message;
            amqp_frame_t frame;
            message.bytes = bytes;
	        message.len = n_bytes;

            /* send message */
	        int istatus = amqp_basic_publish(log->amqp.conn, 1,
                amqp_cstring_bytes(AMQPLOGGER_EXCHANGE),
                amqp_cstring_bytes(routing), 0, 0, NULL, message);

            if (istatus != 0) {
                if (log->amqp.output_warnings) {
                    fprintf(stderr, "WARNING: error on message publication\n");
                }
                _amqp_close(&(log->amqp));
                if (!_amqp_open(&(log->amqp))) {
                    if ( (log->amqp.connected & AMQP_CONNECTED_REMOTE) == 0) {
	                    log->amqp.connected ^= AMQP_CONNECTED_REMOTE;
                    }
                    return 1;
                }
                amqp_confirm_select(log->amqp.conn, 1);
            }

            /* check delivery confirmation */
            amqp_simple_wait_frame(log->amqp.conn, &frame);
            if (frame.payload.method.id == AMQP_BASIC_ACK_METHOD) {
                return 0; // success!
            } else {
                uint64_t body_size = 0;
                uint64_t body_read = 0;
                char *message = NULL;
                char *ptr = NULL;
                amqp_simple_wait_frame(log->amqp.conn, &frame);
                body_size = frame.payload.properties.body_size;
                message = (char *) malloc(sizeof(char)*(body_size+1));
                if (message == NULL) {
                    fprintf(stderr, "FAILED to allocate memory!");
                    exit(1);
                }
                ptr = message;
                while (body_read < body_size) {
                    amqp_simple_wait_frame(log->amqp.conn, &frame);
                    body_read += frame.payload.body_fragment.len;
                    memcpy(ptr, frame.payload.body_fragment.bytes, frame.payload.body_fragment.len);
                    ptr += frame.payload.body_fragment.len;
                }
                *ptr = 0;
                fprintf(stderr, "Got delivery failure: %s\n", message);
                free(message);
                amqp_simple_wait_frame(log->amqp.conn, &frame);
                if (frame.payload.method.id != AMQP_BASIC_ACK_METHOD) {
                    if (log->amqp.output_warnings) {
                        fprintf(stderr, "ERROR: was expecting final NACK from delivery failure\n");
                    }
                }
                return 1;
            }

        }
    }
    return 1;
}

/* private interface implmentation */
/**
 * \fn _amqp_open
 * \brief Open a new connection to RabbitMQ (assuming not connected at start)
 * 
 * The process implemented here is for *writing* only.  This open forms the
 * initial connection, authenticates, creates a channel, creats a topic
 * exchange, and, assuming all that worked, marks the log struct as connected.
 *
 * @param log pointer to amqpLogger struct to connect
 * @return 0 if successful, non-zero otherwise
 */
int _amqp_open(amqpState *amqp) {
	int istatus = 0;

    if (amqp == NULL) {
        return -1;
    }
    if ((amqp->connected & AMQP_CONNECTED_REMOTE) != 0) {
        return -1;
    }

	amqp->conn = amqp_new_connection();
	amqp->socket = amqp_tcp_socket_new(amqp->conn);
	istatus = amqp_socket_open(amqp->socket, AMQPLOGGER_SERVER, AMQPLOGGER_PORT);
	if (istatus != 0) {
        if (amqp->output_warnings) {
            fprintf(stderr, "Failed AMQP connection to %s:%d\n", AMQPLOGGER_SERVER, AMQPLOGGER_PORT);
        }
        return -1;
	}
	//amqp_set_socket(amqp->conn, amqp->socket);
	istatus = _amqp_eval_status(amqp, amqp_login(amqp->conn, AMQPLOGGER_VHOST, 0, 
        AMQPLOGGER_FRAMESIZE, 0, AMQP_SASL_METHOD_PLAIN, 
        AMQPLOGGER_USER, AMQPLOGGER_PASSWORD));

	if (istatus != 0) {
        if (amqp->output_warnings) {
            fprintf(stderr, "Failed AMQP login to %s:%d as %s; Error: %s\n",
                AMQPLOGGER_SERVER, AMQPLOGGER_PORT, AMQPLOGGER_USER,
                amqp->amqp_error_message);
        }
        return -1;
	}

	amqp_channel_open(amqp->conn, 1);
	istatus = _amqp_eval_status(amqp, amqp_get_rpc_reply(amqp->conn));
	if (istatus != 0) {
        if (amqp->output_warnings) {
            fprintf(stderr, "Failed AMQP open channel on %s:%d; Error: %s\n",
                AMQPLOGGER_SERVER, AMQPLOGGER_PORT, amqp->amqp_error_message);
        }
        return -1;
	}

	amqp_exchange_declare(amqp->conn, 1, amqp_cstring_bytes(AMQPLOGGER_EXCHANGE),
        amqp_cstring_bytes("topic"), 0, 0, amqp_empty_table);
	istatus = _amqp_eval_status(amqp, amqp_get_rpc_reply(amqp->conn));
	if (istatus != 0) {
        if (amqp->output_warnings) {
            fprintf(stderr, "Failed to declare exchange: %s; Error: %s\n",
                AMQPLOGGER_EXCHANGE, amqp->amqp_error_message);
        }
        return -1;
	}
	amqp->connected ^= AMQP_CONNECTED_REMOTE;
	return 0;
}

/**
 * \fn _amqp_eval_status
 * \brief Interprets errors encountered in rabbitmq-c library
 * @param amqpState pointer to amqpLogger struct with RabbitMQ connection
 * @param status returned amqp_rpc_reply_t struct from rabbitmq-c fxn call
 * @return 0 if no error, non-zero otherwise
 */
int _amqp_eval_status(amqpState* amqp, amqp_rpc_reply_t status) {
	switch (status.reply_type) {
		case AMQP_RESPONSE_NORMAL:
			return 0;
			break;
		case AMQP_RESPONSE_NONE:

            snprintf(amqp->amqp_error_message, HEADER_BUFFER_SIZE,
                "missing RPC reply type (ReplyVal: %u)", status.reply_type);
			break;
		case AMQP_RESPONSE_LIBRARY_EXCEPTION:
            snprintf(amqp->amqp_error_message, HEADER_BUFFER_SIZE,
                "%s (ReplyVal: %u, LibraryErr: %u)",
                amqp_error_string2(status.library_error), status.reply_type,
                status.library_error);
			break;
		case AMQP_RESPONSE_SERVER_EXCEPTION: {
			switch (status.reply.id) {
				case AMQP_CONNECTION_CLOSE_METHOD: {
					amqp_connection_close_t *m =
                        (amqp_connection_close_t *) status.reply.decoded;
                    snprintf(amqp->amqp_error_message, HEADER_BUFFER_SIZE, 
                        "server connection error %d, message: %*s",
                        (int) m->reply_code, (int) m->reply_text.len,
                        (char const *) m->reply_text.bytes
                    );
					break;
				}
				case AMQP_CHANNEL_CLOSE_METHOD: {
					amqp_channel_close_t *m =
                        (amqp_channel_close_t *) status.reply.decoded;
                    //snprintf(amqp->amqp_error_message, HEADER_BUFFER_SIZE, "server channel error %d, message: %*s", (int) m->reply_code, (int) m->reply_text.len, (char const *) m->reply_text);
					break;
				}
				default:
                    snprintf(amqp->amqp_error_message, HEADER_BUFFER_SIZE,
                        "unknown server error, method id %d",
                        (int) status.reply.id
                    );
					break;
			}
			break;
		}
	}
	return 1;
}

/**
 * \fn _amqp_close
 * \brief Closes an open connection to RabbitMQ
 * @param amqp pointer to amqpState struct with RabbitMQ connection
 * @return nothing
 */
void _amqp_close(amqpState *amqp) {
    int istatus = 0;
	if (amqp->connected & AMQP_CONNECTED_REMOTE) {
        /* process for disconnecting is to:
             1) close channel
             2) close the socket
             3) destroy the conn struct
             4) mark the connection as closed
        */
		istatus = _amqp_eval_status(amqp,
            amqp_channel_close(amqp->conn, 1, AMQP_REPLY_SUCCESS)
        );
		if (istatus != 0) {
            if (amqp->output_warnings) {
                fprintf(stderr, "AMQP channel close failed: %s\n",
                    amqp->amqp_error_message);
            }
			return;
		}

		istatus = _amqp_eval_status(amqp,
            amqp_connection_close(amqp->conn, AMQP_REPLY_SUCCESS)
        );
		if (istatus != 0) {
            if (amqp->output_warnings) {
                fprintf(stderr, "AMQP connection close failed: ",
                    amqp->amqp_error_message);
            }
            return;
		}
		amqp_destroy_connection(amqp->conn);

        /* remove the remote connection bits */
        amqp->connected ^= AMQP_CONNECTED_REMOTE;
	}
}

int _local_send(amqpLogger *log, char const *routing, char const *bytes, int n_bytes) {
    int s_remote;
    struct sockaddr_un remote;
    char buffer[1024];
    char const *ptr = bytes;
    ssize_t sent_bytes = 0;
    memset(&remote, 0, sizeof(struct sockaddr_un));

    if ((s_remote = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        return 1;
    }
    remote.sun_family = AF_UNIX;
    strncpy(remote.sun_path, SOCKET_PATH, sizeof(remote.sun_path) - 1);
    if (connect(s_remote, (struct sockaddr *) &remote, sizeof(struct sockaddr_un)) == -1) {
        return 1;
    }
    log->amqp.connected ^= AMQP_CONNECTED_LOCAL;

    send(s_remote, "AMQPLOGGER", 11, 0);
    recv(s_remote, buffer, 1024, 0);
    if (strcmp(buffer, "OK") != 0) {
        close(s_remote);
        log->amqp.connected ^= AMQP_CONNECTED_LOCAL;
        return 1;
    }
    send(s_remote, routing, strlen(routing) + 1, 0);
    recv(s_remote, buffer, 1024, 0);
    if (strcmp(buffer, "OK") != 0) {
        close(s_remote);
        log->amqp.connected ^= AMQP_CONNECTED_LOCAL;
        return 1;
    }
    snprintf(buffer, 1024, "%d", n_bytes);
    send(s_remote, buffer, strlen(buffer) + 1, 0);
    buffer[0] = 0;
    recv(s_remote, buffer, 1024, 0);
    if (strcmp(buffer, "OK") != 0) {
        close(s_remote);
        log->amqp.connected ^= AMQP_CONNECTED_LOCAL;
        return 1;
    }
    ptr = bytes;
    while (ptr - bytes < n_bytes) {
        sent_bytes = send(s_remote, ptr, n_bytes - (ptr - bytes), 0);
        ptr += sent_bytes;
        if (sent_bytes == 0) {
            break;
        }
    }
    recv(s_remote, buffer, 1024, 0);

    close(s_remote);
    log->amqp.connected ^= AMQP_CONNECTED_LOCAL;

    return (strcmp(buffer, "OK") != 0 || ptr - bytes < n_bytes);
}

int amqpReceiver_initialize(amqpReceiver *recv) {
    if (recv == NULL) {
        return -1;
    }
    recv->amqp.connected = AMQP_CONNECTED_NONE;
    recv->amqp.output_warnings = 0;
    recv->consuming = 0;

    /* zero out the amqp portions of the structure */
    memset((void *) &(recv->queue), 0, sizeof(amqp_bytes_t));
    memset((void *) &(recv->amqp.conn), 0, sizeof(amqp_connection_state_t));
    recv->amqp.socket = NULL;
    memset((void *) &(recv->amqp.status), 0, sizeof(amqp_rpc_reply_t));

    /* open the connection */
    _amqp_open(&(recv->amqp));
    if ( (recv->amqp.connected & AMQP_CONNECTED_REMOTE) == 0) {
        return 1;
    }
    amqp_basic_qos(recv->amqp.conn, 1, 0, 1, 1);
    int istatus = _amqp_eval_status(&(recv->amqp), amqp_get_rpc_reply(recv->amqp.conn));
    if (istatus != 0) {
        if (recv->amqp.output_warnings) {
            fprintf(stderr, "FAILED to set qos prefetch: %s\n", recv->amqp.amqp_error_message);
        }
        return 1;
    }

    /* declare the queue */
    amqp_bytes_t queue_name_bytes = amqp_empty_bytes;
    amqp_queue_declare_ok_t* queue_reply = amqp_queue_declare(recv->amqp.conn, 1, queue_name_bytes, 0, 0, 0, 1, amqp_empty_table);
    istatus = _amqp_eval_status(&(recv->amqp), amqp_get_rpc_reply(recv->amqp.conn));
    if (istatus != 0) {
        if (recv->amqp.output_warnings) {
            fprintf(stderr, "FAILED to declare queue: %s\n", recv->amqp.amqp_error_message);
        }
        return 1;
    }
    recv->queue = amqp_bytes_malloc_dup(queue_reply->queue);
    if (recv->queue.bytes == NULL) {
        _amqp_close(&(recv->amqp));
        return -1;
    }

    return 0;
}

void amqpReceiver_destruct(amqpReceiver *recv) {
    if (recv->queue.bytes != NULL) {
        amqp_bytes_free(recv->queue);
        memset((void *) &(recv->queue), 0, sizeof(amqp_bytes_t));
    }
    recv->consuming = 0;
    if (recv->amqp.connected & AMQP_CONNECTED_REMOTE) {
        _amqp_close(&(recv->amqp));
    }
}

int amqpReceiver_add_routing_key(amqpReceiver* recv, const char *system, const char *hostname, const char *identifier, const char *subidentifier, const char *tag) {
    const char *routing_keys[] = {system, hostname, identifier, subidentifier, tag};
    char routing_key[5*HEADER_BUFFER_SIZE + 6];
    char *ptr = routing_key;
    int idx = 0;
    int len = 0;

    if (recv == NULL || recv->consuming != 0) {
        return -1;
    }
    for (idx = 0; idx < 5; idx++) {
        char *lPtr = NULL;

        if (idx != 0) {
            *ptr++ = '.';
        }
        if (routing_keys[idx] != NULL) {
            memcpy(ptr, routing_keys[idx], HEADER_BUFFER_SIZE*sizeof(char));
            ptr[HEADER_BUFFER_SIZE-1] = 0;

            /* if there are any '.' in the key, then terminate the string */
            lPtr = strchr(ptr, '.');
            if (lPtr != NULL) {
                *lPtr = 0;
            }
        } else {
            *ptr = '*';
            *(ptr+1) = 0;
        }

        /* get length of the routing key */
        len = strlen(ptr);
        len = len < HEADER_BUFFER_SIZE-1 ? len : HEADER_BUFFER_SIZE-1;
        ptr += len;
        *ptr = 0;
    }
    amqp_queue_bind(recv->amqp.conn, 1, recv->queue, amqp_cstring_bytes(AMQPLOGGER_EXCHANGE), amqp_cstring_bytes(routing_key), amqp_empty_table);
    int istatus = _amqp_eval_status(&(recv->amqp), amqp_get_rpc_reply(recv->amqp.conn));
    if (istatus != 0) {
        if (recv->amqp.output_warnings) {
            fprintf(stderr, "FAILED to bind '%s' to queue: %s\n", routing_key, recv->amqp.amqp_error_message);
        }
        return -1;
    }
    return 0;
}

size_t amqpReceiver_get_message(amqpReceiver* recv, char *system, char *hostname, char *identifier, char *subidentifier, char *tag, char **message_buffer, size_t *buffer_size) {
    char *ptr, *sPtr, *ePtr;
    int idx = 0;
    ptr = sPtr = ePtr = NULL;
    char *routing_keys[] = {system, hostname, identifier, subidentifier, tag};

    if (recv == NULL) {
        return 0;
    }
    if (recv->consuming == 0) {
        amqp_basic_consume(recv->amqp.conn, 1, recv->queue, amqp_empty_bytes, 0, 1, 0, amqp_empty_table);
        int istatus = _amqp_eval_status(&(recv->amqp), amqp_get_rpc_reply(recv->amqp.conn));
        if (istatus != 0) {
            if (recv->amqp.output_warnings) {
                fprintf(stderr, "FAILED to consume queue: %s\n", recv->amqp.amqp_error_message);
            }
            return -1;
        }
        recv->consuming = 1;
    }

    for ( ; ; ) {
        amqp_frame_t frame;
        int result;
        size_t body_received;
        size_t body_target;
        amqp_maybe_release_buffers(recv->amqp.conn);
        result = amqp_simple_wait_frame(recv->amqp.conn, &frame);
        if (result < 0) { break; }

        if (frame.frame_type != AMQP_FRAME_METHOD) {
            continue;
        }
        if (frame.payload.method.id != AMQP_BASIC_DELIVER_METHOD) {
            continue;
        }
        amqp_basic_deliver_t* d = (amqp_basic_deliver_t *) frame.payload.method.decoded;

        /* decode the routing key and map to fields */
        sPtr = (char*)(d->routing_key.bytes);
        ePtr = (char*)(d->routing_key.bytes) + (int) d->routing_key.len;
        for (ptr = sPtr, idx = 0; ptr <= ePtr && idx < 5; ptr++ ) {
            if (*ptr == '.' || ptr == ePtr) {
                int len = ptr - sPtr;
                len = len < HEADER_BUFFER_SIZE-1 ? len : HEADER_BUFFER_SIZE-1;
                memcpy(routing_keys[idx], sPtr, len);
                routing_keys[idx][len] = 0;
                sPtr = ptr + 1;
                idx++;
            }
        }
        result = amqp_simple_wait_frame(recv->amqp.conn, &frame);
        if (result < 0) {
            continue;
        }

        if (frame.frame_type != AMQP_FRAME_HEADER) {
            continue;
        }
        //amqp_basic_properties_t* p = (amqp_basic_properties_t *) frame.payload.properties.decoded;

        body_target = frame.payload.properties.body_size;
        if (*buffer_size < body_target+1 || *message_buffer == NULL) {
            *message_buffer = (char *) realloc(*message_buffer, sizeof(char)*(body_target+1));
            if (*message_buffer == NULL) {
                fprintf(stderr, "FAILED to allocate %lu bytes for message buffer!\n", sizeof(char)*(body_target+1));
                return 0;
            }
            *buffer_size = body_target+1;
        }

        char* ptr = *message_buffer;
        body_received = 0;

        while (body_received < body_target) {
            result = amqp_simple_wait_frame(recv->amqp.conn, &frame);
            if (result < 0) {
                break;
            }
            if (frame.frame_type != AMQP_FRAME_BODY) {
                break;
            }
            body_received += frame.payload.body_fragment.len;
            memcpy(ptr, frame.payload.body_fragment.bytes, frame.payload.body_fragment.len);
            ptr += frame.payload.body_fragment.len;
            *ptr = 0;
        }

        if (body_received == body_target) {
            // got full message successfully!
            return body_received;
        }
        break;
    }
    return 0;
}
