%module amqpLogger
%{
/* Includes the header in the wrapper code */
#include "amqpLogger.h"
struct logger {
    amqpLogger *logptr;
    char delim;
};
%}

struct logger {
    amqpLogger *logptr;
    char delim;
};
%extend logger {
    static struct logger *initialize(const char *server, const char *hostname, const char *id, const char *subid) {
        struct logger *log = malloc(sizeof(struct logger));
        if (log == NULL) goto error;
        log->delim = ',';
        log->logptr = malloc(sizeof(amqpLogger));
        if (log->logptr == NULL) goto error;
        int ret = amqpLogger_initialize(log->logptr, AMQP_SERVER_AUTO, server, hostname, id, subid, ',');
        if (ret != 0) goto error;
        return log;
error:
        if (log != NULL) {
            if (log->logptr != NULL) free(log->logptr);
            free(log);
        }
        return NULL;
    }
    logger() {
        return logger_initialize(NULL, NULL, NULL, NULL);
    }
    logger(const char* system, const char* hostname, const char* id, const char* subid) {
        return logger_initialize(system, hostname, id, subid);
    }
    logger(const char* id, const char* subid) {
        return logger_initialize(NULL, NULL, id, subid);
    }
    ~logger() {
        if ($self != NULL) {
            if ($self->logptr != NULL) {
                amqpLogger_destruct($self->logptr);
                free($self->logptr);
            }
            free($self);
        }
    }
    void set_identifiers(const char *id, const char *subid) {
        int ret = amqpLogger_setidentifiers($self->logptr, id, subid);
    }
    int log(const char *tag, const char *message) {
        $self->logptr->delim = $self->delim;
        return amqpLogger_log($self->logptr, 0, tag, message);
    }
};
