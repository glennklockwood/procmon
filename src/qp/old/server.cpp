#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <signal.h>
#include <errno.h>
#include <math.h>
#include <sys/types.h>
#include <pwd.h>

#include <vector>
#include <deque>
#include <unordered_map>
#include <string>
#include <iostream>
#include <algorithm>

#include "ProcData.hh"
#include "ProcIO.hh"
#include "ProcCache.hh"

using namespace std;

int cleanup = 0;
time_t last_update = 0;

int main(int argc, char **argv) {
    const char *mqServer = DEFAULT_AMQP_HOST;
    const char *mqVHost = DEFAULT_AMQP_VHOST;
    int mqPort = DEFAULT_AMQP_PORT;
    const char *mqUser = DEFAULT_AMQP_USER;
    const char *mqPassword = DEFAULT_AMQP_PASSWORD;
    const char *mqExchangeName = DEFAULT_AMQP_EXCHANGE_NAME;
    int mqFrameSize = DEFAULT_AMQP_FRAMESIZE;

    ProcAMQPIO *conn = NULL;

    Cache<procdata> pd_cache(300);
    Cache<procstat> ps_cache(300);
    Cache<procfd>   fd_cache(300);

    /*
    signal(SIGTERM, sig_handler);
    pthread_t screen_thread, monitor_thread;
    int retCode = pthread_create(&screen_thread, NULL, screen_start, screen);
    if (retCode != 0) {
        fatal_error("failed to start screen/interface thread", retCode);
    }
    retCode = pthread_create(&monitor_thread, NULL, monitor_start, &age_timeout);
    if (retCode != 0) {
        fatal_error("failed to start monitor thread", retCode);
    }
    */

    conn = new ProcAMQPIO(mqServer, mqPort, mqVHost, mqUser, mqPassword, mqExchangeName, mqFrameSize, FILE_MODE_READ);
    conn->set_context("*", "*", "*");

    void *data = NULL;
    size_t data_size = 0;
    int nRecords = 0;
    string hostname;
    string identifier;
    string subidentifier;

    /* TODO: add timer thread to look for idle-ness and kill the whole thing */

    while (cleanup == 0) {
        ProcRecordType recordType = conn->read_stream_record(&data, &data_size, &nRecords);
        conn->get_frame_context(hostname, identifier, subidentifier);

        if (data == NULL) {
            continue;
        }
        last_update = time(NULL);

        JobIdent ident(hostname, identifier, subidentifier);

        if (recordType == TYPE_PROCDATA) {
            procdata *ptr = (procdata *) data;
            pd_cache.set(ident, ptr, nRecords);
        } else if (recordType == TYPE_PROCSTAT) {
            procstat *ptr = (procstat *) data;
            ps_cache.set(ident, ptr, nRecords);
        } else if (recordType == TYPE_PROCFD) {
            procfd *ptr = (procfd *) data;
            fd_cache.set(ident,ptr, nRecords);
        }
    }
}
