/*******************************************************************************
procmon, Copyright (c) 2014, The Regents of the University of California,
through Lawrence Berkeley National Laboratory (subject to receipt of any
required approvals from the U.S. Dept. of Energy).  All rights reserved.

If you have questions about your rights to use or distribute this software,
please contact Berkeley Lab's Technology Transfer Department at  TTD@lbl.gov.

The LICENSE file in the root directory of the source code archive describes the
licensing and distribution rights and restrictions on this software.

Author:   Douglas Jacobsen <dmj@nersc.gov>
*******************************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
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

#include <boost/thread.hpp>
#include <boost/thread/shared_mutex.hpp>

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
    size_t roThread_update_time = 10;

    ProcAMQPIO *conn = NULL;

    Cache<procdata> *pd_cache = new Cache<procdata>(300);
    Cache<procstat> *ps_cache = new Cache<procstat>(300);
    Cache<procfd>   *fd_cache = new Cache<procfd>(300);

    Cache<procdata> *pd_cache_ro = NULL;
    Cache<procstat> *ps_cache_ro = NULL;
    Cache<procfd>   *fd_cache_ro = NULL;

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
    time_t last_update = 0;

    while (cleanup == 0) {
        ProcRecordType recordType = conn->read_stream_record(&data, &data_size, &nRecords, 100000);
        if (recordType == TYPE_INVALID) {
            continue;
        }
        conn->get_frame_context(hostname, identifier, subidentifier);

        if (data == NULL) {
            continue;
        }
        last_update = time(NULL);

        JobIdent ident(hostname, identifier, subidentifier);

        if (recordType == TYPE_PROCDATA) {
            procdata *ptr = (procdata *) data;
            pd_cache->set(ident, ptr, nRecords);
        } else if (recordType == TYPE_PROCSTAT) {
            procstat *ptr = (procstat *) data;
            ps_cache->set(ident, ptr, nRecords);
        } else if (recordType == TYPE_PROCFD) {
            procfd *ptr = (procfd *) data;
            fd_cache->set(ident,ptr, nRecords);
        }

        /*
        if (time(NULL) - last_update > roThread_update_time) {
            roData_mtx.lock();
            // copy data
            roData_mtx.unlock();
            last_update = time(NULL);
        }
        */
    }
}
