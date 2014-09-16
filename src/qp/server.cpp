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

using namespace std;

int cleanup = 0;
time_t last_update = 0;

pthread_mutex_t data_lock = PTHREAD_MUTEX_INITIALIZER;

struct JobIdent {
    string hostname;
    string jobid;
    string taskid;

    JobIdent(const string &_hostname, const string &_jobid, const string &_taskid):
        hostname(_hostname), jobid(_jobid), taskid(_taskid) {
    }

    bool operator==(const JobIdent &other) const {
        return hostname == other.hostname && jobid == other.jobid && taskid == other.taskid;
    }

    friend ostream& operator<< (ostream& stream, const JobIdent& ident);
};

ostream& operator<< (ostream& stream, const JobIdent& ident) {
    stream << "[" << ident.hostname << "," << ident.jobid << "," << ident.taskid << "]";
}

namespace std {
    template <>
    struct hash<JobIdent> {
        std::size_t operator()(const JobIdent &k) const {
            using std::size_t;
            using std::hash;
            using std::string;
            return ((hash<string>()(k.hostname) ^ (hash<string>()(k.jobid) << 1)) >> 1) ^ (hash<string>()(k.taskid) << 1);
        }
    };
}

template <typename T> struct JobDelta {
    double dt;

    void set(T *curr_data, T *prev_data, struct JobDelta<T> *old) {
        dt = (curr_data->recTime + curr_data->recTimeUSec*1e-6) - (prev_data->recTime + prev_data->recTimeUSec*1e-6);
    }
};

template <> struct JobDelta<procstat> {
    double dt;
    double stateSince;
    unsigned long delta_stime;
    unsigned long delta_utime;
    unsigned long delta_ioread;
    unsigned long delta_iowrite;

    void set(procstat *curr_data, procstat *prev_data, struct JobDelta<procstat> *old) {
        stateSince = 0;

        dt = (curr_data->recTime + curr_data->recTimeUSec*1e-6) - (prev_data->recTime + prev_data->recTimeUSec*1e-6);
        if (prev_data->state == curr_data->state) {
            if (old != NULL && old->stateSince > 0) {
                stateSince = old->stateSince;
            } else {
                stateSince = prev_data->recTime + prev_data->recTimeUSec*1e-6;
            }
        } else {
            stateSince = curr_data->recTime + curr_data->recTimeUSec*1e-6;
        }
        delta_stime = curr_data->stime - prev_data->stime;
        delta_utime = curr_data->utime - prev_data->utime;
        delta_ioread = curr_data->io_rchar - prev_data->io_rchar;
        delta_iowrite = curr_data->io_wchar - prev_data->io_wchar;
    }
};


template <typename T> struct CacheData {
    T *data;
    int buffer_len;
    int count;

    CacheData()  {
        data = NULL;
        buffer_len = 0;
        count = 0;
    }
    ~CacheData() {
        if (data != NULL) {
            delete[] data;
            data = NULL;
            buffer_len = 0;
            count = 0;
        }
    }

    void set(T *new_data, int n) {
        if (n > buffer_len) {
            int tgt = n < 32 ? 32 : n;
            int old_n = buffer_len;
            if (data != NULL) {
                delete[] data;
            }
            data = new T[tgt];
            buffer_len = tgt;
            cerr << "allocating space for " << n << " objects (was " << old_n << ")" << endl;
        }
        if (data == NULL) {
            /* MEMORY ERROR */
        }
        for (int i = 0; i < n; i++) {
            data[i] = new_data[i];
        }
        count = n;
    }
};

template<typename T> class Cache;

template <typename T> class CacheNode {
    friend class Cache<T>;

    JobIdent *ident;
    struct CacheNode<T> *next;
    struct CacheNode<T> *prev;
    struct CacheData<T> *curr_data;
    struct JobDelta<T> *delta;
    int delta_buflen;
    time_t set_time;

    public:
    CacheNode() {
        ident = NULL;
        next = NULL;
        prev = NULL;
        curr_data = new CacheData<T>();
        set_time = 0;
        delta = NULL;
        delta_buflen = 0;
    }

    ~CacheNode() {
        if (ident != NULL) {
            delete ident;
            ident = NULL;
        }
        if (curr_data != NULL) {
            delete curr_data;
            curr_data = NULL;
        }
        if (delta != NULL) {
            delete[] delta;
            delta = NULL;
            delta_buflen = 0;
        }
    }

    inline void set(JobIdent *_ident, T* data, int nRecords) {
        if (ident != NULL) {
            delete ident;
            curr_data->count = 0;
        }
        ident = new JobIdent(*_ident);

        XXXX TODO - NEED TO FINISH REMOVING PREV_DATA XXXX

        struct CacheData<T> *ptr = prev_data;
        prev_data = curr_data;
        curr_data = ptr;
        curr_data->set(data, nRecords);

        /* map curr procs to old procs */
        int local_map[nRecords];
        memset(local_map, -1, sizeof(int)*nRecords);
        for (int i = 0; i < curr_data->count; i++) {
            for (int j = 0; j < prev_data->count; j++) {
                if (curr_data->data[i].pid == prev_data->data[j].pid && curr_data->data[i].startTime == prev_data->data[j].startTime) {
                    local_map[i] = j;
                }
            }
        }

        /* calcuate deltas between the old obs and the new; the old map still
           contains the indices of the old observation locations */
        JobDelta<T> tmp_deltas[nRecords];
        for (int i = 0; i< curr_data->count; i++) {
            if (local_map[i] == -1) continue;
            T *curr = &(curr_data->data[i]);
            T *prev = &(prev_data->data[local_map[i]]);
            JobDelta<T> *pdelta = (&delta)[local_map[i]];
            tmp_deltas[i].set(curr, prev, pdelta);
        }

        if (map_buflen < curr_data->count) {
            if (map != NULL) {
                delete[] map;
            }
            map = new int[curr_data->count];
            map_buflen = curr_data->count;
        }
        memcpy(map, local_map, sizeof(int) * nRecords);

        if (delta_buflen < curr_data->count) {
            if (delta != NULL) {
                delete[] delta;
            }
            delta = new JobDelta<T>[curr_data->count];
            delta_buflen = curr_data->count;
        }
        memcpy(delta, tmp_deltas, sizeof(JobDelta<T>) * curr_data->count);
    }

    const JobIdent *getJobIdent() {
        return ident;
    }
};

template<typename T> class Cache {
    /* the cache will maintain a circular set of buffers for storing recently 
       acquired data.  the goal of the cache is to buffer all the data 
       structures transmitted so that at the time of a query only the most 
       recent information is available.  since there is a high turn over rate 
       the newest record will always be put at head with the previously most-
       recent record pushed to the next postion.  Thus the oldest records are
       always to the left. A minimum cache-age time will be enforced by growing
       the size of the circular cache if the oldest record is younger than the
       timeout.  */

    int timeout;
    struct CacheNode<T> *head;
    int minsize;
    int currsize;
    unordered_map<JobIdent, CacheNode<T> * >  index;

    public:
    Cache(int _timeout, int _minsize=1000) {
        timeout = _timeout;
        minsize = _minsize;

        CacheNode<T> *ptr = new CacheNode<T>();
        head = ptr;
        for (int i = 1; i < minsize; i++) {
            ptr->next = new CacheNode<T>();
            ptr->next->prev = ptr;
            ptr = ptr->next;
        }
        head->prev = ptr;
        ptr->next = head;
        currsize = minsize;
    }

    void set(JobIdent &ident, T *records, int nRecords) {
        CacheNode<T> *tgt = NULL;
        CacheNode<T> *ptr = NULL;
        time_t curr_time = time(NULL);
        auto it = index.find(ident);
        if (it != index.end()) {
            tgt = it->second;
            if (!(*(tgt->getJobIdent()) == ident)) {
                index.erase(it);
                tgt = NULL;
                /*
            } else {
                cerr << "found existing record for " << ident << ", " << *(tgt->data.ident) << endl;
                */
            }
        }
        if (tgt == NULL) {
            tgt = head->prev;
            if (curr_time - tgt->set_time < timeout) {
                CacheNode<T> *ptr = new CacheNode<T>();
                head->prev->next = ptr;
                ptr->prev = head->prev;
                head->prev = ptr;
                ptr->next = head;
                tgt = ptr;
                currsize++;
                cerr << "increased size to: " << currsize << endl;
            } else if (tgt->getJobIdent() != NULL) {
                cerr << "reusing record from " << *(tgt->getJobIdent()) << " for " << ident << endl;
            }
        }

        tgt->set(&ident, records, nRecords);

        /* remove tgt from it's curent place */
        tgt->prev->next = tgt->next;
        tgt->next->prev = tgt->prev;

        /* insert tgt before head */
        head->prev->next = tgt;
        tgt->next = head;
        tgt->prev = head->prev;
        head->prev = tgt;

        /* move head to tgt */
        head = tgt;
        index[ident] = head;

        head->set_time = curr_time;
    }

    void trim() {
        CacheNode<T> *ptr = head->prev;
        CacheNode<T> *tmp = NULL;
        time_t curr_time = time(NULL);
        while (curr_time - ptr->set_time > timeout && currsize > minsize) {
            ptr->prev->next = ptr->next;
            ptr->next-prev = ptr->prev;
            tmp = ptr;
            ptr = ptr->prev;
            delete tmp;
            currsize--;
        }
    }
};

struct ThreadData {
    Cache<procdata> *pd;
    Cache<procstat> *ps;
    Cache<procfd>   *fd;
    int sockfd;

    ThreadData(Cache<procdata> *_pd, Cache<procstat> *_ps, Cache<procfd> *_fd, int _sockfd):
        pd(_pd), ps(_ps), fd(_fd), sockfd(_sockfd);
    {}
    ThreadData(ThreadData *other) {
        ps = other->ps;
        pd = other->pd;
        fd = other->fd;
        sockfd = other->sockfd;
    }
};

void *worker_thread(void *args) {
    ThreadData *myData = (ThreadData *) args;
    char buffer[];
}


void *server_thread(void *args) {
    ThreadData *localData = (ThreadData *) args;

    /* setup network listener */
    int sockfd, newsockfd, clilen;
    struct sockaddr_in cli_addr, serv_addr;
    pthread_t worker_thread;

    if ((sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        /* error */
    }
    memset(&serv_addr, 0, sizeof(struct sockaddr_in));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    serv_addr.sin_port = htons(31213);
    if (bind(sockfd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
        /* error */
    }
    listen(sockfd, 50);

    while (cleanup == 0) {
        clilen = sizeof(cli_addr);
        newsockfd = accept(sockfd, (struct sockaddr *) &cli_addr, &clilen);
        if (newsockfd < 0) {
            /* error */
            continue;
        }
        ThreadData *workerData = new ThreadData(localData);
        workerData->sockfd = newsockfd;
        pthread_create(&worker_thread, NULL, worker_start, workerData);
    }
    return NULL;
}

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

    caches localCaches(&pd_cache, &ps_cache, &fd_cache);

    pthread_t server_thread;
    int retCode = pthread_create(&server_thread, NULL, server_start, &localCaches);

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

        pthread_mutex_lock(&data_lock);
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
        pthread_mutex_unlock(&data_lock);
    }
}
