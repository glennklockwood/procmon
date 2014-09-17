#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <signal.h>
#include <errno.h>
#include <ncurses.h>
#include <math.h>
#include <sys/types.h>
#include <pwd.h>

#include <vector>
#include <unordered_map>
#include <string>
#include <iostream>
#include <algorithm>

#include "ProcData.hh"
#include "ProcIO.hh"

using namespace std;

class TopCurses;

pthread_mutex_t data_lock = PTHREAD_MUTEX_INITIALIZER;
time_t last_update = 0;
int cleanup = 0;
int age_timeout = 60;
unordered_map<int,string> usernames;
void fatal_error(const char *error, int err);
static void screen_exit(TopCurses *screen, int exit_code);

int cmp_procstat_pid(const void *t_a, const void *t_b) {
    const procstat *a = (const procstat *) t_a;
    const procstat *b = (const procstat *) t_b;
    return a->pid - b->pid;
}

int cmp_procdata_pid(const void *t_a, const void *t_b) {
    const procdata *a = (const procdata *) t_a;
    const procdata *b = (const procdata *) t_b;
    return a->pid - b->pid;
}

class JobDataDelta;

class JobData {
  friend class JobDataDelta;
  public:
    JobData() {
        ps = NULL;
        pd = NULL;
        n_ps = 0;
        n_pd = 0;
    }
    ~JobData() {
        if (ps != NULL) delete ps;
        if (pd != NULL) delete pd;
        ps = NULL;
        pd = NULL;
        n_ps = 0;
        n_pd = 0;
    }
    void setProcStat(procstat *data, int n) {
        ps = new procstat[n];
        if (ps == NULL) {
            fatal_error("Couldn't allocate memory, failing.", 0);
        }
        memcpy(ps, data, sizeof(procstat) * n);
        n_ps = n;
        qsort(ps, n_ps, sizeof(procstat), cmp_procstat_pid);
    }
    void setProcData(procdata *data, int n) {
        pd = new procdata[n];
        if (pd == NULL) {
            fatal_error("Couldn't allocate memory, failing.", 0);
        }
        memcpy(pd, data, sizeof(procdata) * n);
        n_pd = n;
        qsort(pd, n_pd, sizeof(procdata), cmp_procdata_pid);
    }
      
    procstat *ps;
    procdata *pd;
    int n_ps;
    int n_pd;
};

class JobDataDelta {
  public:
    JobDataDelta(JobData *current, JobData *old) {
        n_items = current->n_ps;
        d_stime = new double[n_items];
        d_utime = new double[n_items];
        d_ioread = new double[n_items];
        d_iowrite = new double[n_items];
        for (int i = 0; i < n_items; i++) {
            procstat *ps = &(current->ps[i]);
            bool found = false;
            for (int j = 0; j < old->n_ps || old->ps[j].pid > ps->pid; j++) {
                if (ps->pid == old->ps[j].pid) {
                    d_stime[i] = ps->stime - old->ps[j].stime;
                    d_utime[i] = ps->utime - old->ps[j].utime;
                    d_ioread[i] = ps->io_rchar - old->ps[j].io_rchar;
                    d_iowrite[i] = ps->io_wchar - old->ps[j].io_wchar;
                    found = true;
                    dt = (ps->recTime + ps->recTimeUSec*1e-6) - (old->ps[j].recTime + old->ps[j].recTimeUSec*1e-6);
                }
            }
            if (!found) {
                dt = NAN;
            }
        }
    }
    ~JobDataDelta() {
        delete d_stime;
        delete d_utime;
        delete d_ioread;
        delete d_iowrite;
    }
    int n_items;
    double *d_stime;
    double *d_utime;
    double *d_ioread;
    double *d_iowrite;
    double dt;
};

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
};

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

struct PrintData {
    string hostname;
    string command;
    string username;
    int pid;
    int ppid;
    time_t recTime;
    double total_cpu;
    double rate_cpu;
    double wallTime;
    double rss;
    double vmem;
    int threads;
    char state;
    int mpiRank;
    double total_ioRead;
    double total_ioWrite;
    double rate_ioRead;
    double rate_ioWrite;
};

struct CurrentJobData {
    JobData *data;
    JobDataDelta *delta;
    vector<PrintData *> records;

    CurrentJobData(JobData *data, JobDataDelta *delta) {
        this->data = data;
        this->delta = delta;
    }

    ~CurrentJobData() {
        if (data != NULL) delete data;
        if (delta != NULL) delete delta;
        for (auto it = records.begin(); it != records.end(); it++) {
            delete *it;
        }
        data = NULL;
        delta = NULL;
    }
};

struct cmpJobIdent {
    bool operator()(const JobIdent& a, const JobIdent& b) const {
        return a.hostname == b.hostname && a.jobid == b.jobid && a.taskid == b.taskid;
    }
};

enum DisplayType {
    STRING,
    TIME,
    MEMORY,
    INTEGER,
    DOUBLE,
};

class DisplayData {
    friend class TopCurses;
    public:
    DisplayData(string _label, int _width, bool _widthFixed,
        double _widthCoefficient):
        label(_label), baseWidth(_width),
        widthFixed(_widthFixed), widthCoefficient(_widthCoefficient)
    {
        width = baseWidth;
    }
    virtual void display(int y, int x, PrintData *data) = 0;
    void displayLabel(int y, int x) {
        bool boldNext = false;
        for (int i = 0; i < label.size(); i++) {
            if (label[i] == '&') {
                boldNext = true;
                continue;
            }
            if (boldNext) {
                attr_on(A_BOLD, NULL);
            }
            mvaddch(y,x,label[i]);
            if (boldNext) {
                attr_off(A_BOLD, NULL);
                boldNext = false;
            }
            x++;
        }
    }

    protected:
    string label;
    int baseWidth;
    int width;
    bool widthFixed;
    double widthCoefficient;

    static char *format_time(double value, char *buffer, int buflen, int maxwidth) {
        int hours = 0;
        int minutes = 0;
        int seconds = 0;
        int millis = 0;
        int s = floor(value);
        millis = (value - s) * 1000;
        seconds = s % 60;
        s /= 60;
        minutes = s % 60;
        s /= 60;
        hours = s;
        int count = snprintf(buffer, buflen, "%02d:%02d:%02d.%04d", hours, minutes, seconds, millis);
        if (count > maxwidth) {
            memset( buffer, '*', min(buflen, maxwidth));
            buffer[min(buflen,maxwidth)] = 0;
        }
        return buffer;
    }

    static char *format_memory(double value, char *buffer, int buflen, int maxwidth) {
        const char *suffix = "BKMGTPE";
        int count = 0;
        while (value > 1024) {
            value /= 1024;
            count++;
        }
        count = snprintf(buffer, buflen, "%0.1f%c", value, suffix[count]);
        if (count > maxwidth) {
            memset(buffer, '*', min(buflen, maxwidth));
            buffer[min(buflen,maxwidth)] = 0;
        }
        return buffer;
    }
};

class DisplayAge: public DisplayData {
    public:
    DisplayAge(): DisplayData("&AGE", 3, true, 0.0) {
    }
    virtual void display(int y, int x, PrintData *data) {
        mvprintw(y, x, "%*d", width, (time(NULL) - data->recTime));
    }
};
class DisplayMpiRank: public DisplayData {
    public:
    DisplayMpiRank(): DisplayData("&MPI", 4, true, 0.0) {
    }
    virtual void display(int y, int x, PrintData *data) {
        mvprintw(y, x, "%*d", width, data->mpiRank);
    }
};
class DisplayUser: public DisplayData {
    public:
    DisplayUser() : DisplayData("&USER", 8, true, 0.0) {
    }
    virtual void display(int y, int x, PrintData *data) {
        mvprintw(y, x, "%-.*s", width, data->username.c_str());
    }
};
class DisplayHost: public DisplayData {
    public:
    DisplayHost() : DisplayData("&HOST", 10, true, 0.0) {
    }
    virtual void display(int y, int x, PrintData *data) {
        mvprintw(y, x, "%-.*s", width, data->hostname.c_str());
    }
};
class DisplayPid: public DisplayData {
    public:
    DisplayPid() : DisplayData("&PID", 6, true, 0.0) {
    }
    virtual void display(int y, int x, PrintData *data) {
        mvprintw(y, x, "%*d", width, data->pid);
    }
};
class DisplayPPid: public DisplayData {
    public:
    DisplayPPid() : DisplayData("PP&ID", 6, true, 0.0) {
    }
    virtual void display(int y, int x, PrintData *data) {
        mvprintw(y, x, "%*d", width, data->ppid);
    }
};
class DisplayCommand: public DisplayData {
    public:
    DisplayCommand() : DisplayData("&COMMAND", 6, false, 1.0) {
    }
    virtual void display(int y, int x, PrintData *data) {
        mvprintw(y, x, "%-.*s", width, data->command.c_str());
    }
};
class DisplayCpuRate: public DisplayData {
    public:
    DisplayCpuRate() : DisplayData("CPU&%", 5, true, 0.0) {
    }
    virtual void display(int y, int x, PrintData *data) {
        mvprintw(y, x, "%0.1f", width, data->rate_cpu);
    }
};
class DisplayCpuTotal: public DisplayData {
    public:
    DisplayCpuTotal() : DisplayData("CPU", 15, true, 0.0) {
    }
    virtual void display(int y, int x, PrintData *data) {
        char buffer[1024];
        mvprintw(y, x, "%s", format_time(data->total_cpu, buffer, 1024, width));
    }
};
class DisplayState: public DisplayData {
    public:
    DisplayState() : DisplayData("&S", 1, true, 0.0) {
    }
    virtual void display(int y, int x, PrintData *data) {
        mvprintw(y, x, "%c", data->state);
    }
};
class DisplayDuration: public DisplayData {
    public:
    DisplayDuration() : DisplayData("&DURATION", 15, true, 0.0) { }
    virtual void display(int y, int x, PrintData *data) {
        char buffer[1024];
        mvprintw(y, x, "%s", format_time(data->wallTime, buffer, 1024, width));
    }
};
class DisplayRSS: public DisplayData {
    public:
    DisplayRSS() : DisplayData("&RSS", 7, true, 0.0) { }
    virtual void display(int y, int x, PrintData *data) {
        char buffer[1024];
        mvprintw(y, x, "%s", format_memory(data->rss, buffer, 1024, width));
    }
};
class DisplayVMem: public DisplayData {
    public:
    DisplayVMem() : DisplayData("&VMEM", 7, true, 0.0) { }
    virtual void display(int y, int x, PrintData *data) {
        char buffer[1024];
        mvprintw(y, x, "%s", format_memory(data->vmem, buffer, 1024, width));
    }
};
class DisplayIORTotal: public DisplayData {
    public:
    DisplayIORTotal() : DisplayData("I&O_R", 7, true, 0.0) { }
    virtual void display(int y, int x, PrintData *data) {
        char buffer[1024];
        mvprintw(y, x, "%s", format_memory(data->total_ioRead, buffer, 1024, width));
    }
};
class DisplayIOWTotal: public DisplayData {
    public:
    DisplayIOWTotal() : DisplayData("IO_&W", 7, true, 0.0) { }
    virtual void display(int y, int x, PrintData *data) {
        char buffer[1024];
        mvprintw(y, x, "%s", format_memory(data->total_ioWrite, buffer, 1024, width));
    }
};
class DisplayIORRate: public DisplayData {
    public:
    DisplayIORRate() : DisplayData("IO_R/s", 7, true, 0.0) { }
    virtual void display(int y, int x, PrintData *data) {
        char buffer[1024];
        mvprintw(y, x, "%s", format_memory(data->rate_ioRead, buffer, 1024, width));
    }
};
class DisplayIOWRate: public DisplayData {
    public:
    DisplayIOWRate() : DisplayData("IO_W/s", 7, true, 0.0) { }
    virtual void display(int y, int x, PrintData *data) {
        char buffer[1024];
        mvprintw(y, x, "%s", format_memory(data->rate_ioWrite, buffer, 1024, width));
    }
};
class DisplayThreads: public DisplayData {
    public:
    DisplayThreads() : DisplayData("&TH", 3, true, 0.0) { }
    virtual void display(int y, int x, PrintData *data) {
        mvprintw(y, x, "%*d", width, data->threads);
    }
};

bool sortHostname(PrintData *a, PrintData *b) {
    if (a == NULL || b == NULL) return false;
    return a->hostname < b->hostname;
}
bool sortHostnameRev(PrintData *a, PrintData *b) {
    if (a == NULL || b == NULL) return false;
    return !sortHostname(a, b);
}
bool sortUsername(PrintData *a, PrintData *b) {
    if (a == NULL || b == NULL) return false;
    return a->username < b->username;
}
bool sortUsernameRev(PrintData *a, PrintData *b) {
    if (a == NULL || b == NULL) return false;
    return a->username > b->username;
}
bool sortRSS(PrintData *a, PrintData *b) {
    if (a == NULL || b == NULL) return false;
    return a->rss < b->rss;
}

bool sortRSSRev(PrintData *a, PrintData *b) {
    if (a == NULL || b == NULL) return false;
    return a->rss > b->rss;
}

bool sortVMem(PrintData *a, PrintData *b) {
    if (a == NULL || b == NULL) return false;
    return a->vmem < b->vmem;
}

bool sortCpuRate(PrintData *a, PrintData *b) {
    if (a == NULL || b == NULL) return false;
    return a->rate_cpu < b->rate_cpu;
}
bool sortCpuRateRev(PrintData *a, PrintData *b) {
    if (a == NULL || b == NULL) return false;
    return !sortCpuRate(a, b);
}
bool sortMpiRank(PrintData *a, PrintData *b) {
    if (a == NULL || b == NULL) return false;
    return a->mpiRank < b->mpiRank;
}
bool sortMpiRankRev(PrintData *a, PrintData *b) {
    if (a == NULL || b == NULL) return false;
    return !sortMpiRank(a, b);
}

bool sortThreads(PrintData *a, PrintData *b) {
    if (a == NULL || b == NULL) return false;
    return a->threads < b->threads;
}
bool sortThreadsRev(PrintData *a, PrintData *b) {
    if (a == NULL || b == NULL) return false;
    return !sortThreads(a,b);
}

bool sortTotalIORead(PrintData *a, PrintData *b) {
    if (a == NULL || b == NULL) return false;
    return a->total_ioRead < b->total_ioRead;
}
bool sortTotalIOReadRev(PrintData *a, PrintData *b) {
    if (a == NULL || b == NULL) return false;
    return !sortTotalIORead(a,b);
}
bool sortTotalIOWrite(PrintData *a, PrintData *b) {
    if (a == NULL || b == NULL) return false;
    return a->total_ioWrite < b->total_ioWrite;
}
bool sortTotalIOWriteRev(PrintData *a, PrintData *b) {
    if (a == NULL || b == NULL) return false;
    return !sortTotalIOWrite(a,b);
}
bool sortPid(PrintData *a, PrintData *b) {
    if (a == NULL || b == NULL) return false;
    return a->pid < b->pid;
}
bool sortPidRev(PrintData *a, PrintData *b) {
    if (a == NULL || b == NULL) return false;
    return !sortPid(a,b);
}
bool sortAge(PrintData *a, PrintData *b) {
    if (a == NULL || b == NULL) return false;
    return a->recTime < b->recTime;
}
bool sortAgeRev(PrintData *a, PrintData *b) {
    if (a == NULL || b == NULL) return false;
    return !sortAge(a,b);
}
bool sortPPid(PrintData *a, PrintData *b) {
    if (a == NULL || b == NULL) return false;
    return a->ppid < b->ppid;
}
bool sortPPidRev(PrintData *a, PrintData *b) {
    if (a == NULL || b == NULL) return false;
    return !sortPPid(a,b);
}

bool sortVMemRev(PrintData *a, PrintData *b) {
    if (a == NULL || b == NULL) return false;
    return a->vmem > b->vmem;
}

bool sortDuration(PrintData *a, PrintData *b) {
    if (a == NULL || b == NULL) return false;
    return a->wallTime < b->wallTime;
}
bool sortDurationRev(PrintData *a, PrintData *b) {
    if (a == NULL || b == NULL) return false;
    return a->wallTime > b->wallTime;
}

class TopCurses {
  public:
    unordered_map<JobIdent, CurrentJobData*> jobData;
    unordered_map<JobIdent, JobData*> newJobData;
    vector<PrintData *> printData;
    string system;
    string jobid;
    string taskid;
    vector<DisplayData *> columns;
    int highlight_row;
    size_t item_start;
    int xlim, ylim;
    bool (*sortFunction)(PrintData *,PrintData *);

    TopCurses(const string &system, const string &jobid, const string &taskid) {
        this->system = system;
        this->jobid = jobid;
        this->taskid = taskid;
        initscr();
        cbreak(); // let os handle ctrl-c, ctrl-z
        keypad(stdscr, TRUE);
        noecho();
        halfdelay(5);
        highlight_row = 0;
        item_start = 0;
        xlim = 0;
        ylim = 0;
        sortFunction = NULL;

        columns.push_back(new DisplayAge());
        columns.push_back(new DisplayUser());
        columns.push_back(new DisplayHost());
        columns.push_back(new DisplayPid());
        columns.push_back(new DisplayPPid());
        columns.push_back(new DisplayCommand());
        columns.push_back(new DisplayCpuRate());
        columns.push_back(new DisplayState());
        columns.push_back(new DisplayDuration());
        columns.push_back(new DisplayRSS());
        columns.push_back(new DisplayVMem());
        columns.push_back(new DisplayIORTotal());
        columns.push_back(new DisplayIORRate());
        columns.push_back(new DisplayIOWTotal());
        columns.push_back(new DisplayIOWRate());
        columns.push_back(new DisplayThreads());
        columns.push_back(new DisplayMpiRank());
        columns.push_back(new DisplayCpuTotal());
    }

    void printScreen() {
        getmaxyx(stdscr, ylim, xlim);
        erase();

        /* trim the data if any has aged out */
        time_t curr_time = time(NULL);
        int err = 0;
        class AgePrintObj {
            time_t cutoff;
            public:
            AgePrintObj(time_t _cutoff) {
                cutoff = _cutoff;
            }
            bool operator()(PrintData *tgt) {
                return tgt->recTime < cutoff;
            }
        };
        if ((err = pthread_mutex_lock(&data_lock)) != 0) fatal_error("printScreen failed to lock token.", err);
        printData.erase(remove_if(printData.begin(), printData.end(), AgePrintObj(curr_time - age_timeout)), printData.end());
        if ((err = pthread_mutex_unlock(&data_lock)) != 0) fatal_error("printScreen failed to unlock token.", err);

        if (sortFunction != NULL) {
            sort(printData.begin(), printData.end(), sortFunction);
        }
        /* determine window position */
        if (highlight_row < 0) {
            if (item_start - highlight_row < 0) {
                item_start = 0;
            } else {
                item_start -= highlight_row;
            }
            highlight_row = 0;
        } else if (highlight_row >= ylim) {
            item_start += (highlight_row - ylim)+1;
            if (item_start > printData.size()) {
                if (printData.size() - ylim > 0) {
                    item_start = printData.size() - ylim;
                } else {
                    item_start = 0;
                }
            }
            highlight_row = ylim - 1;
        }

        int extra = xlim;
        for (auto it = columns.begin(), end = columns.end(); it != end; it++) {
             extra -= (*it)->baseWidth;
             extra--; // for the space padding
        }
        for (auto it = columns.begin(), end = columns.end(); it != end; it++) {
            if (!(*it)->widthFixed && extra > 0) {
                (*it)->width = (*it)->baseWidth + (*it)->widthCoefficient * extra;
            } else {
                (*it)->width = (*it)->baseWidth;
            }
        }

        mvprintw(0, 0, "System: %s, Job: %s, Task: %s", system.c_str(), jobid.c_str(), taskid.c_str());
        int ypos = 1;
        int xpos = 0;
        int idx = 0;
        int item_idx = item_start;
        for (auto it = columns.begin(), end = columns.end(); it != end && xpos + (*it)->width <= xlim; it++) {
            (*it)->displayLabel(ypos, xpos);
            xpos += (*it)->width + 1;
        }
        ypos = 2;
        while (ypos <= ylim && item_idx < printData.size()) {
            PrintData *item = printData[item_idx];
            xpos = 0;
            idx = 0;
            char buffer[1024];
            if (highlight_row == ypos) {
                attr_on(A_REVERSE, NULL);
            }
            for (auto it = columns.begin(), end = columns.end(); it != end && xpos + (*it)->width <= xlim; it++) {
                (*it)->display(ypos, xpos, item);
                xpos += (*it)->width + 1;
            }
            if (highlight_row == ypos) {
                attr_off(A_REVERSE, NULL);
            }

            item_idx++;
            ypos++;
        }
        if (ypos == 2) {
            mvprintw(3, 0, "Awaiting data.");
        }
        refresh();
    }

    void eventLoop() {
        printScreen();
        while (cleanup == 0) {
            int ch = getch();
            switch (ch) {
                case 'q': kill(getpid(), SIGTERM); break;
                case 'd': sortFunction = sortDuration; break;
                case 'D': sortFunction = sortDurationRev; break;
                case 'v': sortFunction = sortVMem; break;
                case 'V': sortFunction = sortVMemRev; break;
                case 'r': sortFunction = sortRSS; break;
                case 'R': sortFunction = sortRSSRev; break;
                case 'h': sortFunction = sortHostname; break;
                case 'H': sortFunction = sortHostnameRev; break;
                case 'u': sortFunction = sortUsername; break;
                case 'U': sortFunction = sortUsernameRev; break;
                case '5': sortFunction = sortCpuRate; break;
                case '%': sortFunction = sortCpuRateRev; break;
                case 'm': sortFunction = sortMpiRank; break;
                case 'M': sortFunction = sortMpiRankRev; break;
                case 'p': sortFunction = sortPid; break;
                case 'P': sortFunction = sortPidRev; break;
                case 'i': sortFunction = sortPPid; break;
                case 'I': sortFunction = sortPPidRev; break;
                case 't': sortFunction = sortThreads; break;
                case 'T': sortFunction = sortThreadsRev; break;
                case 'o': sortFunction = sortTotalIORead; break;
                case 'O': sortFunction = sortTotalIOReadRev; break;
                case 'w': sortFunction = sortTotalIOWrite; break;
                case 'W': sortFunction = sortTotalIOWriteRev; break;
                case 'a': sortFunction = sortAge; break;
                case 'A': sortFunction = sortAgeRev; break;
                case KEY_UP: highlight_row--; break;
                case KEY_DOWN: highlight_row++; break;
                case KEY_PPAGE: highlight_row -= 2*ylim; break;
                case KEY_NPAGE: highlight_row += ylim; break;
            };
            printScreen();
        }
    }

    void setProcData(const string &hostname, const string &jobid, const string &taskid, procdata *data, int n) {
        JobIdent ident(hostname, jobid, taskid);
        JobData *newData = NULL;
        int err;
        if ((err = pthread_mutex_lock(&data_lock)) != 0) fatal_error("setProcData failed to lock token.", err);
        auto it = newJobData.find(ident);
        if (it != newJobData.end()) {
            newData = (*it).second;
        } else {
            newData = new JobData();
        }
        newData->setProcData(data, n);
        if (checkAndMaybeSet(ident, newData)) {
            if (it != newJobData.end()) {
                newJobData.erase(it);
            }
        } else {
            newJobData[ident] = newData;
        }
        if ((err = pthread_mutex_unlock(&data_lock)) != 0) fatal_error("setProcData failed to unlock token.", err);
    }

    void setProcStat(const string &hostname, const string &jobid, const string &taskid, procstat *data, int n) {
        JobIdent ident(hostname, jobid, taskid);
        JobData *newData = NULL;
        int err;
        if ((err = pthread_mutex_lock(&data_lock)) != 0) fatal_error("setProcStat failed to lock token.", err);
        auto it = newJobData.find(ident);
        if (it != newJobData.end()) {
            newData = (*it).second;
        } else {
            newData = new JobData();
        }
        newData->setProcStat(data, n);
        if (checkAndMaybeSet(ident, newData)) {
            if (it != newJobData.end()) {
                newJobData.erase(it);
            }
        } else {
            newJobData[ident] = newData;
        }
        if ((err = pthread_mutex_unlock(&data_lock)) != 0) fatal_error("setProcStat failed to unlock token.", err);
    }

    bool checkAndMaybeSet(JobIdent &ident, JobData *newData) {
        class TrimPrintObj {
            CurrentJobData *jobData;
            public:
            TrimPrintObj(CurrentJobData *_jobData) {
                jobData = _jobData;
            }
            bool operator()(PrintData *tgt) {
                return find(jobData->records.begin(), jobData->records.end(), tgt) != jobData->records.end();
            }
        };

        if (newData->ps == NULL || newData->pd == NULL) {
            return false;
        }
        JobDataDelta *delta = NULL;
        auto it = jobData.find(ident);
        if (it != jobData.end()) {
            CurrentJobData *curr = it->second;
            JobData *oldData = (*it).second->data;
            delta = new JobDataDelta(newData, oldData);
            printData.erase(remove_if(printData.begin(), printData.end(), TrimPrintObj(curr)), printData.end());
            delete (*it).second;
        }
        CurrentJobData *currData = new CurrentJobData(newData, delta);
        jobData[ident] = currData;

        for (int i = 0; i < newData->n_pd; i++) {
            procdata *pd = &(newData->pd[i]);
            procstat *ps = NULL;
            int ps_idx = 0;
            for (int j = 0; j < newData->n_ps && newData->ps[j].pid <= pd->pid; j++) {
                if (newData->ps[j].pid == pd->pid) {
                    ps = &(newData->ps[j]);
                    ps_idx = j;
                }
            }
            if (ps == NULL) {
                continue;
            }
            PrintData *d = new PrintData();
            d->hostname = ident.hostname;
            d->command =  string(pd->cmdArgs);
            auto user_it = usernames.find(ps->realUid);
            if (user_it != usernames.end()) {
                d->username = user_it->second;
            } else {
                struct passwd *user = getpwuid(ps->realUid);
                string username = "unknown";
                if (user != NULL) {
                    username = string(user->pw_name);
                }
                usernames[ps->realUid] = username;
                d->username = username;
            }

            replace(d->command.begin(), d->command.end(), '|', ' ');
            d->wallTime = (pd->recTime + pd->recTimeUSec*1e-6) - (pd->startTime + pd->startTimeUSec*1e-6);
            d->rate_cpu = NAN;
            d->rate_ioRead = NAN;
            d->rate_ioWrite = NAN;
            d->recTime = ps->recTime;
            d->pid = ps->pid;
            d->ppid = ps->ppid;
            if (delta != NULL && delta->dt > 0) {
                if (delta->d_utime[ps_idx] != NAN && delta->d_stime[ps_idx] != NAN)
                    d->rate_cpu = (delta->d_utime[ps_idx] + delta->d_stime[ps_idx]) / delta->dt;
                if (delta->d_ioread[ps_idx] != NAN) d->rate_ioRead = delta->d_ioread[ps_idx] / delta->dt;
                if (delta->d_iowrite[ps_idx] != NAN) d->rate_ioWrite = delta->d_iowrite[ps_idx] / delta->dt;
            }
            d->total_cpu = (ps->stime + ps->utime) / 100.;
            d->rss = ps->rss;
            d->vmem = ps->vsize;
            d->threads = ps->numThreads;
            d->state = ps->state;
            d->total_ioRead = ps->io_rchar;
            d->total_ioWrite = ps->io_wchar;
            d->mpiRank = ps->rtPriority;
            currData->records.push_back(d);
            printData.push_back(d);
        }

        return true;
    }

    ~TopCurses() {
        endwin();

        for (auto it = columns.begin(); it != columns.end(); it++) {
            delete *it;
        }
    }

protected:

};

TopCurses *screen = NULL;
ProcAMQPIO *conn = NULL;

static void *screen_start(void *t_curses) {
    TopCurses *curses = (TopCurses *) t_curses;
    curses->eventLoop();
}

static void *monitor_start(void *t_timeout) {
    int *timeout = (int *) t_timeout;
    last_update = time(NULL); //pretend we've seen an update recently
    int delta = 0;
    while (cleanup == 0) {
        sleep(2);
        delta = time(NULL) - last_update;
        if (delta > *timeout) {
            char buffer[1024];
            snprintf(buffer, 1024, "No data received for %d seconds", delta);
            fatal_error(buffer, 0);
        }
    }
}

static void screen_exit(TopCurses *screen, int exit_code) {
    if (screen != NULL) {
        delete screen;
    }
    if (conn != NULL) {
        delete conn;
    }
    cleanup = 1;
    exit(exit_code);
}

void fatal_error(const char *error, int err) {
    if (err != 0) {
        char buffer[1024];
        snprintf(buffer, 1024, "Failed: %s; bailing out.\n");
        errno = err;
        perror(buffer);
    } else {
        fprintf(stderr, "Failed: %s; %d; bailing out.\n", error, err);
    }
    kill(getpid(), SIGTERM);
}

void sig_handler(int signum) {
    screen_exit(screen, 128 + signum);
}

int main(int argc, char **argv) {
    const char *mqServer = DEFAULT_AMQP_HOST;
    const char *mqVHost = DEFAULT_AMQP_VHOST;
    int mqPort = DEFAULT_AMQP_PORT;
    const char *mqUser = DEFAULT_AMQP_USER;
    const char *mqPassword = DEFAULT_AMQP_PASSWORD;
    const char *mqExchangeName = DEFAULT_AMQP_EXCHANGE_NAME;
    int mqFrameSize = DEFAULT_AMQP_FRAMESIZE;

    const char *req_identifier = argv[1];
    const char *req_subidentifier = "*";


    screen = new TopCurses(getenv("NERSC_HOST"), req_identifier, req_subidentifier);
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

    conn = new ProcAMQPIO(mqServer, mqPort, mqVHost, mqUser, mqPassword, mqExchangeName, mqFrameSize, FILE_MODE_READ);
    conn->set_context("*", req_identifier, req_subidentifier);

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

        if (recordType == TYPE_PROCDATA) {
            procdata *ptr = (procdata *) data;
            screen->setProcData(hostname, identifier, subidentifier, ptr, nRecords);
        } else if (recordType == TYPE_PROCSTAT) {
            procstat *ptr = (procstat *) data;
            screen->setProcStat(hostname, identifier, subidentifier, ptr, nRecords);
        }
    }
    screen_exit(screen, 0);
}
