#ifndef __QPROC_HH_
#define __QPROC_HH_

#include <string>
#include <vector>
#include <map>
#include <algorithm>
#include <iterator>
#include <stdlib.h>
#include <iostream>

#include "ProcData.hh"
#include "ProcCache.hh"

#include <nclq.hh>

#include <boost/program_options.hpp>

class qpDataSource : public nclq::DataSource {
    const vector<nclq::VarDescriptor> symbols;
    Cache<procdata> *pd;
    Cache<procstat> *ps;
    Cache<procfd>   *fd;

    bool iterateOverPS;
    bool iterateOverPD;
    bool iterateOverFD;

    int pid;

    /* need variables to keep state in each cache and track current
       position in cache arrays for each datatype */

    void prepareQuery(nclq::Expression *expr);
    public:
    qpDataSource(Cache<procdata>*, Cache<procstat>*, Cache<procfd>*);
    ~qpDataSource();

    virtual const vector<nclq::VarDescriptor> &getSymbols() const;
    virtual bool getNext();
    virtual bool setValue(int idx, nclq::Data *data, nclq::SymbolType outputType);
    virtual int size();
    virtual void outputValue(ostream& out, int idx);
    virtual void prepareQuery(vector<nclq::Expression *>& declarations,
            vector<nclq::Expression *>& queries, 
            vector<nclq::Expression *>& output);
};


struct ProcessData {
    procstat *ps;
    procdata *pd;
    JobDelta<procstat> *delta_ps;
    const char *hostname;

    ProcessData(procstat *_ps, procdata *_pd, JobDelta<procstat> *_delta_ps, const char *_hostname):
        ps(_ps), pd(_pd), delta_ps(_delta_ps), hostname(_hostname)
    {
    }

    static inline void setIdentifier(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->identifier);
    }
    static inline void setSubidentifier(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->subidentifier);
    }
    static inline void setCmdArgs(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->subidentifier);
    }
    static inline void setHostname(ProcessData *p, Data *data) {
        data->setValue(hostname);
    }
    static inline void setExecName(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->pd == NULL) return;
        data->setValue(p->pd->execName);
    }
    static inline void setCmdArgs(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->pd == NULL) return;
        data->setValue(p->pd->cmdArgs);
    }
    static inline intType setCmdArgBytes(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->pd == NULL) return;
        data->setValue(p->ps->cmdArgBytes);
    }
    static inline void setExePath(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->pd == NULL) return;
        data->setValue(p->pd->exePath);
    }
    static inline void setCwdPath(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->pd == NULL) return;
        data->setValue(p->pd->cwdPath);
    }
    static inline void setObsTime(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->recTime + p->ps->recTimeUSec*1e-6);
    }
    static inline void setStartTime(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->startTime + p->ps->startTimeUSec*1e-6);
    }
    static inline void setPid(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->pid);
    }
    static inline void setPpid(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->ppid);
    }
    static inline void setPgrp(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->pgrp);
    }
    static inline void setSession(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->session);
    }
    static inline void setTty(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->tty);
    }
    static inline void setTpgid(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->tpgid);
    }
    static inline void setRealUid(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->realUid);
    }
    static inline void setEffUid(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->effUid);
    }
    static inline void setRealGid(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->realGid);
    }
    static inline void setEffGid(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->effGid);
    }
    static inline void setFlags(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->flags);
    }
    static inline void setUtime(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->utime);
    }
    static inline void setStime(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->stime);
    }
    static inline void setPriority(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->priority);
    }
    static inline void setNice(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->nice);
    }
    static inline void setNumThreads(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->numThreads);
    }
    static inline void setVsize(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->vsize);
    }
    static inline void setRss(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->rss);
    }
    static inline void setRsslim(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->rsslim);
    }
    static inline void setSignal(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->signal);
    }
    static inline void setBlocked(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->blocked);
    }
    static inline void setSigignore(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->sigignore);
    }
    static inline void setSigcatch(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->sigcatch);
    }
    static inline void setRtPriority(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->rtPriority);
    }
    static inline void setPolicy(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->policy);
    }
    static inline void setDelayacctBlkIOTicks(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->delayacctBlkIOTicks);
    }
    static inline void setDelayacctBlkIOTicks(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->delayacctBlkIOTicks);
    }
    static inline void setGuestTime(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->guestTime);
    }
    static inline void setVmpeak(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->vmpeak);
    }
    static inline void setRsspeak(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->rsspeak);
    }
    static inline void setCpusAllowed(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->cpusAllowed);
    }
    static inline void setIORchar(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->io_rchar);
    }
    static inline void setIOWchar(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->io_wchar);
    }
    static inline void setIOSyscr(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->io_syscr);
    }
    static inline void setIOSyscw(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->io_syscw);
    }
    static inline void setIOReadBytes(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->io_readBytes);
    }
    static inline void setIOWriteBytes(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->io_writeBytes);
    }
    static inline void setIOCancelledWriteBytes(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->io_cancelledWriteBytes);
    }
    static inline void setMSize(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->m_size);
    }
    static inline void setMResident(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->m_resident);
    }
    static inline void setMShare(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->m_share);
    }
    static inline void setMText(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->m_text);
    }
    static inline void setMData(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->m_data);
    }
    static inline void setStateSince(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->delta_ps == NULL || delta_ps->stateSince == 0) return;
        data->setValue(p->delta_ps->stateSince);
    }
    static inline void setStateAge(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->delta_ps == NULL || delta_ps->stateSince == 0) return;
        data->setValue( time(NULL) - p->delta_ps->stateSince);
    }
    static inline void setDeltaStime(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->delta_ps == NULL) return;
        data->setValue(p->delta_ps->delta_stime);
    }
    static inline void setDeltaUtime(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->delta_ps == NULL) return;
        data->setValue(p->delta_ps->delta_utime);
    }
    static inline void setDeltaIoread(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->delta_ps == NULL) return;
        data->setValue(p->delta_ps->delta_ioread);
    }
    static inline void setDeltaIowrite(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->delta_ps == NULL) return;
        data->setValue(p->delta_ps->delta_iowrite);
    }

    static inline void setCputicks(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->utime + ps->stime);
    }

    static inline void setDeltaCputicks(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->delta_ps == NULL) return;
        data->setValue(p->delta_ps->delta_stime + p->delta_ps->delta_utime);
    }

    static inline void setIo(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(p->ps->io_wchar + p->ps->io-rchar);
    }

    static inline void setDeltaIo(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->delta_ps == NULL) return;
        data->setValue(p->delta_ps->io_read + p->delta_ps->io_write);
    }

    static inline void setAge(ProcessData *p, Data *data) {
        if (p == NULL || data == NULL || p->ps == NULL) return;
        data->setValue(time(NULL) - p->ps->recTime);
    }

    static inline void setState(ProcessData *p, Data *data) {
        data->setValue(p->ps->state);
    }
};


namespace po = boost::program_options;
namespace fs = boost::filesystem;


std::ostream& operator<<(std::ostream &out, Data& data);


class QProcConfiguration {
public:
    std::vector<std::string> declarations;
    std::vector<std::string> queries;
    std::vector<std::string> outputFilenames;
    std::vector<std::string> outputColumns;
    std::string header;

    bool verbose;
    std::string configFile;
    char delimiter;

    std::vector<VarDescriptor> qVariables;
    std::map<std::string,int> qVariableMap;

    QProcConfiguration(int argc, char** argv);
};

#endif
