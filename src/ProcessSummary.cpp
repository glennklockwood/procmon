#include "ProcessSummary.hh"

ProcessSummary::ProcessSummary() {
    memset(this, 0, sizeof(ProcessSummary));
}

void ProcessSummary::update(const procdata *pd) {
}

void ProcessSummary::update(const procstat *ps) {
}

void ProcessSummary::update(const procfd *fd) {
}

