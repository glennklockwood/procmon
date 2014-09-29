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

Scriptable *Scriptable::getScriptable(const char *exePath, const char *cmdArgs) {
    const char *execName = exePath;
    const char *last_slash = strrchr(exePath, '/');
    if (last_slash != NULL) execName = last_slash + 1;
    if (strcmp(execName, "java") == 0) {
        return new JavaScriptable(exePath, cmdArgs);
    } else if (strncmp(execName, "python", 6) == 0) {
        return new PythonScriptable(exePath, cmdArgs);
    } else if (strncmp(execName, "perl", 4) == 0) {
        return new PerlScriptable(exePath, cmdArgs);
    } else if (strncmp(execName, "ruby", 4) == 0) {
        return new RubyScriptable(exePath, cmdArgs);
    } else if (strncmp(execName, "sh", 2) == 0 || strncmp(execName, "bash", 4) == 0) {
        return new BashScriptable(exePath, cmdArgs);
    } else if (strncmp(execName, "csh", 3) == 0 || strncmp(execName, "tcsh", 4) == 0) {
        return new CshScriptable(exePath, cmdArgs);
    }
}
