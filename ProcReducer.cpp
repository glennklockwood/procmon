#include "ProcData.hh"
#include "ProcIO.hh"

/* ProcReducer
    * Takes in procmon data from all sources and only writes the minimal record
    * Writes an HDF5 file for all procdata foreach day
*/

/* TODO: write FILE_MODE_READ setup for ProcAMPQIO */

int main(int argc, char **argv) {

    ProcReducerConfig config(argc, argv);
    ProcAMPQIO conn(config.mqServer, config.mqPort, config.mqVHost, config.mqUser, config.mqPassword, config.mqExchangeName, config.mqFrameSize, FILE_MODE_READ);
    conn.setContext("*","*","*");
    ProcIO* outputFile = NULL;
    procstat* procstatPtr = NULL;
    procdata* procdataPtr = NULL;

    time_t currTimestamp = time(NULL);
    struct tm currTm;
    memset(&currTm, 0, sizeof(struct tm));
    currTm.tm_isdst = -1;
    localtime_r(&currTimestamp, &currTm);
    int last_day = currTm.tm_mday
    int nRecords = 0;
    std::string hostname, identifier, subidentifier;

    while (true) {
        currTimestamp = time(NULL);
        localtime_r(&currTimestamp, &currTm);

        if (currTm.tm_mday != last_day || outputFile == NULL) {
            if (outputFile != NULL) {
                delete outputFile;
                outputFile = NULL;
            }
            outputFile = new ProcHDF5IO(filenameBuffer, FILE_MODE_WRITE);
        }
        last_day = currTm.tm_mday;

        ProcFileRecordType recordType = conn.read_stream_record(&procdataPtr, &procstatPtr, &nRecords);
        conn.get_context(hostname, identifier, subidentifier);

        for (int i = 0; i < nRecords; i++) {
            int pid = recordType == TYPE_PROCDATA ? (*procdataPtr)[i].pid : recordType == TYPE_PROCSTAT ? (*procstatPtr)[i].pid : -1;
            if (pid <= 0) {
                std::cerr <,
            std::string key = hostname + "/" + identifier + "." subidentifier + ":" + std::to_string( recordType == TYPE_PROCDATA ? 
        
        
        /* got some records! Now, to look up the latest of these
    }
    return 0;
}
