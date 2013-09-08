#include "config.h"
#include "ProcData.hh"
#include "ProcIO.hh"
#include <signal.h>
#include <string.h>
#include <iostream>

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <pthread.h>

#include <boost/program_options.hpp>
#define PROCMUXER_VERSION 2.2
namespace po = boost::program_options;

/* ProcMuxer
    * Takes in procmon data from all sources and writes it all out to hdf5 without filtering
    * Writes an HDF5 file for all procdata foreach day
*/

/* global variables - these are global for signal handling */
int cleanUpExitFlag = 0;
int resetOutputFileFlag = 0;
string queue_name;

void sig_handler(int signum) {
    /* if we receive any trapped signal, just set the cleanUpFlag
     * this will break the infinite loop and cause the message
     * buffer to get written out
     */
     cleanUpExitFlag = 1;
}

void sighup_handler(int signum) {
    /* if we receive any trapped signal, just set the cleanUpFlag
     * this will break the infinite loop and cause the message
     * buffer to get written out
     */
     resetOutputFileFlag = 1;
}

void version() {
    cout << "ProcMuxer " << PROCMUXER_VERSION;
    cout << endl;
    exit(0);
}


class ProcMuxerConfig {
public:
    ProcMuxerConfig(int argc, char **argv) {
        string tempMem;
        char buffer[BUFFER_SIZE];
        bzero(buffer, BUFFER_SIZE);
        gethostname(buffer, BUFFER_SIZE);
        buffer[BUFFER_SIZE-1] = 0;
        group = string(buffer);
        my_id = getpid();
        /* Parse command line arguments */
        po::options_description basic("Basic Options");
        basic.add_options()
            ("version", "Print version information")
            ("help,h", "Print help message")
            ("verbose,v", "Print extra (debugging) information")
        ;
        po::options_description config("Configuration Options");
        config.add_options()
            ("daemonize,d", "Daemonize the procmon process")
            ("hostname,H",po::value<std::string>(&(this->hostname))->default_value(DEFAULT_REDUCER_HOSTNAME), "identifier for tagging data")
            ("identifier,I",po::value<std::string>(&(this->identifier))->default_value(DEFAULT_REDUCER_IDENTIFIER), "identifier for tagging data")
            ("subidentifier,S",po::value<std::string>(&(this->subidentifier))->default_value(DEFAULT_REDUCER_SUBIDENTIFIER), "secondary identifier for tagging data")
            ("outputhdf5prefix,O",po::value<std::string>(&(this->outputHDF5FilenamePrefix))->default_value(DEFAULT_REDUCER_OUTPUT_PREFIX), "prefix for hdf5 output filename [final names will be prefix.YYYYMMDDhhmmss.h5 (required)")
            ("cleanfreq,c",po::value<int>(&(this->cleanFreq))->default_value(DEFAULT_REDUCER_CLEAN_FREQUENCY), "time between process hash table cleaning runs")
            ("maxage,m",po::value<int>(&(this->maxProcessAge))->default_value(DEFAULT_REDUCER_MAX_PROCESS_AGE), "timeout since last communication from a pid before allowing removal from hash table (cleaning)")
            ("maxwrites,w",po::value<int>(&(this->maxFileWrites))->default_value(DEFAULT_REDUCER_MAX_FILE_WRITES), "maximum writes for a single file before resetting")
            ("totalwrites,t",po::value<int>(&(this->maxTotalWrites))->default_value(DEFAULT_REDUCER_MAX_TOTAL_WRITES), "total writes before exiting")
            ("maxmem,M",po::value<string>(tempMem)->default_vaulue(DEFAULT_REDUCER_MAX_MEMORY), "maximum vmem consumption before exiting")
            ("statblock",po::value<int>(&(this->statBlockSize))->default_value(DEFAULT_STAT_BLOCK_SIZE), "number of stat records per block in hdf5 file" )
            ("datablock",po::value<int>(&(this->dataBlockSize))->default_value(DEFAULT_DATA_BLOCK_SIZE), "number of data records per block in hdf5 file" )
            ("fdblock",po::value<int>(&(this->dataBlockSize))->default_value(DEFAULT_FD_BLOCK_SIZE), "number of data records per block in hdf5 file" )
            ("group,g",po::value<string>(&(this->group)), "ProcMuxer group (defaults to local hostname)")
            ("id,i",po::value<int>(&(this->my_id)), "ProcMuxer ID (defaults to pid)")
            ("debug", "enter debugging mode")
        ;
        po::options_description mqconfig("AMQP Configuration Options");
        mqconfig.add_options()
            ("mqhostname,H", po::value<std::string>(&(this->mqServer))->default_value(DEFAULT_AMQP_HOST), "hostname for AMQP Server")
            ("mqport,P",po::value<unsigned int>(&(this->mqPort))->default_value(DEFAULT_AMQP_PORT), "port for AMQP Server")
            ("mqvhost,V",po::value<std::string>(&(this->mqVHost))->default_value(DEFAULT_AMQP_VHOST), "virtual-host for AMQP Server")
            ("mqexchange,E",po::value<std::string>(&(this->mqExchangeName))->default_value("procmon"), "exchange name for AMQP Server")
            ("mqUser",po::value<std::string>(&(this->mqUser))->default_value(DEFAULT_AMQP_USER), "virtual-host for AMQP Server")
            ("mqPassword",po::value<std::string>(&(this->mqPassword)), "virtual-host for AMQP Server")
            ("mqframe,F",po::value<unsigned int>(&(this->mqFrameSize))->default_value(DEFAULT_AMQP_FRAMESIZE), "maximum frame size for AMQP Messages (bytes)")
        ;

        po::options_description options;
        options.add(basic).add(config).add(mqconfig);
        po::variables_map vm;
        try {
            po::store(po::command_line_parser(argc, argv).options(options).run(), vm);
            po::notify(vm);
            if (vm.count("help")) {
                std::cout << options << std::endl;
                exit(0);
            }
            if (vm.count("version")) {
                version();
                exit(0);
            }
            if (vm.count("daemonize")) {
                daemonize = true;
            } else {
                daemonize = false;
            }
            if (vm.count("debug")) {
                debug = true;
            }
            if (vm.count("mqPassword") == 0) {
                mqPassword = DEFAULT_AMQP_PASSWORD;
            }
        } catch (std::exception &e) {
            std::cout << e.what() << std::endl;
            std::cout << options << std::endl;
            exit(1);
        }
    }
    string mqServer;
    string mqVHost;
    string mqExchangeName;
    string mqUser;
    string mqPassword;
    unsigned int mqPort;
    unsigned int mqFrameSize;
    unsigned int maxFileWrites;
    unsigned int maxTotalWrites;
    unsigned long maxMem;
    bool debug;
    bool daemonize;
    string group;
    int my_id;

    string hostname;
    string identifier;
    string subidentifier;

    string pidfile;

    string outputHDF5FilenamePrefix;

    int cleanFreq;
    int maxProcessAge;
    int dataBlockSize;
    int statBlockSize;
    int fdBlockSize;

};
ProcMuxerConfig *config = NULL;

static void daemonize() {
	pid_t pid, sid;

	if (getppid() == 1) {
		return; // already daemonized
	}
	pid = fork();
	if (pid < 0) {
		exit(1); // failed to fork
	}
	if (pid > 0) {
		exit(0); // this is the parent, so exit
	}
	umask(077);

	sid = setsid();
	if (sid < 0) {
		exit(1);
	}

	if ((chdir("/")) < 0) {
		exit(1);
	}

	freopen("/dev/null", "r", stdin);
	freopen("/dev/null", "w", stdout);
	freopen("/dev/null", "w", stderr);
}

/* pidfile start-up routine.
   Should be called after daemonizing, but before any privilege reduction.

   Will check if an existing pid file exists, if it does it will read that
   pid file, check to see if that pid is still running.

   If the pid isn't running, then the existing pidfile will be unlinked
  
   If the pid is running, then exit()

   Finally, the results of getpid() will be written into the pidfile. If the
   pid file fails to write, then exit()
*/
void pidfile(const string& pidfilename) {
    if (pidfilename.length() == 0) return;

    FILE *pidfile = NULL;
    char buffer[BUFFER_SIZE];
    pid_t my_pid = getpid();

    /* try to open existing pidfile */
    if ((pidfile = fopen(pidfilename.c_str(), "r")) != NULL) {

        /* try to read the pid */
        int readBytes = fread(buffer, sizeof(char), BUFFER_SIZE, pidfile);
        fclose(pidfile);

        if (readBytes > 0) {
            pid_t pid = atoi(buffer);

            if (pid != 0 && pid != my_pid) {
                struct stat st_stat;
                snprintf(buffer, BUFFER_SIZE, "/proc/%d/status", pid);
                if (stat(buffer, &st_stat) == 0) {
                    /* the process still exists! */
                    fprintf(stderr, "Process %d is still running, exiting.\n", pid);
                    exit(0);
                } else {
                    unlink(pidfilename.c_str());
                }
            }
        }
    }
    if ((pidfile = fopen(pidfilename.c_str(), "w")) != NULL) {
        fprintf(pidfile, "%d\n", my_pid);
        fclose(pidfile);
        return;
    }
    fprintf(stderr, "FAILED to write pidfile %s, exiting.", pidfilename.c_str());
    exit(1);
}

static bool control_amqp_status(amqp_rpc_reply_t status) {
    string amqpErrorMessage;
    switch (status.reply_type) {
        case AMQP_RESPONSE_NORMAL:
            return false;
            break;
        case AMQP_RESPONSE_NONE:
            amqpErrorMessage = "missing RPC reply type (ReplyVal:" + to_string( (unsigned int) status.reply_type) + ")";
            break;
        case AMQP_RESPONSE_LIBRARY_EXCEPTION:
            amqpErrorMessage = string(amqp_error_string2(status.library_error)) + " (ReplyVal:" + to_string( (unsigned int) status.reply_type) + ", LibraryErr: " + to_string( (unsigned int) status.library_error) + ")";
            break;
        case AMQP_RESPONSE_SERVER_EXCEPTION: {
            switch (status.reply.id) {
                case AMQP_CONNECTION_CLOSE_METHOD: {
                    amqp_connection_close_t *m = (amqp_connection_close_t *) status.reply.decoded;
                    amqpErrorMessage = "server connection error " + to_string((int) m->reply_code) + ", message: " +  string(reinterpret_cast<const char *>(m->reply_text.bytes), (int) m->reply_text.len);
                    break;
                }
                case AMQP_CHANNEL_CLOSE_METHOD: {
                    amqp_channel_close_t *m = (amqp_channel_close_t *) status.reply.decoded;
                    amqpErrorMessage = "server channel error " + to_string((int) m->reply_code) + ", message: " +  string(reinterpret_cast<const char *>(m->reply_text.bytes), (int) m->reply_text.len);
                    break;
                }
                default:
                    amqpErrorMessage = "unknown server error, method id " + to_string((int)status.reply.id);
                    break;
            }
            break;
        }
    }
    cerr << amqpErrorMessage << endl;
    return true;
}

static bool control_amqp_open(amqp_connection_state_t *conn) {
    *conn = amqp_new_connection();
    amqp_socket_t *socket = amqp_tcp_socket_new(*conn);
    int istatus = amqp_socket_open(socket, config->mqServer.c_str(), config->mqPort);
    if (istatus != 0) {
        cerr << "Failed AMQP connection to " << config->mqServer << ":" << config->mqPort << endl;
        return false;
    }
    if (control_amqp_status(amqp_login(*conn, config->mqVHost.c_str(), 0, config->mqFrameSize, 0, AMQP_SASL_METHOD_PLAIN, config->mqUser.c_str(), config->mqPassword.c_str()))) {
        cerr << "Failed AMQP login." << endl;
        return false;
    }
    amqp_channel_open(*conn, 1);
    if (control_amqp_status(amqp_get_rpc_reply(*conn))) {
        cerr << "Failed AMQP open channel." << endl;
        return false;
    }
    amqp_channel_open(*conn, 2);
    if (control_amqp_status(amqp_get_rpc_reply(*conn))) {
        cerr << "Failed AMQP open channel 2." << endl;
        return false;
    }
    amqp_exchange_declare(*conn, 1, amqp_cstring_bytes("ProcMan_Control"), amqp_cstring_bytes("topic"), 0, 0, amqp_empty_table);
    if (control_amqp_status(amqp_get_rpc_reply(*conn))) {
        cerr << "Failed ProcMan_Control exchange declaration" << endl;
        return false;
    }
    amqp_exchange_declare(*conn, 2, amqp_cstring_bytes("ProcMuxer_Control"), amqp_cstring_bytes("topic"), 0, 0, amqp_empty_table);
    if (control_amqp_status(amqp_get_rpc_reply(*conn))) {
        cerr << "Failed ProcMuxer_Control exchange declaration" << endl;
        return false;
    }
    amqp_queue_declare_ok_t* queue_reply = amqp_queue_declare(*conn, 1, amqp_empty_bytes, 0, 0, 1, 1, amqp_empty_table);
    if (control_amqp_status(amqp_get_rpc_reply(*conn))) {
        cerr << "Failed ProcMan_Control queue declaration" << endl;
        return false;
    }
    amqp_bytes_t queue = amqp_bytes_malloc_dup(queue_reply->queue);
    if (queue.bytes == NULL) {
        cerr << "Failed AMQP queue declare: out of memory!" << endl;
        return false;
    }
    amqp_queue_bind(*conn, 1, queue, amqp_cstring_bytes("ProcMan_Control"), amqp_cstring_bytes("*.*.*"), amqp_empty_table);
    if (control_amqp_status(amqp_get_rpc_reply(*conn))) {
        cerr << "Failed ProcMan_Control queue bind" << endl;
        amqp_bytes_free(queue);
        return false;
    }
    amqp_basic_consume(*conn, 1, queue, amqp_empty_bytes, 0, 1, 0, amqp_empty_table);
    if (control_amqp_status(amqp_get_rpc_reply(*conn))) {
        cerr << "Failed ProcMan_Control queue consume" << endl;
        amqp_bytes_free(queue);
        return false;
    }
    amqp_bytes_free(queue);
    return true;
}

static void control_amqp_close(amqp_connection_state_t *conn) {
    if (control_amqp_status(amqp_channel_close(*conn, 1, AMQP_REPLY_SUCCESS))) {
        string message = "AMQP channel close failed";
        cerr << message << endl;
    }
    if (control_amqp_status(amqp_channel_close(*conn, 2, AMQP_REPLY_SUCCESS))) {
        string message = "AMQP channel2 close failed";
        cerr << message << endl;
    }
    if (control_amqp_status(amqp_connection_close(*conn, AMQP_REPLY_SUCCESS))) {
        string message = "AMQP connection close failed";
        cerr << message << endl;
    }
    amqp_destroy_connection(*conn);
}

static bool control_amqp_read_message(amqp_connection_state_t *conn, string& group, int& id, string& messageType, string& message) {
    for ( ; ; ) {
        amqp_frame_t frame;
        int result;
        size_t body_received;
        size_t body_target;
        amqp_maybe_release_buffers(*conn);
        result = amqp_simple_wait_frame(*conn, &frame);
        if (result < 0) { break; }

        if (frame.frame_type != AMQP_FRAME_METHOD) {
            continue;
        }
        if (frame.payload.method.id != AMQP_BASIC_DELIVER_METHOD) {
            continue;
        }
        amqp_basic_deliver_t* d = (amqp_basic_deliver_t *) frame.payload.method.decoded;
        string routingKey((char*)d->routing_key.bytes, 0, (int) d->routing_key.len);
        result = amqp_simple_wait_frame(*conn, &frame);
        if (result < 0) {
            continue;
        }

        if (frame.frame_type != AMQP_FRAME_HEADER) {
            continue;
        }
        //amqp_basic_properties_t* p = (amqp_basic_properties_t *) frame.payload.properties.decoded;

        body_target = frame.payload.properties.body_size;
        char message_buffer[body_target+1];
        char* ptr = message_buffer;
        body_received = 0;

        while (body_received < body_target) {
            result = amqp_simple_wait_frame(*conn, &frame);
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
            size_t endPos = routingKey.find('.');
            group.assign(routingKey, 0, endPos);
            size_t endPos2 = routingKey.find('.', endPos+1);
            string temp;
            temp.assign(routingKey, endPos+1, endPos2);
            id = atoi(temp.c_str());
            messageType.assign(routingKey, endPos2+1, string::npos);
            message = string(message_buffer);
            return true;
        }
        break;
    }
    return false;
}

static bool control_send_message(amqp_connection_state_t *conn, const char *tag, char *_message) {
    char routingKey[512];
    bool message_sent = false;
    snprintf(routingKey, 512, "%s.%d.%s", config->group.c_str(), config->my_id, tag);
    routingKey[511] = 0;
    amqp_bytes_t message;
    message.bytes = _message;
    message.len = strlen(_message);
    for (int count = 0; count <= 1; count++) {
        int istatus = amqp_basic_publish(*conn, 2, amqp_cstring_bytes("ProcMuxer_Control"), amqp_cstring_bytes(routingKey), 0, 0, NULL, message);
        switch (istatus) {
            case 0:
                message_sent = true;
                break;
            case AMQP_STATUS_SOCKET_ERROR:
            case AMQP_STATUS_CONNECTION_CLOSED:
                /* deconstruct existing connection (if it exists), and rebuild */
                control_amqp_close(conn); //close the connection without acting on errors
                control_amqp_open(conn);
                break;
        }
        if (istatus != 0) {
            fprintf(stderr, "WARNING: error on message publication: %d\n", istatus);
        }
        if (message_sent) break;
    }
    return message_sent;
}

static void *control_thread_start(void *) {
    amqp_connection_state_t conn;
    char buffer[BUFFER_SIZE];
    if (!control_amqp_open(&conn)) {
        cerr << "Failed to open initial connection, bailing." << endl;
        exit(1);
    }

    while (cleanUpExitFlag == 0) {
        string group;
        string messageType;
        int id = 0;
        string message;
        if (control_amqp_read_message(&conn, group, id, messageType, message)) {
            if (group != "Any" && group != config->group) {
                continue; //this message is not for me
            }
            if (id != 0 && id != config->my_id) {
                continue; //this message is not for me
            }
            if (message == "ping") {
                gethostname(buffer, BUFFER_SIZE);
                buffer[BUFFER_SIZE-1] = 0;
                control_send_message(&conn, "pong", buffer);
            } else if (message == "get_prefix") {
                snprintf(buffer, BUFFER_SIZE, "%s", config->outputHDF5FilenamePrefix.c_str());
                control_send_message(&conn, "prefix", buffer);
            }
        }
    }

    control_amqp_close(&conn);
}

int main(int argc, char **argv) {
    cleanUpExitFlag = 0;
    resetOutputFileFlag = 0;
    char buffer[BUFFER_SIZE];

    config = new ProcMuxerConfig(argc, argv);

	if (config->daemonize) {
		daemonize();
	}

    pthread_t control_thread;
    int retCode = pthread_create(&control_thread, NULL, control_thread_start, NULL);
    if (retCode != 0) {
        errno = retCode;
        perror("Failed to start control thread, bailing out.");
        exit(1);
    }
    queue_name = string("ProcMuxer_") + config->group;


    ProcAMQPIO *conn = new ProcAMQPIO(config->mqServer, config->mqPort, config->mqVHost, config->mqUser, config->mqPassword, config->mqExchangeName, config->mqFrameSize, FILE_MODE_READ);
    conn->set_queue_name(queue_name);
    conn->set_context(config->hostname, config->identifier, config->subidentifier);
    ProcHDF5IO* outputFile = NULL;

    time_t currTimestamp = time(NULL);
    time_t lastClean = 0;
    struct tm currTm;
    memset(&currTm, 0, sizeof(struct tm));
    currTm.tm_isdst = -1;
    localtime_r(&currTimestamp, &currTm);
    int last_hour = currTm.tm_hour;
    string hostname, identifier, subidentifier;

    int saveCnt = 0;
    int nRecords = 0;
    string outputFilename;
    int count = 0;
    void* data = NULL;
    size_t data_size = 0;

    signal(SIGINT, sig_handler);
    signal(SIGTERM, sig_handler);
    signal(SIGXCPU, sig_handler);
    signal(SIGUSR1, sig_handler);
    signal(SIGUSR2, sig_handler);
    signal(SIGHUP, sighup_handler);

    while (cleanUpExitFlag == 0) {
        currTimestamp = time(NULL);
        localtime_r(&currTimestamp, &currTm);

        if (currTm.tm_hour !=  last_hour || outputFile == NULL || resetOutputFileFlag == 2) {
            /* flush the file buffers and clean up the old references to the old file */
            if (outputFile != NULL) {
                outputFile->flush();
                delete outputFile;
                outputFile = NULL;
            }

            /* use the current date and time as the suffix for this file */
            strftime(buffer, BUFFER_SIZE, "%Y%m%d%H%M%S", &currTm);
            outputFilename = config->outputHDF5FilenamePrefix + "." + string(buffer) + ".h5";
            outputFile = new ProcHDF5IO(outputFilename, FILE_MODE_WRITE);
            resetOutputFileFlag = 0;
        }
        last_hour = currTm.tm_hour;

        ProcRecordType recordType = conn->read_stream_record(&data, &data_size, &nRecords);
        conn->get_frame_context(hostname, identifier, subidentifier);

        if (data == NULL) {
            continue;
        }

        string message_type = "NA";
        outputFile->set_context(hostname, identifier, subidentifier);
        if (recordType == TYPE_PROCDATA) {
            procdata *ptr = (procdata *) data;
            outputFile->write_procdata(ptr, 0, nRecords);
            message_type = "procdata";
        } else if (recordType == TYPE_PROCSTAT) {
            procstat *ptr = (procstat *) data;
            outputFile->write_procstat(ptr, 0, nRecords);
            message_type = "procstat";
        } else if (recordType == TYPE_PROCFD) {
            procfd *ptr = (procfd *) data;
            outputFile->write_procfd(ptr, 0, nRecords);
            message_type = "procfd";
        }
        cerr << "Received message: " << hostname << "." << identifier << "." << subidentifier << "." << message_type << endl;

        time_t currTime = time(NULL);
        if (currTime - lastClean > config->cleanFreq || cleanUpExitFlag != 0 || resetOutputFileFlag == 1) {
            cout << "Begin Clean; Flushing data to disk..." << endl;
            outputFile->flush();
            outputFile->trim_segments(currTime - config->maxProcessAge);
            cout << "Flush Complete" << endl;

            int before_processes = 0;
            int before_process_capacity = 0;
            int after_processes = 0;
            int after_process_capacity = 0;
            time_t nowTime = time(NULL);
            time_t deltaTime = nowTime - currTime;
            cout << "Cleaning finished in " << deltaTime << " seconds" << endl;
            cout << "Data pool size: " <<  data_size << endl;
            lastClean = currTime;
            if (resetOutputFileFlag == 1) {
                resetOutputFileFlag++;
            }
        }
    }
    free(data);

    if (outputFile != NULL) {
        outputFile->flush();
        delete outputFile;
        outputFile = NULL;
    }
    if (conn != NULL) {
        delete conn;
        conn = NULL;
    }
    delete config;
    return 0;
}
