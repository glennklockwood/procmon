#include "ProcIO.hh"
#include "ProcData.hh"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <iostream>
#include <vector>
#include <string>

using namespace std;

ProcIO::ProcIO() {
	contextSet = false;
}

ProcIO::~ProcIO() {
}
bool ProcIO::set_context(const string& hostname, const string& identifier, const string& subidentifier) {
	cout << "set_context: ProcIO BaseClass called -- THIS IS BAD -- YOU REALLY SHOULDN'T SEE THIS!" << endl;
	return false;
}

unsigned int ProcIO::write_procdata(procdata* start_ptr, int count) {
	cout << "write_procdata: ProcIO BaseClass called -- THIS IS BAD -- YOU REALLY SHOULDN'T SEE THIS!" << endl;
	return 0;
}

unsigned int ProcIO::write_procstat(procstat* start_ptr, int count) {
	cout << "write_procstat: ProcIO BaseClass called -- THIS iS BAD -- YOU REALLY SHOULDN'T SEE THIS!" << endl;
	return 0;
}

unsigned int ProcIO::write_procfd(procfd* start_ptr, int count) {
	cout << "write_procfd: ProcIO BaseClass called -- THIS iS BAD -- YOU REALLY SHOULDN'T SEE THIS!" << endl;
	return 0;
}

#ifdef USE_AMQP
ProcAMQPIO::ProcAMQPIO(const string& _mqServer, int _port, const string& _mqVHost, 
	const string& _username, const string& _password, const string& _exchangeName, 
	const int _frameSize, const ProcIOFileMode _mode):

	mqServer(_mqServer),port(_port),mqVHost(_mqVHost),username(_username),password(_password),
	exchangeName(_exchangeName),frameSize(_frameSize),mode(_mode)

{
	connected = false;
	amqpError = false;
    try {
	    _amqp_open();
    } catch (const ProcIOException &e) {
        cerr << "Caught (and ignored) exception: " << e.what() << endl;
    }
}

bool ProcAMQPIO::_amqp_open() {
	int istatus = 0;
	conn = amqp_new_connection();
	socket = amqp_tcp_socket_new(conn);
	istatus = amqp_socket_open(socket, mqServer.c_str(), port);
	if (istatus != 0) {
		throw ProcIOException("Failed AMQP connection to " + mqServer + ":" + to_string(port));
	}
	//amqp_set_socket(conn, socket);
	_amqp_eval_status(amqp_login(conn, mqVHost.c_str(), 0, frameSize, 0, AMQP_SASL_METHOD_PLAIN, username.c_str(), password.c_str()));
	if (amqpError) {
		throw ProcIOException("Failed AMQP login to " + mqServer + ":" + to_string(port) + " as " + username + "; Error: " + amqpErrorMessage);
	}

	amqp_channel_open(conn, 1);
	_amqp_eval_status(amqp_get_rpc_reply(conn));
	if (amqpError) {
		throw ProcIOException("Failed AMQP open channel on " + mqServer + ":" + to_string(port) + "; Error: " + amqpErrorMessage);
	}

	amqp_exchange_declare(conn, 1, amqp_cstring_bytes(exchangeName.c_str()), amqp_cstring_bytes("topic"), 0, 0, amqp_empty_table);
	_amqp_eval_status(amqp_get_rpc_reply(conn));
	if (amqpError) {
		throw ProcIOException("Failed to declare exchange: " + exchangeName + "; Error: " + amqpErrorMessage);
	}
	connected = true;
	return connected;
}

bool ProcAMQPIO::_amqp_close(bool throw_errors) {
	if (connected) {
		_amqp_eval_status(amqp_channel_close(conn, 1, AMQP_REPLY_SUCCESS));
		if (amqpError) {
            string message = "AMQP channel close failed: " + amqpErrorMessage;
            if (throw_errors) {
                throw ProcIOException(message);
            } else {
                cerr << message << endl;
            }
		}
		_amqp_eval_status(amqp_connection_close(conn, AMQP_REPLY_SUCCESS));
		if (amqpError) {
            string message = "AMQP connection close failed: " + amqpErrorMessage;
            if (throw_errors) {
                throw ProcIOException(message);
            } else {
                cerr << message << endl;
            }
		}
		amqp_destroy_connection(conn);
        connected = false;
	}
    return !connected;
}

bool ProcAMQPIO::_amqp_eval_status(amqp_rpc_reply_t status) {
	amqpError = false;
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
	amqpError = true;
	return amqpError;
}

ProcAMQPIO::~ProcAMQPIO() {
    _amqp_close(false);
}

ProcRecordType ProcAMQPIO::read_stream_record(void **data, size_t *pool_size, int *nRec) {
	ProcRecordType recType = TYPE_INVALID;
    for ( ; ; ) {
        amqp_frame_t frame;
        int result;
        size_t body_received;
        size_t body_target;
        amqp_maybe_release_buffers(conn);
        result = amqp_simple_wait_frame(conn, &frame);
        if (result < 0) { break; }

        if (frame.frame_type != AMQP_FRAME_METHOD) {
            continue;
        }
        if (frame.payload.method.id != AMQP_BASIC_DELIVER_METHOD) {
            continue;
        }
        amqp_basic_deliver_t* d = (amqp_basic_deliver_t *) frame.payload.method.decoded;
        string routingKey((char*)d->routing_key.bytes, 0, (int) d->routing_key.len);
        result = amqp_simple_wait_frame(conn, &frame);
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
            result = amqp_simple_wait_frame(conn, &frame);
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
			_set_frame_context(routingKey);
			char *ptr = message_buffer;
			char *sPtr = message_buffer;
			int nRecords = 0;
			while (ptr - message_buffer < body_target) {
				if (*ptr == '=') {
					if (strncmp(sPtr, "nRecords", ptr-sPtr) != 0) {
						break;
					}
					sPtr = ptr+1;
				} else if (*ptr == '\n') {
					nRecords = atoi(sPtr);
					sPtr = ptr + 1;
					break;
				}
				ptr++;
			}
			if (nRecords > 0) {
                size_t required_size = 0;
				if (frameMessageType == "procstat") {
					required_size = sizeof(procstat) * nRecords;
					recType = TYPE_PROCSTAT;
				} else if (frameMessageType == "procdata") {
					required_size = sizeof(procdata) * nRecords;
					recType = TYPE_PROCDATA;
				} else if (frameMessageType == "procfd") {
                    required_size = sizeof(procfd) * nRecords;
                    recType = TYPE_PROCFD;
                }
                size_t alloc_size = (size_t) (required_size * 1.2);
                if (*data == NULL || *pool_size < required_size) {
                    *data = realloc(*data, alloc_size);
                    *pool_size = alloc_size;
                }
				if (*data == NULL) {
					throw ProcIOException("failed to allocate memory for " + to_string(nRecords) + " " + frameMessageType + " records");
				}
                bzero(*data, required_size);
				if (recType == TYPE_PROCSTAT) {
					_read_procstat((procstat*) *data, nRecords, sPtr, body_received - (sPtr - message_buffer));
					*nRec = nRecords;
				} else if (recType == TYPE_PROCDATA) {
					_read_procdata((procdata*) *data, nRecords, sPtr, body_received - (sPtr - message_buffer));
					*nRec = nRecords;
				} else if (recType == TYPE_PROCFD) {
                    _read_procfd((procfd*) *data, nRecords, sPtr, body_received - (sPtr - message_buffer));
                    *nRec = nRecords;
                }
			}
        }
        break;
    }
	return recType;
}

bool ProcAMQPIO::_read_procstat(procstat *startPtr, int nRecords, char* buffer, int nBytes) {
	char* ptr = buffer;
	char* ePtr = buffer + nBytes;
	char* sPtr = ptr;


    int pos = 0;
	int idx = 0;
	bool done = false;
	procstat* procStat = startPtr;
    while (idx < nRecords && ptr < ePtr) {
        if (*ptr == ',' || *ptr == '\n') {
			if (done) {
				procStat = &(startPtr[++idx]);
				pos = 0;
				done = false;
			}
            if (*ptr == '\n') {
				done = true;
			}
            *ptr = 0;
            switch (pos) {
                case 0: procStat->pid = atoi(sPtr); break;
                case 1: procStat->recTime = strtoul(sPtr, &ptr, 10); break;
                case 2: procStat->recTimeUSec = strtoul(sPtr, &ptr, 10); break;
                case 3: procStat->startTime = strtoul(sPtr, &ptr, 10); break;
                case 4: procStat->startTimeUSec = strtoul(sPtr, &ptr, 10); break;
                case 5: procStat->state = *sPtr; break;
                case 6: procStat->ppid = atoi(sPtr); break;
                case 7: procStat->pgrp = atoi(sPtr); break;
                case 8: procStat->session = atoi(sPtr); break;
                case 9: procStat->tty = atoi(sPtr); break;
                case 10: procStat->tpgid = atoi(sPtr); break;
                case 11: procStat->flags = (unsigned int) strtoul(sPtr, &ptr, 10); break;
                case 12: procStat->utime = strtoul(sPtr, &ptr, 10); break;
                case 13: procStat->stime = strtoul(sPtr, &ptr, 10); break;
                case 14: procStat->priority = strtol(sPtr, &ptr, 10); break;
                case 15: procStat->nice = strtol(sPtr, &ptr, 10); break;
                case 16: procStat->numThreads = strtol(sPtr, &ptr, 10); break;
                case 17: procStat->vsize = strtoul(sPtr, &ptr, 10); break;
                case 18: procStat->rss = strtoul(sPtr, &ptr, 10); break;
                case 19: procStat->rsslim = strtoul(sPtr, &ptr, 10); break;
                case 20: procStat->vmpeak = strtoul(sPtr, &ptr, 10); break;
                case 21: procStat->rsspeak = strtoul(sPtr, &ptr, 10); break;
                case 22: procStat->signal = strtoul(sPtr, &ptr, 10); break;
                case 23: procStat->blocked = strtoul(sPtr, &ptr, 10); break;
                case 24: procStat->sigignore = strtoul(sPtr, &ptr, 10); break;
                case 25: procStat->sigcatch = strtoul(sPtr, &ptr, 10); break;
                case 26: procStat->cpusAllowed = atoi(sPtr); break;
                case 27: procStat->rtPriority = (unsigned int) strtoul(sPtr, &ptr, 10); break;
                case 28: procStat->policy = (unsigned int) strtoul(sPtr, &ptr, 10); break;
                case 29: procStat->guestTime = strtoul(sPtr, &ptr, 10); break;
                case 30: procStat->delayacctBlkIOTicks = strtoull(sPtr, &ptr, 10); break;
                case 31: procStat->io_rchar = strtoull(sPtr, &ptr, 10); break;
                case 32: procStat->io_wchar = strtoull(sPtr, &ptr, 10); break;
                case 33: procStat->io_syscr = strtoull(sPtr, &ptr, 10); break;
                case 34: procStat->io_syscw = strtoull(sPtr, &ptr, 10); break;
                case 35: procStat->io_readBytes = strtoull(sPtr, &ptr, 10); break;
                case 36: procStat->io_writeBytes = strtoull(sPtr, &ptr, 10); break;
                case 37: procStat->io_cancelledWriteBytes = strtoull(sPtr, &ptr, 10); break;
                case 38: procStat->m_size = strtoul(sPtr, &ptr, 10); break;
                case 39: procStat->m_resident = strtoul(sPtr, &ptr, 10); break;
                case 40: procStat->m_share = strtoul(sPtr, &ptr, 10); break;
                case 41: procStat->m_text = strtoul(sPtr, &ptr, 10); break;
                case 42: procStat->m_data = strtoul(sPtr, &ptr, 10); break;
                case 43: procStat->realUid = strtoul(sPtr, &ptr, 10); break;
                case 44: procStat->effUid = strtoul(sPtr, &ptr, 10); break;
                case 45: procStat->realGid = strtoul(sPtr, &ptr, 10); break;
                case 46: procStat->effGid = strtoul(sPtr, &ptr, 10); break;
            }
            pos++;
            sPtr = ptr + 1;
        }
        ptr++;
    }
    return idx == nRecords;
}

bool ProcAMQPIO::_read_procdata(procdata *startPtr, int nRecords, char* buffer, int nBytes) {
	char* ptr = buffer;
	char* ePtr = buffer + nBytes;
	char* sPtr = ptr;

    int pos = 0;
	int idx = 0;
	int readBytes = -1;
	bool done = false;
	procdata* procData = startPtr;
    while (idx < nRecords && ptr < ePtr) {
        if ((*ptr == ',' || *ptr == '\n') && (readBytes < 0 || readBytes == (ptr-sPtr))) {
			if (done) {
				procData = &(startPtr[++idx]);
				pos = 0;
				readBytes = -1;
				done = false;
			}
            if (*ptr == '\n') {
				done = true;
			}
            *ptr = 0;
            switch (pos) {
                case 0: procData->pid = atoi(sPtr); break;
                case 1: procData->ppid = atoi(sPtr); break;
                case 2: procData->recTime = strtoul(sPtr, &ptr, 10); break;
                case 3: procData->recTimeUSec = strtoul(sPtr, &ptr, 10); break;
                case 4: procData->startTime = strtoul(sPtr, &ptr, 10); break;
                case 5: procData->startTimeUSec = strtoul(sPtr, &ptr, 10); break;
                case 6: readBytes = atoi(sPtr); break;
                case 7:
					memcpy(procData->execName, sPtr, sizeof(char)*readBytes);
					procData->execName[readBytes < BUFFER_SIZE ? readBytes : BUFFER_SIZE-1] = 0;
					readBytes = -1;
					break;
                case 8: readBytes = atoi(sPtr); break;
                case 9:
					memcpy(procData->cmdArgs, sPtr, sizeof(char)*readBytes);
					procData->cmdArgs[readBytes < BUFFER_SIZE ? readBytes : BUFFER_SIZE-1] = 0;
					procData->cmdArgBytes = readBytes;
					readBytes = -1;
					break;
                case 10: readBytes = atoi(sPtr); break;
                case 11:
					memcpy(procData->exePath, sPtr, sizeof(char)*readBytes);
					procData->exePath[readBytes < EXEBUFFER_SIZE ? readBytes : EXEBUFFER_SIZE-1] = 0;
					readBytes = -1;
					break;
                case 12: readBytes = atoi(sPtr); break;
                case 13:
					memcpy(procData->cwdPath, sPtr, sizeof(char)*readBytes);
					procData->cwdPath[readBytes < BUFFER_SIZE ? readBytes : BUFFER_SIZE - 1] = 0;
					readBytes = -1;
					break;
            }
            pos++;
            sPtr = ptr + 1;
        }
        ptr++;
    }
    return idx != nRecords;
}

bool ProcAMQPIO::_read_procfd(procfd *startPtr, int nRecords, char* buffer, int nBytes) {
	char* ptr = buffer;
	char* ePtr = buffer + nBytes;
	char* sPtr = ptr;

    int pos = 0;
	int idx = 0;
	int readBytes = -1;
	bool done = false;
	procfd* procFD = startPtr;
    while (idx < nRecords && ptr < ePtr) {
        if ((*ptr == ',' || *ptr == '\n') && (readBytes < 0 || readBytes == (ptr-sPtr))) {
			if (done) {
				procFD = &(startPtr[++idx]);
				pos = 0;
				readBytes = -1;
				done = false;
			}
            if (*ptr == '\n') {
				done = true;
			}
            *ptr = 0;
            switch (pos) {
                case 0: procFD->pid = atoi(sPtr); break;
                case 1: procFD->ppid = atoi(sPtr); break;
                case 2: procFD->recTime = strtoul(sPtr, &ptr, 10); break;
                case 3: procFD->recTimeUSec = strtoul(sPtr, &ptr, 10); break;
                case 4: procFD->startTime = strtoul(sPtr, &ptr, 10); break;
                case 5: procFD->startTimeUSec = strtoul(sPtr, &ptr, 10); break;
                case 6: readBytes = atoi(sPtr); break;
                case 7:
					memcpy(procFD->path, sPtr, sizeof(char)*readBytes);
					procFD->path[readBytes < BUFFER_SIZE ? readBytes : BUFFER_SIZE - 1] = 0;
					readBytes = -1;
					break;
                case 8: procFD->fd = atoi(sPtr); break;
                case 9: procFD->mode = (unsigned int) atoi(sPtr); break;
            }
            pos++;
            sPtr = ptr + 1;
        }
        ptr++;
    }
    return idx != nRecords;
}

unsigned int ProcAMQPIO::write_procdata(procdata* start_ptr, int count) {
    unsigned int nBytes = 0;
    char* ptr = buffer;
    ptr += snprintf(ptr, AMQP_BUFFER_SIZE, "nRecords=%d\n", count);
    for (int i = 0; i < count; i++) {
        procdata* procData = &(start_ptr[i]);
        ptr += snprintf(ptr, AMQP_BUFFER_SIZE - (ptr - buffer), "%d,%d,%lu,%lu,%lu,%lu", procData->pid,procData->ppid,procData->recTime,procData->recTimeUSec,procData->startTime,procData->startTimeUSec);
        ptr += snprintf(ptr, AMQP_BUFFER_SIZE - (ptr - buffer), ",%lu,%s", strlen(procData->execName), procData->execName);
        ptr += snprintf(ptr, AMQP_BUFFER_SIZE - (ptr - buffer), ",%lu,", procData->cmdArgBytes);
		bcopy(procData->cmdArgs, ptr, procData->cmdArgBytes); ptr += procData->cmdArgBytes;
        ptr += snprintf(ptr, AMQP_BUFFER_SIZE - (ptr - buffer), ",%lu,%s", strlen(procData->exePath), procData->exePath);
        ptr += snprintf(ptr, AMQP_BUFFER_SIZE - (ptr - buffer), ",%lu,%s\n", strlen(procData->cwdPath), procData->cwdPath);
    }
	*ptr = 0;
    nBytes = ptr - buffer;
    if (nBytes == AMQP_BUFFER_SIZE) {
        fprintf(stderr, "WARNING: sending full buffer -- data will be truncated\n");
    }

	amqp_bytes_t message;
	message.bytes = buffer;
	message.len = nBytes;
    return _send_message("procdata", message) ? nBytes : 0;
}

unsigned int ProcAMQPIO::write_procstat(procstat* start_ptr, int count) {
    unsigned int nBytes = 0;
    char* ptr = buffer;
    ptr += snprintf(ptr, AMQP_BUFFER_SIZE, "nRecords=%d\n", count);
    for (int i = 0; i < count; i++) {
        procstat* procStat = &(start_ptr[i]);
        ptr += snprintf(ptr, AMQP_BUFFER_SIZE - (ptr - buffer), "%d,%lu,%lu,%lu,%lu",procStat->pid,procStat->recTime,procStat->recTimeUSec,procStat->startTime,procStat->startTimeUSec);
        ptr += snprintf(ptr, AMQP_BUFFER_SIZE - (ptr - buffer), ",%c,%d,%d,%d",procStat->state,procStat->ppid,procStat->pgrp,procStat->session);
        ptr += snprintf(ptr, AMQP_BUFFER_SIZE - (ptr - buffer), ",%d,%d,%u,%lu,%lu",procStat->tty,procStat->tpgid,procStat->flags,procStat->utime,procStat->stime);
        ptr += snprintf(ptr, AMQP_BUFFER_SIZE - (ptr - buffer), ",%ld,%ld,%ld,%lu,%lu",procStat->priority,procStat->nice,procStat->numThreads,procStat->vsize,procStat->rss);
        ptr += snprintf(ptr, AMQP_BUFFER_SIZE - (ptr - buffer), ",%lu,%lu,%lu,%lu,%lu",procStat->rsslim,procStat->vmpeak,procStat->rsspeak,procStat->signal,procStat->blocked);
        ptr += snprintf(ptr, AMQP_BUFFER_SIZE - (ptr - buffer), ",%lu,%lu,%d,%u,%u",procStat->sigignore,procStat->sigcatch,procStat->cpusAllowed,procStat->rtPriority,procStat->policy);
        ptr += snprintf(ptr, AMQP_BUFFER_SIZE - (ptr - buffer), ",%lu,%llu,%llu,%llu,%llu",procStat->guestTime,procStat->delayacctBlkIOTicks,procStat->io_rchar,procStat->io_wchar,procStat->io_syscr);
        ptr += snprintf(ptr, AMQP_BUFFER_SIZE - (ptr - buffer), ",%llu,%llu,%llu,%llu,%lu",procStat->io_syscw,procStat->io_readBytes,procStat->io_writeBytes,procStat->io_cancelledWriteBytes, procStat->m_size);
        ptr += snprintf(ptr, AMQP_BUFFER_SIZE - (ptr - buffer), ",%lu,%lu,%lu,%lu,%lu",procStat->m_resident,procStat->m_share,procStat->m_text,procStat->m_data, procStat->realUid); 
        ptr += snprintf(ptr, AMQP_BUFFER_SIZE - (ptr - buffer), ",%lu,%lu,%lu\n",procStat->effUid, procStat->realGid, procStat->effGid);
    }
	*ptr = 0;
    nBytes = ptr - buffer;
    if (nBytes == AMQP_BUFFER_SIZE) {
        fprintf(stderr, "WARNING: sending full buffer -- data will be truncated\n");
    }

	amqp_bytes_t message;
	message.bytes = buffer;
	message.len = nBytes;
    return _send_message("procstat", message) ? nBytes : 0;
}

bool ProcAMQPIO::_send_message(const char *tag, amqp_bytes_t& message) {
	char routingKey[512];
    bool message_sent = false;
	snprintf(routingKey, 512, "%s.%s.%s.%s", hostname.c_str(), identifier.c_str(), subidentifier.c_str(), tag);
    routingKey[511] = 0;
    for (int count = 0; count <= PROCMON_AMQP_MESSAGE_RETRIES; count++) {
	    int istatus = amqp_basic_publish(conn, 1, amqp_cstring_bytes(exchangeName.c_str()), amqp_cstring_bytes(routingKey), 0, 0, NULL, message);
        switch (istatus) {
            case 0:
                message_sent = true;
                break;
            case AMQP_STATUS_SOCKET_ERROR:
            case AMQP_STATUS_CONNECTION_CLOSED:
                /* deconstruct existing connection (if it exists), and rebuild */
                _amqp_close(false); //close the connection without acting on errors
                try {
                    _amqp_open();   //attempt to reconnect
                } catch (const ProcIOException& e) {
                    cerr << "Trapped (and ignoring) error: " << e.what() << endl;
                }
                break;

        }
        if (istatus != 0) {
		    fprintf(stderr, "WARNING: error on message publication: %d\n", istatus);
        }
        if (message_sent) break;
	}
    return message_sent;
}

unsigned int ProcAMQPIO::write_procfd(procfd* start_ptr, int count) {
    unsigned int nBytes = 0;
    char* ptr = buffer;
    ptr += snprintf(ptr, AMQP_BUFFER_SIZE, "nRecords=%d\n", count);
    for (int i = 0; i < count; i++) {
        procfd* procFD = &(start_ptr[i]);
        ptr += snprintf(ptr, AMQP_BUFFER_SIZE - (ptr - buffer), "%d,%d,%lu,%lu,%lu,%lu", procFD->pid,procFD->ppid,procFD->recTime,procFD->recTimeUSec,procFD->startTime,procFD->startTimeUSec);
        ptr += snprintf(ptr, AMQP_BUFFER_SIZE - (ptr - buffer), ",%lu,%s", strlen(procFD->path), procFD->path);
        ptr += snprintf(ptr, AMQP_BUFFER_SIZE - (ptr - buffer), ",%d,%u\n", procFD->fd,procFD->mode);
    }
	*ptr = 0;
    nBytes = ptr - buffer;
    if (nBytes == AMQP_BUFFER_SIZE) {
        fprintf(stderr, "WARNING: sending full buffer -- data will be truncated\n");
    }

	amqp_bytes_t message;
	message.bytes = buffer;
	message.len = nBytes;
    return _send_message("procfd", message) ? nBytes : 0;
}

bool ProcAMQPIO::set_context(const string& _hostname, const string& _identifier, const string& _subidentifier) {
	size_t endPos = _hostname.find('.');
	endPos = endPos == string::npos ? _hostname.size() : endPos;
	hostname.assign(_hostname, 0, endPos);

	endPos = _identifier.find('.');
	endPos = endPos == string::npos ? _identifier.size() : endPos;
	identifier.assign(_identifier, 0, endPos);

	endPos = _subidentifier.find('.');
	endPos = endPos == string::npos ? _subidentifier.size() : endPos;
	subidentifier.assign(_subidentifier, 0, endPos);

	if (mode == FILE_MODE_READ) {
		try {
			_amqp_bind_context();
		} catch (ProcIOException& e) {
			ProcIOException e2(string("FAILED to set_context in ProcAMQPIO (declare and bind queue): ") + e.what());
			throw e2;
		}
	}
	contextSet = true;
	return true;
}

bool ProcAMQPIO::set_queue_name(const string& _queue_name) {
	if (mode == FILE_MODE_READ && !contextSet) {
        queue_name = _queue_name;
        cerr << "setting plannned queue name to " << queue_name << endl;
        return true;
    }
    return false;
}


bool ProcAMQPIO::get_frame_context(string& _hostname, string& _identifier, string& _subidentifier) {
	_hostname = frameHostname;
	_identifier = frameIdentifier;
	_subidentifier = frameSubidentifier;
	return true;
}

bool ProcAMQPIO::_set_frame_context(const string& routingKey) {
	size_t pos = 0;
	size_t lpos = 0;
	int idx = 0;
	while ((pos = routingKey.find('.', lpos)) != string::npos) {
		switch(idx++) {
			case 0:
				frameHostname.assign(routingKey, lpos, pos - lpos);
				break;
			case 1:
				frameIdentifier.assign(routingKey, lpos, pos - lpos);
				break;
			case 2:
				frameSubidentifier.assign(routingKey, lpos, pos - lpos);
				break;
		}
		lpos = pos+1;
	}
	if (idx == 3) {
		frameMessageType.assign(routingKey, lpos, routingKey.size() - lpos);
	}
	return idx == 3;
}

bool ProcAMQPIO::_amqp_bind_context() {
    amqp_bytes_t queue_name_bytes = amqp_empty_bytes;
    if (queue_name.length() > 0) {
        queue_name_bytes = amqp_cstring_bytes(queue_name.c_str());
        cerr << "picked up queue_name: " << queue_name << endl;
    }
	amqp_queue_declare_ok_t* queue_reply = amqp_queue_declare(conn, 1, queue_name_bytes, 0, 0, 0, 1, amqp_empty_table);
	_amqp_eval_status(amqp_get_rpc_reply(conn));
	if (amqpError) {
		throw ProcIOException("Failed AMQP queue declare on " + mqServer + ":" + to_string(port) + ", exchange " + exchangeName + "; Error: " + amqpErrorMessage);
	}

	amqp_bytes_t queue = amqp_bytes_malloc_dup(queue_reply->queue);
    if (queue.bytes == NULL) {
        throw ProcIOException("Failed AMQP queue declare: out of memory!");
    }

    string bindKey = hostname + "." + identifier + "." + subidentifier + ".*";
    amqp_queue_bind(conn, 1, queue, amqp_cstring_bytes(exchangeName.c_str()), amqp_cstring_bytes(bindKey.c_str()), amqp_empty_table);
    _amqp_eval_status(amqp_get_rpc_reply(conn));
    if (amqpError) {
        throw ProcIOException("Failed AMQP queue bind on " + mqServer + ":" + to_string(port) + ", exchange " + exchangeName + "; Error: " + amqpErrorMessage);
    }
    amqp_basic_consume(conn, 1, queue, amqp_empty_bytes, 0, 1, 0, amqp_empty_table);
    _amqp_eval_status(amqp_get_rpc_reply(conn));
    if (amqpError) {
        throw ProcIOException("Failed AMQP queue bind on " + mqServer + ":" + to_string(port) + ", exchange " + exchangeName + "; Error: " + amqpErrorMessage);
    }
	amqp_bytes_free(queue);
    return true;
}
#endif

#ifdef USE_HDF5
hdf5Ref::hdf5Ref(hid_t file, hid_t type_procstat, hid_t type_procdata, hid_t type_procfd, hid_t type_procobs, const string& hostname, ProcIOFileMode mode, unsigned int statBlockSize, unsigned int dataBlockSize, unsigned int fdBlockSize, unsigned int obsBlockSize) {
	group = -1;
	procstatDS = -1;
	procdataDS = -1;
    procfdDS = -1;
    procobsDS = -1;
	procstatSizeID = -1;
	procdataSizeID = -1;
    procfdSizeID = -1;
    procobsSizeID = -1;
	procstatSize = 0;
	procdataSize = 0;
    procfdSize = 0;
    procobsSize = 0;

    if (H5Lexists(file, hostname.c_str(), H5P_DEFAULT) == 1) {
        group = H5Gopen2(file, hostname.c_str(), H5P_DEFAULT);
	} else if (mode == FILE_MODE_WRITE) {
        group = H5Gcreate(file, hostname.c_str(), H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    }
    if (group < 0) {
       	throw ProcIOException("Failed to access hostname group: " + hostname);
    }

    if (H5Lexists(group, "procstat", H5P_DEFAULT) == 1 || mode == FILE_MODE_WRITE) {
	    procstatSize = open_dataset("procstat", type_procstat, statBlockSize, &procstatDS, &procstatSizeID, 0);
    }
    if (H5Lexists(group, "procdata", H5P_DEFAULT) == 1 || mode == FILE_MODE_WRITE) {
	    procdataSize = open_dataset("procdata", type_procdata, dataBlockSize, &procdataDS, &procdataSizeID, 0);
    }
    if (H5Lexists(group, "procfd", H5P_DEFAULT) == 1 || mode == FILE_MODE_WRITE) {
        procfdSize = open_dataset("procfd", type_procfd, fdBlockSize, &procfdDS, &procfdSizeID, 9);
    }
    if (H5Lexists(group, "procobs", H5P_DEFAULT) == 1 || mode == FILE_MODE_WRITE) {
        procobsSize = open_dataset("procobs", type_procobs, fdBlockSize, &procobsDS, &procobsSizeID, 9);
    }
}

unsigned int hdf5Ref::open_dataset(const char* dsName, hid_t type, int chunkSize, hid_t *dataset, hid_t *attribute, unsigned int zip_level) {
	unsigned int size = 0;

	if (group < 0) {
		throw ProcIOException("Called openDataset before group was opened!");
	}

    if (H5Lexists(group, dsName, H5P_DEFAULT) == 1) {
        *dataset = H5Dopen2(group, dsName, H5P_DEFAULT);
		*attribute = H5Aopen(*dataset, "nRecords", H5P_DEFAULT);
		H5Aread(*attribute, H5T_NATIVE_UINT, &size);
    } else {
        hid_t param;
		hid_t a_id;
		hsize_t rank = 1;
        hsize_t initial_dims = chunkSize;
        hsize_t maximal_dims = H5S_UNLIMITED;
        hid_t dataspace = H5Screate_simple(rank, &initial_dims, &maximal_dims);
    	hsize_t chunk_dims = chunkSize;

        param = H5Pcreate(H5P_DATASET_CREATE);
        if (zip_level > 0) {
            H5Pset_deflate(param, zip_level);
        }
        H5Pset_chunk(param, rank, &chunk_dims);
        *dataset = H5Dcreate(group, dsName, type, dataspace, H5P_DEFAULT, param, H5P_DEFAULT);
        H5Pclose(param);
        H5Sclose(dataspace);

		a_id = H5Screate(H5S_SCALAR);
		*attribute = H5Acreate2(*dataset, "nRecords", H5T_NATIVE_UINT, a_id, H5P_DEFAULT, H5P_DEFAULT);
		H5Awrite(*attribute, H5T_NATIVE_UINT, &size);
    }
	return size;
}

hdf5Ref::~hdf5Ref() {
	if (procstatSizeID >= 0) {
		H5Aclose(procstatSizeID);
	}
	if (procdataSizeID >= 0) {
		H5Aclose(procdataSizeID);
	}
    if (procfdSizeID >= 0) {
        H5Aclose(procfdSizeID);
    }
    if (procobsSizeID >= 0) {
        H5Aclose(procobsSizeID);
    }
	if (procstatDS >= 0) {
		H5Dclose(procstatDS);
	}
	if (procdataDS >= 0) {
		H5Dclose(procdataDS);
	}
    if (procfdDS >= 0) {
        H5Dclose(procfdDS);
    }
    if (procobsDS >= 0) {
        H5Dclose(procobsDS);
    }
	if (group >= 0) {
		H5Gclose(group);
	}
}

ProcHDF5IO::ProcHDF5IO(const string& _filename, ProcIOFileMode _mode, 
	unsigned int _statBlockSize, unsigned int _dataBlockSize,
    unsigned int _fdBlockSize, unsigned int _obsBlockSize): 
	
	filename(_filename),mode(_mode),statBlockSize(_statBlockSize),
	dataBlockSize(_dataBlockSize),fdBlockSize(_fdBlockSize),obsBlockSize(_obsBlockSize)
{
	file = -1;
	strType_exeBuffer = -1;
	strType_buffer = -1;
	strType_idBuffer = -1;
	type_procdata = -1;
	type_procstat = -1;
    type_procfd = -1;
    type_procobs = -1;
    override_context = false;

	if (mode == FILE_MODE_WRITE) {
    	file = H5Fopen(filename.c_str(), H5F_ACC_CREAT | H5F_ACC_RDWR, H5P_DEFAULT);
	} else if (mode == FILE_MODE_READ) {
    	file = H5Fopen(filename.c_str(), H5F_ACC_RDONLY, H5P_DEFAULT);
	}
	if (file < 0) {
		throw ProcIOException("Failed to open HDF5 file: " + filename);
	}
    initialize_types();
}
	
ProcHDF5IO::~ProcHDF5IO() {
    herr_t status;
	for (auto iter = openRefs.begin(), end = openRefs.end(); iter != end; ++iter) {
		delete (*iter).second;
	}
    if (type_procdata > 0) status = H5Tclose(type_procdata);
    if (type_procstat > 0) status = H5Tclose(type_procstat);
    if (type_procfd > 0) status = H5Tclose(type_procfd);
    if (type_procobs > 0) status = H5Tclose(type_procobs);
    if (strType_exeBuffer > 0) status = H5Tclose(strType_exeBuffer);
    if (strType_buffer > 0) status = H5Tclose(strType_buffer);
    if (file > 0) status = H5Fclose(file);
    //status = H5close();
}

void ProcHDF5IO::set_override_context(bool val) {
    override_context = val;
}

unsigned int ProcHDF5IO::read_procdata(procdata* procData, unsigned int id) {
    return read_procdata(procData, id, 1);
}

unsigned int ProcHDF5IO::read_procstat(procstat* procStat, unsigned int id) {
    return read_procstat(procStat, id, 1);
}

unsigned int ProcHDF5IO::read_procfd(procfd* procFD, unsigned int id) {
    return read_procfd(procFD, id, 1);
}

unsigned int ProcHDF5IO::read_procobs(procobs* procObs, unsigned int id) {
    return read_procobs(procObs, id, 1);
}

bool ProcHDF5IO::set_context(const string& _hostname, const string& _identifier, const string& _subidentifier) {
	size_t endPos = _hostname.find('.');
	endPos = endPos == string::npos ? _hostname.size() : endPos;
	string t_hostname(_hostname, 0, endPos);

	endPos = _identifier.find('.');
	endPos = endPos == string::npos ? _identifier.size() : endPos;
	string t_identifier(_identifier, 0, endPos);

	endPos = _subidentifier.find('.');
	endPos = endPos == string::npos ? _subidentifier.size() : endPos;
	string t_subidentifier(_subidentifier, 0, endPos);

	string key = t_hostname;
	
	auto refIter = openRefs.find(key);
	if (refIter == openRefs.end()) {
		/* need to create new hdf5Ref */
		hdf5Ref* ref = new hdf5Ref(file, type_procstat, type_procdata, type_procfd, type_procobs, t_hostname, mode, statBlockSize, dataBlockSize, fdBlockSize, obsBlockSize);
		auto added = openRefs.insert({key,ref});
		if (added.second) refIter = added.first;
	}
	if (refIter != openRefs.end()) {
		hdf5Segment = (*refIter).second;
	} else {
		hdf5Segment = nullptr;
	}

	hostname = t_hostname;
	identifier = t_identifier;
	subidentifier = t_subidentifier;
	contextSet = hdf5Segment != nullptr;
	return true;
}

void ProcHDF5IO::initialize_types() {
    herr_t status; 

    /* setup data structure types */
    strType_exeBuffer = H5Tcopy(H5T_C_S1);
    status = H5Tset_size(strType_exeBuffer, EXEBUFFER_SIZE);
    if (status < 0) {
        throw ProcIOException("Failed to set strType_exeBuffer size");
    }
    
    strType_buffer = H5Tcopy(H5T_C_S1);
    status = H5Tset_size(strType_buffer, BUFFER_SIZE);
    if (status < 0) {
        throw ProcIOException("Failed to set strType_buffer size");
    }

	strType_idBuffer = H5Tcopy(H5T_C_S1);
	status = H5Tset_size(strType_idBuffer, IDENTIFIER_SIZE);
	if (status < 0) {
		throw ProcIOException("Failed to set strType_idBuffer size");
	}

    type_procdata = H5Tcreate(H5T_COMPOUND, sizeof(procdata));
    if (type_procdata < 0) throw ProcIOException("Failed to create type_procdata");
	H5Tinsert(type_procdata, "identifier", HOFFSET(procdata, identifier), strType_idBuffer);
	H5Tinsert(type_procdata, "subidentifier", HOFFSET(procdata, subidentifier), strType_idBuffer);
    H5Tinsert(type_procdata, "execName", HOFFSET(procdata, execName), strType_exeBuffer);
    H5Tinsert(type_procdata, "cmdArgBytes", HOFFSET(procdata, cmdArgBytes), H5T_NATIVE_ULONG);
    H5Tinsert(type_procdata, "cmdArgs", HOFFSET(procdata, cmdArgs), strType_buffer);
    H5Tinsert(type_procdata, "exePath", HOFFSET(procdata, exePath), strType_buffer);
    H5Tinsert(type_procdata, "cwdPath", HOFFSET(procdata, cwdPath), strType_buffer);
    H5Tinsert(type_procdata, "recTime", HOFFSET(procdata, recTime), H5T_NATIVE_ULONG);
    H5Tinsert(type_procdata, "recTimeUSec", HOFFSET(procdata, recTimeUSec), H5T_NATIVE_ULONG);
    H5Tinsert(type_procdata, "startTime", HOFFSET(procdata, startTime), H5T_NATIVE_ULONG);
    H5Tinsert(type_procdata, "startTimeUSec", HOFFSET(procdata, startTimeUSec), H5T_NATIVE_ULONG);
    H5Tinsert(type_procdata, "pid", HOFFSET(procdata, pid), H5T_NATIVE_UINT);
    H5Tinsert(type_procdata, "ppid", HOFFSET(procdata, ppid), H5T_NATIVE_UINT);

    type_procstat = H5Tcreate(H5T_COMPOUND, sizeof(procstat));
    if (type_procstat < 0) throw ProcIOException("Failed to create type_procstat");
	H5Tinsert(type_procstat, "identifier", HOFFSET(procstat, identifier), strType_idBuffer);
	H5Tinsert(type_procstat, "subidentifier", HOFFSET(procstat, subidentifier), strType_idBuffer);
    H5Tinsert(type_procstat, "pid", HOFFSET(procstat, pid), H5T_NATIVE_UINT);
    H5Tinsert(type_procstat, "recTime", HOFFSET(procstat, recTime), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "recTimeUSec", HOFFSET(procstat, recTimeUSec), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "startTime", HOFFSET(procstat, startTime), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "startTimeUSec", HOFFSET(procstat, startTimeUSec), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "state", HOFFSET(procstat, state), H5T_NATIVE_CHAR);
    H5Tinsert(type_procstat, "ppid", HOFFSET(procstat, ppid), H5T_NATIVE_INT);
    H5Tinsert(type_procstat, "pgrp", HOFFSET(procstat, pgrp), H5T_NATIVE_INT);
    H5Tinsert(type_procstat, "session", HOFFSET(procstat, session), H5T_NATIVE_INT);
    H5Tinsert(type_procstat, "tty", HOFFSET(procstat, tty), H5T_NATIVE_INT);
    H5Tinsert(type_procstat, "tpgid", HOFFSET(procstat, tpgid), H5T_NATIVE_INT);
    H5Tinsert(type_procstat, "realUid", HOFFSET(procstat, realUid), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "effUid", HOFFSET(procstat, effUid), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "realGid", HOFFSET(procstat, realGid), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "effGid", HOFFSET(procstat, effGid), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "flags", HOFFSET(procstat, flags), H5T_NATIVE_UINT);
    H5Tinsert(type_procstat, "utime", HOFFSET(procstat, utime), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "stime", HOFFSET(procstat, stime), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "priority", HOFFSET(procstat, priority), H5T_NATIVE_LONG);
    H5Tinsert(type_procstat, "nice", HOFFSET(procstat, nice), H5T_NATIVE_LONG);
    H5Tinsert(type_procstat, "numThreads", HOFFSET(procstat, numThreads), H5T_NATIVE_LONG);
    H5Tinsert(type_procstat, "vsize", HOFFSET(procstat, vsize), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "rss", HOFFSET(procstat, rss), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "rsslim", HOFFSET(procstat, rsslim), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "signal", HOFFSET(procstat, signal), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "blocked", HOFFSET(procstat, blocked), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "sigignore", HOFFSET(procstat, sigignore), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "sigcatch", HOFFSET(procstat, sigcatch), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "rtPriority", HOFFSET(procstat, rtPriority), H5T_NATIVE_UINT);
    H5Tinsert(type_procstat, "policy", HOFFSET(procstat, policy), H5T_NATIVE_UINT);
    H5Tinsert(type_procstat, "delayacctBlkIOTicks", HOFFSET(procstat, delayacctBlkIOTicks), H5T_NATIVE_ULLONG);
    H5Tinsert(type_procstat, "guestTime", HOFFSET(procstat, guestTime), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "vmpeak", HOFFSET(procstat, vmpeak), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "rsspeak", HOFFSET(procstat, rsspeak), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "cpusAllowed", HOFFSET(procstat, cpusAllowed), H5T_NATIVE_INT);
    H5Tinsert(type_procstat, "io_rchar", HOFFSET(procstat, io_rchar), H5T_NATIVE_ULLONG);
    H5Tinsert(type_procstat, "io_wchar", HOFFSET(procstat, io_wchar), H5T_NATIVE_ULLONG);
    H5Tinsert(type_procstat, "io_syscr", HOFFSET(procstat, io_syscr), H5T_NATIVE_ULLONG);
    H5Tinsert(type_procstat, "io_syscw", HOFFSET(procstat, io_syscw), H5T_NATIVE_ULLONG);
    H5Tinsert(type_procstat, "io_readBytes", HOFFSET(procstat, io_readBytes), H5T_NATIVE_ULLONG);
    H5Tinsert(type_procstat, "io_writeBytes", HOFFSET(procstat, io_writeBytes), H5T_NATIVE_ULLONG);
    H5Tinsert(type_procstat, "io_cancelledWriteBytes", HOFFSET(procstat, io_cancelledWriteBytes), H5T_NATIVE_ULLONG);
    H5Tinsert(type_procstat, "m_size", HOFFSET(procstat, m_size), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "m_resident", HOFFSET(procstat, m_resident), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "m_share", HOFFSET(procstat, m_share), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "m_text", HOFFSET(procstat, m_text), H5T_NATIVE_ULONG);
    H5Tinsert(type_procstat, "m_data", HOFFSET(procstat, m_data), H5T_NATIVE_ULONG);

    type_procfd = H5Tcreate(H5T_COMPOUND, sizeof(procfd));
    if (type_procfd < 0) throw ProcIOException("Failed to create type_procfd");
	H5Tinsert(type_procfd, "identifier", HOFFSET(procfd, identifier), strType_idBuffer);
	H5Tinsert(type_procfd, "subidentifier", HOFFSET(procfd, subidentifier), strType_idBuffer);
    H5Tinsert(type_procfd, "pid", HOFFSET(procfd, pid), H5T_NATIVE_UINT);
    H5Tinsert(type_procfd, "ppid", HOFFSET(procfd, ppid), H5T_NATIVE_UINT);
    H5Tinsert(type_procfd, "recTime", HOFFSET(procfd, recTime), H5T_NATIVE_ULONG);
    H5Tinsert(type_procfd, "recTimeUSec", HOFFSET(procfd, recTimeUSec), H5T_NATIVE_ULONG);
    H5Tinsert(type_procfd, "startTime", HOFFSET(procfd, startTime), H5T_NATIVE_ULONG);
    H5Tinsert(type_procfd, "startTimeUSec", HOFFSET(procfd, startTimeUSec), H5T_NATIVE_ULONG);
    H5Tinsert(type_procfd, "path", HOFFSET(procfd, path), strType_buffer);
    H5Tinsert(type_procfd, "fd", HOFFSET(procfd, fd), H5T_NATIVE_UINT);
    H5Tinsert(type_procfd, "mode", HOFFSET(procfd, mode), H5T_NATIVE_UINT);

    type_procobs = H5Tcreate(H5T_COMPOUND, sizeof(procobs));
    if (type_procobs < 0) throw ProcIOException("Failed to create type_procobs");
	H5Tinsert(type_procobs, "identifier", HOFFSET(procobs, identifier), strType_idBuffer);
	H5Tinsert(type_procobs, "subidentifier", HOFFSET(procobs, subidentifier), strType_idBuffer);
    H5Tinsert(type_procobs, "pid", HOFFSET(procobs, pid), H5T_NATIVE_UINT);
    H5Tinsert(type_procobs, "recTime", HOFFSET(procobs, recTime), H5T_NATIVE_ULONG);
    H5Tinsert(type_procobs, "recTimeUSec", HOFFSET(procobs, recTimeUSec), H5T_NATIVE_ULONG);
    H5Tinsert(type_procobs, "startTime", HOFFSET(procobs, startTime), H5T_NATIVE_ULONG);
    H5Tinsert(type_procobs, "startTimeUSec", HOFFSET(procobs, startTimeUSec), H5T_NATIVE_ULONG);
}

unsigned int ProcHDF5IO::write_procstat(procstat* start_pointer, unsigned int start_id, int count) {
    if (!override_context) {
	    for (int i = 0; i < count; i++) {
		    snprintf(start_pointer[i].identifier, IDENTIFIER_SIZE, "%s", identifier.c_str());
		    snprintf(start_pointer[i].subidentifier, IDENTIFIER_SIZE, "%s", subidentifier.c_str());
	    }
    }
    return write_dataset(TYPE_PROCSTAT, type_procstat, (void*) start_pointer, start_id, count, statBlockSize);
}

unsigned int ProcHDF5IO::write_procdata(procdata* start_pointer, unsigned int start_id, int count) {
    if (!override_context) {
	    for (int i = 0; i < count; i++) {
		    snprintf(start_pointer[i].identifier, IDENTIFIER_SIZE, "%s", identifier.c_str());
		    snprintf(start_pointer[i].subidentifier, IDENTIFIER_SIZE, "%s", subidentifier.c_str());
	    }
    }
    return write_dataset(TYPE_PROCDATA, type_procdata, (void*) start_pointer, start_id, count, dataBlockSize);
}

unsigned int ProcHDF5IO::write_procfd(procfd* start_pointer, unsigned int start_id, int count) {
    if (!override_context) {
	    for (int i = 0; i < count; i++) {
		    snprintf(start_pointer[i].identifier, IDENTIFIER_SIZE, "%s", identifier.c_str());
		    snprintf(start_pointer[i].subidentifier, IDENTIFIER_SIZE, "%s", subidentifier.c_str());
	    }
    }
    return write_dataset(TYPE_PROCFD, type_procfd, (void*) start_pointer, start_id, count, fdBlockSize);
}

unsigned int ProcHDF5IO::write_procobs(procobs* start_pointer, unsigned int start_id, int count) {
    if (!override_context) {
	    for (int i = 0; i < count; i++) {
		    snprintf(start_pointer[i].identifier, IDENTIFIER_SIZE, "%s", identifier.c_str());
		    snprintf(start_pointer[i].subidentifier, IDENTIFIER_SIZE, "%s", subidentifier.c_str());
	    }
    }
    return write_dataset(TYPE_PROCOBS, type_procobs, (void*) start_pointer, start_id, count, obsBlockSize);
}

unsigned int ProcHDF5IO::read_procstat(procstat* start_pointer, unsigned int start_id, unsigned int count) {
    return read_dataset(TYPE_PROCSTAT, type_procstat, (void*) start_pointer, start_id, count);
}

unsigned int ProcHDF5IO::read_procdata(procdata* start_pointer, unsigned int start_id, unsigned int count) {
    return read_dataset(TYPE_PROCDATA, type_procdata, (void*) start_pointer, start_id, count);
}

unsigned int ProcHDF5IO::read_procfd(procfd* start_pointer, unsigned int start_id, unsigned int count) {
    return read_dataset(TYPE_PROCFD, type_procfd, (void*) start_pointer, start_id, count);
}

unsigned int ProcHDF5IO::read_procobs(procobs* start_pointer, unsigned int start_id, unsigned int count) {
    return read_dataset(TYPE_PROCOBS, type_procobs, (void*) start_pointer, start_id, count);
}

unsigned int ProcHDF5IO::read_dataset(ProcRecordType recordType, hid_t type, void* start_pointer, unsigned int start_id, unsigned int count) {
    if (hdf5Segment == nullptr) {
        return 0;
    }
	hdf5Segment->lastUpdate = time(NULL);

    hsize_t targetRecords = 0;
    hsize_t localRecords = count;
    hsize_t remoteStart = start_id > 0 ? start_id - 1 : 0;
    hsize_t localStart = 0;
    hsize_t nRecords = 0;

	hid_t ds = -1;
    if (recordType == TYPE_PROCSTAT) {
       ds = hdf5Segment->procstatDS;
    } else if (recordType == TYPE_PROCDATA) {
       ds = hdf5Segment->procdataDS;
    } else if (recordType == TYPE_PROCFD) {
       ds = hdf5Segment->procfdDS;
    } else if (recordType == TYPE_PROCOBS) {
       ds = hdf5Segment->procobsDS;
    }
    hid_t dataspace = H5Dget_space(ds);
    hid_t memspace = H5Screate_simple(1, &targetRecords, NULL);
    herr_t status = 0;

    //int rank = H5Sget_simple_extent_ndims(dataspace);
    status = H5Sget_simple_extent_dims(dataspace, &nRecords, NULL);
    if (remoteStart < nRecords) {
        targetRecords = count < (nRecords - remoteStart) ? count : (nRecords - remoteStart);

        status = H5Sselect_hyperslab(dataspace, H5S_SELECT_SET, &remoteStart, H5P_DEFAULT, &targetRecords, H5P_DEFAULT);
        status = H5Sselect_hyperslab(memspace, H5S_SELECT_SET, &localStart, H5P_DEFAULT, &localRecords, H5P_DEFAULT);
        status = H5Dread(ds, type, H5S_ALL, dataspace, H5P_DEFAULT, start_pointer);
    }

    H5Sclose(dataspace);
    H5Sclose(memspace);

    return (int) targetRecords;
}

unsigned int ProcHDF5IO::write_dataset(ProcRecordType recordType, hid_t type, void* start_pointer, unsigned int start_id, int count, int chunkSize) {
    hsize_t rank = 1;
    hsize_t maxRecords = 0;
	hsize_t startRecord = 0;
    hsize_t targetRecords = 0;
    hsize_t newRecords = count;
	unsigned int old_nRecords = 0;

	bool append = start_id == 0;
	startRecord = start_id > 0 ? start_id - 1 : 0;

	hid_t ds = -1;
    hid_t size_attribute = -1;
    unsigned int *nRecords = NULL;
    if (recordType == TYPE_PROCSTAT) {
       ds = hdf5Segment->procstatDS;
       size_attribute = hdf5Segment->procstatSizeID;
       nRecords = &(hdf5Segment->procstatSize);
    } else if (recordType == TYPE_PROCDATA) {
       ds = hdf5Segment->procdataDS;
       size_attribute = hdf5Segment->procdataSizeID;
       nRecords = &(hdf5Segment->procdataSize);
    } else if (recordType == TYPE_PROCFD) {
       ds = hdf5Segment->procfdDS;
       size_attribute = hdf5Segment->procfdSizeID;
       nRecords = &(hdf5Segment->procfdSize);
    } else if (recordType == TYPE_PROCOBS) {
       ds = hdf5Segment->procobsDS;
       size_attribute = hdf5Segment->procobsSizeID;
       nRecords = &(hdf5Segment->procobsSize);
    }
    hid_t filespace;
    herr_t status;

	if (nRecords == NULL) {
		throw ProcIOException("Couldn't identify dataset size records (recordType " + to_string((int)recordType) + ") invalid");
	}

	if (hdf5Segment == nullptr) return 0;
	hdf5Segment->lastUpdate = time(NULL);
    hid_t dataspace = H5Dget_space(ds);

    status = H5Sget_simple_extent_dims(dataspace, &maxRecords, NULL);

	if (append) startRecord = *nRecords;
    targetRecords = startRecord + count > maxRecords ? startRecord + count : maxRecords;

    status = H5Dset_extent(ds, &targetRecords);
    filespace = H5Dget_space(ds);

    status = H5Sselect_hyperslab(filespace, H5S_SELECT_SET, &startRecord, NULL, &newRecords, NULL);
    dataspace = H5Screate_simple(rank, &newRecords, NULL);

    H5Dwrite(ds, type, dataspace, filespace, H5P_DEFAULT, start_pointer);

	old_nRecords = *nRecords;
	*nRecords = startRecord + count > *nRecords ? startRecord + count : *nRecords;

	H5Awrite(size_attribute, H5T_NATIVE_UINT, nRecords);

	start_id = startRecord+1;

    H5Sclose(filespace);
    H5Sclose(dataspace);
    return start_id;
}

void ProcHDF5IO::flush() {
	H5Fflush(file, H5F_SCOPE_GLOBAL);
}

void ProcHDF5IO::trim_segments(time_t cutoff) {
	for (auto iter = openRefs.begin(), end = openRefs.end(); iter != end; ) {
		hdf5Ref* ref = iter->second;
		if (ref->lastUpdate < cutoff) {
			delete ref;
			openRefs.erase(iter++);
		} else {
			++iter;
		}
	}
}
	
unsigned int ProcHDF5IO::get_nprocstat() {
	if (hdf5Segment == nullptr) return 0;
	return hdf5Segment->procstatSize;
}

unsigned int ProcHDF5IO::get_nprocdata() {
	if (hdf5Segment == nullptr) return 0;
	return hdf5Segment->procdataSize;
}

unsigned int ProcHDF5IO::get_nprocfd() {
	if (hdf5Segment == nullptr) return 0;
	return hdf5Segment->procfdSize;
}

unsigned int ProcHDF5IO::get_nprocobs() {
	if (hdf5Segment == nullptr) return 0;
	return hdf5Segment->procobsSize;
}

herr_t op_func(hid_t loc_id, const char *name, const H5L_info_t *info, void *op_data) {
    vector<string> *hosts = (vector<string> *) op_data;
    hosts->push_back(string(name));
    return 0;
}

bool ProcHDF5IO::get_hosts(vector<string>& hosts) {
    herr_t status;
    status = H5Literate(this->file, H5_INDEX_NAME, H5_ITER_NATIVE, NULL, op_func, (void*) &hosts);
    return status > 0;
}

#endif

int ProcTextIO::fill_buffer() {
    if (sPtr != NULL) {
        bcopy(buffer, sPtr, sizeof(char)*(ptr-sPtr));
        ptr = buffer + (ptr - sPtr);
        sPtr = buffer;
    } else {
        sPtr = buffer;
        ptr = buffer;
    }
    int readBytes = fread(ptr, sizeof(char), TEXT_BUFFER_SIZE - (ptr-sPtr), filePtr);
    if (readBytes == 0) {
        return 0;
    }
    if (sPtr != NULL) {
        ptr = buffer + (ptr - sPtr);
        sPtr = buffer;
    }
    ePtr = ptr + readBytes;
    return readBytes;
}


ProcRecordType ProcTextIO::read_stream_record(procdata* procData, procstat* procStat, procfd* procFD) {
    while (true) {
        if (sPtr == NULL || ptr == ePtr) {
            int bytes = fill_buffer();
            if (bytes == 0) return TYPE_INVALID;
        }
        if (*ptr == ',') {
            *ptr = 0;
            if (strcmp(sPtr, "procstat") == 0) {
                sPtr = ptr + 1;
                return read_procstat(procStat) ? TYPE_PROCSTAT : TYPE_INVALID;
            } else if (strcmp(sPtr, "procdata") == 0) {
                sPtr = ptr + 1;
                return read_procdata(procData) ? TYPE_PROCDATA : TYPE_INVALID;
            } else if (strcmp(sPtr, "procfd") == 0) {
                sPtr = ptr + 1;
                return read_procfd(procFD) ? TYPE_PROCFD : TYPE_INVALID;
            }
        }
        ptr++;
    }
}

bool ProcTextIO::read_procstat(procstat* procStat) {
    int pos = 0;
    int readBytes = -1;
    bool done = false;
    while (!done) {
        if (sPtr == NULL || ptr == ePtr) {
            int bytes = fill_buffer();
            if (bytes == 0) return true;
        }
        if ((*ptr == ',' || *ptr == '\n') && (readBytes < 0 || readBytes == (ptr-sPtr))) {
            if (*ptr == '\n') done = true;
            *ptr = 0;
            switch (pos) {
                case 0: procStat->pid = atoi(sPtr); break;
                case 1: procStat->recTime = strtoul(sPtr, &ptr, 10); break;
                case 2: procStat->recTimeUSec = strtoul(sPtr, &ptr, 10); break;
                case 3: procStat->startTime = strtoul(sPtr, &ptr, 10); break;
                case 4: procStat->startTimeUSec = strtoul(sPtr, &ptr, 10); break;
                case 5: procStat->state = *sPtr; break;
                case 6: procStat->ppid = atoi(sPtr); break;
                case 7: procStat->pgrp = atoi(sPtr); break;
                case 8: procStat->session = atoi(sPtr); break;
                case 9: procStat->tty = atoi(sPtr); break;
                case 10: procStat->tpgid = atoi(sPtr); break;
                case 11: procStat->flags = (unsigned int) strtoul(sPtr, &ptr, 10); break;
                case 12: procStat->utime = strtoul(sPtr, &ptr, 10); break;
                case 13: procStat->stime = strtoul(sPtr, &ptr, 10); break;
                case 14: procStat->priority = strtol(sPtr, &ptr, 10); break;
                case 15: procStat->nice = strtol(sPtr, &ptr, 10); break;
                case 16: procStat->numThreads = strtol(sPtr, &ptr, 10); break;
                case 17: procStat->vsize = strtoul(sPtr, &ptr, 10); break;
                case 18: procStat->rss = strtoul(sPtr, &ptr, 10); break;
                case 19: procStat->rsslim = strtoul(sPtr, &ptr, 10); break;
                case 20: procStat->vmpeak = strtoul(sPtr, &ptr, 10); break;
                case 21: procStat->rsspeak = strtoul(sPtr, &ptr, 10); break;
                case 22: procStat->signal = strtoul(sPtr, &ptr, 10); break;
                case 23: procStat->blocked = strtoul(sPtr, &ptr, 10); break;
                case 24: procStat->sigignore = strtoul(sPtr, &ptr, 10); break;
                case 25: procStat->sigcatch = strtoul(sPtr, &ptr, 10); break;
                case 26: procStat->cpusAllowed = atoi(sPtr); break;
                case 27: procStat->rtPriority = (unsigned int) strtoul(sPtr, &ptr, 10); break;
                case 28: procStat->policy = (unsigned int) strtoul(sPtr, &ptr, 10); break;
                case 29: procStat->guestTime = strtoul(sPtr, &ptr, 10); break;
                case 30: procStat->delayacctBlkIOTicks = strtoull(sPtr, &ptr, 10); break;
                case 31: procStat->io_rchar = strtoull(sPtr, &ptr, 10); break;
                case 32: procStat->io_wchar = strtoull(sPtr, &ptr, 10); break;
                case 33: procStat->io_syscr = strtoull(sPtr, &ptr, 10); break;
                case 34: procStat->io_syscw = strtoull(sPtr, &ptr, 10); break;
                case 35: procStat->io_readBytes = strtoull(sPtr, &ptr, 10); break;
                case 36: procStat->io_writeBytes = strtoull(sPtr, &ptr, 10); break;
                case 37: procStat->io_cancelledWriteBytes = strtoull(sPtr, &ptr, 10); break;
                case 38: procStat->m_size = strtoul(sPtr, &ptr, 10); break;
                case 39: procStat->m_resident = strtoul(sPtr, &ptr, 10); break;
                case 40: procStat->m_share = strtoul(sPtr, &ptr, 10); break;
                case 41: procStat->m_text = strtoul(sPtr, &ptr, 10); break;
                case 42: procStat->m_data = strtoul(sPtr, &ptr, 10); break;
                case 43: procStat->realUid = strtoul(sPtr, &ptr, 10); break;
                case 44: procStat->effUid = strtoul(sPtr, &ptr, 10); break;
                case 45: procStat->realGid = strtoul(sPtr, &ptr, 10); break;
                case 46: procStat->effGid = strtoul(sPtr, &ptr, 10); done = true; break;
            }
            pos++;
            sPtr = ptr + 1;
        }
        ptr++;
    }
    return true;
}

bool ProcTextIO::read_procdata(procdata* procData) {
    int pos = 0;
    int readBytes = -1;
    bool done = false;
    procData->pid = 0;
    while (!done) {
        if (sPtr == NULL || ptr == ePtr) {
            int bytes = fill_buffer();
            if (bytes == 0) break;
        }
        if ((*ptr == ',' || *ptr == '\n') && (readBytes < 0 || readBytes == (ptr-sPtr))) {
            if (*ptr == '\n') done = true;
            *ptr = 0;
            switch (pos) {
                case 0: procData->pid = atoi(sPtr); break;
                case 1: procData->ppid = atoi(sPtr); break;
                case 2: procData->recTime = strtoul(sPtr, &ptr, 10); break;
                case 3: procData->recTimeUSec = strtoul(sPtr, &ptr, 10); break;
                case 4: procData->startTime = strtoul(sPtr, &ptr, 10); break;
                case 5: procData->startTimeUSec = strtoul(sPtr, &ptr, 10); break;
                case 6: readBytes = atoi(sPtr); break;
                case 7: memcpy(procData->execName, sPtr, sizeof(char)*readBytes); readBytes = -1; break;
                case 8: readBytes = atoi(sPtr); break;
                case 9: memcpy(procData->cmdArgs, sPtr, sizeof(char)*readBytes); procData->cmdArgBytes = readBytes; readBytes = -1; break;
                case 10: readBytes = atoi(sPtr); break;
                case 11: memcpy(procData->exePath, sPtr, sizeof(char)*readBytes); readBytes = -1; break;
                case 12: readBytes = atoi(sPtr); break;
                case 13: memcpy(procData->cwdPath, sPtr, sizeof(char)*readBytes); readBytes = -1; break;
            }
            pos++;
            sPtr = ptr + 1;
        }
        ptr++;
    }
    return procData->pid != 0;
}

bool ProcTextIO::read_procfd(procfd* procFD) {
    int pos = 0;
    int readBytes = -1;
    bool done = false;
    procFD->pid = 0;
    while (!done) {
        if (sPtr == NULL || ptr == ePtr) {
            int bytes = fill_buffer();
            if (bytes == 0) break;
        }
        if ((*ptr == ',' || *ptr == '\n') && (readBytes < 0 || readBytes == (ptr-sPtr))) {
            if (*ptr == '\n') done = true;
            *ptr = 0;
            switch (pos) {
                case 0: procFD->pid = atoi(sPtr); break;
                case 1: procFD->ppid = atoi(sPtr); break;
                case 2: procFD->recTime = strtoul(sPtr, &ptr, 10); break;
                case 3: procFD->recTimeUSec = strtoul(sPtr, &ptr, 10); break;
                case 4: procFD->startTime = strtoul(sPtr, &ptr, 10); break;
                case 5: procFD->startTimeUSec = strtoul(sPtr, &ptr, 10); break;
                case 6: readBytes = atoi(sPtr); break;
                case 7:
					memcpy(procFD->path, sPtr, sizeof(char)*readBytes);
					procFD->path[readBytes < BUFFER_SIZE ? readBytes : BUFFER_SIZE] = 0;
					readBytes = -1;
					break;
                case 8: procFD->fd = atoi(sPtr); break;
                case 9: procFD->mode = (unsigned int) atoi(sPtr); break;
            }
            pos++;
            sPtr = ptr + 1;
        }
        ptr++;
    }
    return procFD->pid != 0;
}

unsigned int ProcTextIO::write_procdata(procdata* start_ptr, int cnt) {
    unsigned int nBytes = 0;
    for (int i = 0; i < cnt; i++) {
        procdata* procData = &(start_ptr[i]);
        nBytes += fprintf(filePtr, "procdata,%s,%s,%s", hostname.c_str(), identifier.c_str(), subidentifier.c_str());
        nBytes += fprintf(filePtr, ",%d,%d,%lu,%lu,%lu,%lu",procData->pid,procData->ppid,procData->recTime,procData->recTimeUSec,procData->startTime,procData->startTimeUSec);
        nBytes += fprintf(filePtr, ",%lu,%s", strlen(procData->execName), procData->execName);
        nBytes += fprintf(filePtr, ",%lu,%s", procData->cmdArgBytes, procData->cmdArgs);
        nBytes += fprintf(filePtr, ",%lu,%s", strlen(procData->exePath), procData->exePath);
        nBytes += fprintf(filePtr, ",%lu,%s\n", strlen(procData->cwdPath), procData->cwdPath);
    }
    return nBytes;
}

unsigned int ProcTextIO::write_procstat(procstat* start_ptr, int cnt) {
    unsigned int nBytes = 0;
    for (int i = 0; i < cnt; i++) {
        procstat* procStat = &(start_ptr[i]);
        nBytes += fprintf(filePtr, "procstat,%s,%s,%s", hostname.c_str(), identifier.c_str(), subidentifier.c_str());
        nBytes += fprintf(filePtr, ",%d,%lu,%lu,%lu,%lu",procStat->pid,procStat->recTime,procStat->recTimeUSec,procStat->startTime,procStat->startTimeUSec);
        nBytes += fprintf(filePtr, ",%c,%d,%d,%d",procStat->state,procStat->ppid,procStat->pgrp,procStat->session);
        nBytes += fprintf(filePtr, ",%d,%d,%u,%lu,%lu",procStat->tty,procStat->tpgid,procStat->flags,procStat->utime,procStat->stime);
        nBytes += fprintf(filePtr, ",%ld,%ld,%ld,%lu,%lu",procStat->priority,procStat->nice,procStat->numThreads,procStat->vsize,procStat->rss);
        nBytes += fprintf(filePtr, ",%lu,%lu,%lu,%lu,%lu",procStat->rsslim,procStat->vmpeak,procStat->rsspeak,procStat->signal,procStat->blocked);
        nBytes += fprintf(filePtr, ",%lu,%lu,%d,%u,%u",procStat->sigignore,procStat->sigcatch,procStat->cpusAllowed,procStat->rtPriority,procStat->policy);
        nBytes += fprintf(filePtr, ",%lu,%llu,%llu,%llu,%llu",procStat->guestTime,procStat->delayacctBlkIOTicks,procStat->io_rchar,procStat->io_wchar,procStat->io_syscr);
        nBytes += fprintf(filePtr, ",%llu,%llu,%llu,%llu,%lu",procStat->io_syscw,procStat->io_readBytes,procStat->io_writeBytes,procStat->io_cancelledWriteBytes, procStat->m_size);
        nBytes += fprintf(filePtr, ",%lu,%lu,%lu,%lu,%lu",procStat->m_resident,procStat->m_share,procStat->m_text,procStat->m_data, procStat->realUid); 
        nBytes += fprintf(filePtr, ",%lu,%lu,%lu\n",procStat->effUid, procStat->realGid, procStat->effGid);
    }
    return nBytes;
}

unsigned int ProcTextIO::write_procfd(procfd* start_ptr, int cnt) {
    unsigned int nBytes = 0;
    for (int i = 0; i < cnt; i++) {
        procfd* procFD = &(start_ptr[i]);
        nBytes += fprintf(filePtr, "procfd,%s,%s,%s", hostname.c_str(), identifier.c_str(), subidentifier.c_str());
        nBytes += fprintf(filePtr, ",%d,%d,%lu,%lu,%lu,%lu",procFD->pid,procFD->ppid,procFD->recTime,procFD->recTimeUSec,procFD->startTime,procFD->startTimeUSec);
        nBytes += fprintf(filePtr, ",%lu,%s", strlen(procFD->path), procFD->path);
        nBytes += fprintf(filePtr, ",%d,%u\n", procFD->fd, procFD->mode);
    }
    return nBytes;
}

bool ProcTextIO::set_context(const string& _hostname, const string& _identifier, const string& _subidentifier) {
	size_t endPos = _hostname.find('.');
	endPos = endPos == string::npos ? _hostname.size() : endPos;
	hostname.assign(_hostname, 0, endPos);

	endPos = _identifier.find('.');
	endPos = endPos == string::npos ? _identifier.size() : endPos;
	identifier.assign(_identifier, 0, endPos);

	endPos = _subidentifier.find('.');
	endPos = endPos == string::npos ? _subidentifier.size() : endPos;
	subidentifier.assign(_subidentifier, 0, endPos);
	return true;
}

ProcTextIO::ProcTextIO(const string& _filename, ProcIOFileMode _mode) {
	filename = _filename;
	mode = _mode;

	filePtr = NULL;

	if (mode == FILE_MODE_WRITE) {
		filePtr = fopen(filename.c_str(), "a");
	} else if (mode == FILE_MODE_READ) {
		filePtr = fopen(filename.c_str(), "r");
	}
	if (filePtr == NULL) {
		throw ProcIOException("Couldn't open text file: " + filename);
	}
}

ProcTextIO::~ProcTextIO() {
	if (filePtr != NULL) {
		fclose(filePtr);
	}
}
