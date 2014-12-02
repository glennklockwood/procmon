#include "ProcIO2.hh"
#include "ProcData.hh"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <iostream>
#include <vector>
#include <string>

using namespace std;

namespace pmio2 {

#ifdef USE_AMQP
    /*
AmqpIo::AmqpIo(const string& _mqServer, int _port, const string& _mqVHost, 
	const string& _username, const string& _password,
    const string& _exchangeName, const int _frameSize, const IoMode _mode):

	mqServer(_mqServer),port(_port),mqVHost(_mqVHost),username(_username),
    password(_password),exchangeName(_exchangeName),frameSize(_frameSize),
    mode(_mode)

{
	connected = false;
	amqpError = false;
    memset(&conn, 0, sizeof(amqp_connection_state_t));
    try {
	    _amqp_open();
    } catch (const IoException &e) {
        cerr << "Caught (and ignored) exception: " << e.what() << endl;
    }
}

bool AmqpIo::addDataset(shared_ptr<Dataset> ptr, const Context &context, const string &dsName) {
    IoMethod::addDataset(ptr, context, dsName);
    currentDatasets[dsName] = ptr;
    return true;
}

bool AmqpIo::_amqp_open() {
	int istatus = 0;
	conn = amqp_new_connection();
	socket = amqp_tcp_socket_new(conn);
	istatus = amqp_socket_open(socket, mqServer.c_str(), port);
	if (istatus != 0) {
        char errBuffer[1024];
        snprintf(errBuffer, 1024, "Failed AMQP connection to %s:%d",
            mqServer.c_str(), port
        );
        throw IoException(errBuffer);
	}
	//amqp_set_socket(conn, socket);
	_amqp_eval_status(
        amqp_login(
            conn, mqVHost.c_str(), 0, frameSize, 0, AMQP_SASL_METHOD_PLAIN,
            username.c_str(), password.c_str()
        )
    );
	if (amqpError) {
        char errBuffer[1024];
        snprintf(errBuffer, 1024, "Failed AMQP login to %s:%d as %s; Error: %s",
            mqServer.c_str(), port, username.c_str(), amqpErrorMessage.c_str()
        );
        throw IoException(errBuffer);
	}

	amqp_channel_open(conn, 1);
	_amqp_eval_status(amqp_get_rpc_reply(conn));
	if (amqpError) {
        char errBuffer[1024];
        snprintf(errBuffer, 1024, "Failed AMQP open channel on %s:%d; Error: %s",
            mqServer.c_str(), port, amqpErrorMessage.c_str()
        );
        throw IoException(errBuffer);
	}

	amqp_exchange_declare(conn, 1, amqp_cstring_bytes(exchangeName.c_str()), amqp_cstring_bytes("topic"), 0, 0, amqp_empty_table);
	_amqp_eval_status(amqp_get_rpc_reply(conn));
	if (amqpError) {
		throw IoException("Failed to declare exchange: " + exchangeName + "; Error: " + amqpErrorMessage);
	}
	connected = true;
	return connected;
}

bool AmqpIo::_amqp_close(bool throw_errors) {
	if (connected) {
		_amqp_eval_status(amqp_channel_close(conn, 1, AMQP_REPLY_SUCCESS));
		if (amqpError) {
            string message = "AMQP channel close failed: " + amqpErrorMessage;
            if (throw_errors) {
                throw IoException(message);
            } else {
                cerr << message << endl;
            }
		}
		_amqp_eval_status(amqp_connection_close(conn, AMQP_REPLY_SUCCESS));
		if (amqpError) {
            string message = "AMQP connection close failed: " + amqpErrorMessage;
            if (throw_errors) {
                throw IoException(message);
            } else {
                cerr << message << endl;
            }
		}
		amqp_destroy_connection(conn);
        connected = false;
	}
    return !connected;
}

bool AmqpIo::_amqp_eval_status(amqp_rpc_reply_t status) {
    char messageBuffer[1024];
	amqpError = false;
	switch (status.reply_type) {
		case AMQP_RESPONSE_NORMAL:
			return false;
			break;
		case AMQP_RESPONSE_NONE:
            snprintf(messageBuffer, 1024, "missing RPC reply type (ReplyVal: %u)", (unsigned int) status.reply_type);
            amqpErrorMessage = string(messageBuffer);
			break;
		case AMQP_RESPONSE_LIBRARY_EXCEPTION:
            snprintf(messageBuffer, 1024, "%s (ReplyVal: %u)", amqp_error_string2(status.library_error), (unsigned int) status.reply_type);
            amqpErrorMessage = string(messageBuffer);
			break;
		case AMQP_RESPONSE_SERVER_EXCEPTION: {
			switch (status.reply.id) {
				case AMQP_CONNECTION_CLOSE_METHOD: {
					amqp_connection_close_t *m = (amqp_connection_close_t *) status.reply.decoded;
                    snprintf(messageBuffer, 1024, "server connection error %d, message: ", (int) m->reply_code);
					amqpErrorMessage = string(messageBuffer) + string(reinterpret_cast<const char *>(m->reply_text.bytes), (int) m->reply_text.len);
					break;
				}
				case AMQP_CHANNEL_CLOSE_METHOD: {
					amqp_channel_close_t *m = (amqp_channel_close_t *) status.reply.decoded;
                    snprintf(messageBuffer, 1024, "server channel error %d, message: ", (int) m->reply_code);
					amqpErrorMessage = string(messageBuffer) + string(reinterpret_cast<const char *>(m->reply_text.bytes), (int) m->reply_text.len);
					break;
				}
				default:
                    snprintf(messageBuffer, 1024, "unknown server error, method id %d", (int)status.reply.id);
					amqpErrorMessage = string(messageBuffer);
					break;
			}
			break;
		}
	}
	amqpError = true;
	return amqpError;
}

AmqpIo::~AmqpIo() {
    _amqp_close(false);
}

//
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
                } else if (frameMessageType == "netstat") {
                    required_size = sizeof(netstat) * nRecords;
                    recType = TYPE_NETSTAT;
                }
                size_t alloc_size = (size_t) (required_size * 1.2);
                if (*data == NULL || *pool_size < required_size) {
                    *data = realloc(*data, alloc_size);
                    *pool_size = alloc_size;
                }
				if (*data == NULL) {
                    char errBuffer[1024];
                    snprintf(errBuffer, 1024, "failed to allocate memory for %d %s records", nRecords, frameMessageType.c_str());
                    throw ProcIOException(errBuffer);
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
                } else if (recType == TYPE_NETSTAT) {
                    _read_netstat((netstat*) *data, nRecords, sPtr, body_received - (sPtr - message_buffer));
                }
			}
        }
        break;
    }
	return recType;
}



AmqpDataset::AmqpDataset(shared_ptr<AmqpIo> _amqp, const Context &context,
        const string &dsName
AmqpProcstat::AmqpProcstat(shared_ptr<AmqpIo> _amqp, const Context &context):
    Dataset(_amqp, context, "procstat"),
    amqp(_amqp)
{
}

size_t AmqpProcstat::read(vector<procstat> &buffer, int nRecords, char *buffer, int nBytes) {
	char* ptr = buffer;
	char* ePtr = buffer + nBytes;
	char* sPtr = ptr;

    buffer.resize(nRecords);
    if (nRecords == 0) {
        return 0;
    }

    int pos = 0;
	int idx = 0;
	bool done = false;
	procstat* procStat = &(buffer[0]);
    while (idx < nRecords && ptr < ePtr) {
        if (*ptr == ',' || *ptr == '\n') {
			if (done) {
				procStat = &(buffer[++idx]);
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
    return idx;
}

AmqpProcdata::AmqpProcdata(shared_ptr<AmqpIo> _amqp, const Context &context):
    Dataset(_amqp, context, "procdata"), amqp(_amqp)
{
}

size_t AmqpProcData::read(vector<procdata> &buffer, int nRecords, char *buffer, int nBytes) {

	char* ptr = buffer;
	char* ePtr = buffer + nBytes;
	char* sPtr = ptr;

    buffer.resize(nRecords);
    if (nRecords == 0) {
        return 0;
    }

    int pos = 0;
	int idx = 0;
	int readBytes = -1;
	bool done = false;
	procdata* procData = &(buffer[0]);
    while (idx < nRecords && ptr < ePtr) {
        if ((*ptr == ',' || *ptr == '\n') && (readBytes < 0 || readBytes == (ptr-sPtr))) {
			if (done) {
				procData = &(buffer[++idx]);
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
    return idx;
}

size_t AmqpProcfd(vector<procfd> buffer, int nRecords, char *buffer, int nBytes) {
	char* ptr = buffer;
	char* ePtr = buffer + nBytes;
	char* sPtr = ptr;

    buffer.resize(nRecords);
    if (nRecords == 0) {
        return 0;
    }

    int pos = 0;
	int idx = 0;
	int readBytes = -1;
	bool done = false;
	procfd* procFD = &(buffer[0]);
    while (idx < nRecords && ptr < ePtr) {
        if ((*ptr == ',' || *ptr == '\n') && (readBytes < 0 || readBytes == (ptr-sPtr))) {
			if (done) {
				procFD = &(buffer[++idx]);
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
    return idx;
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
                // deconstruct existing connection (if it exists), and rebuild 
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

unsigned int ProcAMQPIO::write_netstat(netstat* start_ptr, int count) {
    unsigned int nBytes = 0;
    char* ptr = buffer;
    ptr += snprintf(ptr, AMQP_BUFFER_SIZE, "nRecords=%d\n", count);
    for (int i = 0; i < count; i++) {
        netstat* net = &(start_ptr[i]);
        ptr += snprintf(ptr, AMQP_BUFFER_SIZE - (ptr - buffer),
            "%lu,%lu,%lu,%u,%lu,%u,%u,%lu,%lu,%u,%lu,%lu,%lu,%lu,%lu,%u,%d\n",
            net->recTime, net->recTimeUSec, net->local_address,
            net->local_port, net->remote_address, net->remote_port,
            net->state, net->tx_queue, net->rx_queue, net->tr,
            net->ticks_expire, net->retransmit, net->uid,
            net->timeout, net->inode, net->refCount, net->type
        );
    }
	*ptr = 0;
    nBytes = ptr - buffer;
    if (nBytes == AMQP_BUFFER_SIZE) {
        fprintf(stderr, "WARNING: sending full buffer -- data will be truncated\n");
    }

	amqp_bytes_t message;
	message.bytes = buffer;
	message.len = nBytes;
    return _send_message("netstat", message) ? nBytes : 0;
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
        char errBuffer[1024];
        snprintf(errBuffer, 1024, "Failed AMQP queue declare on %s:%d, exchange %s; Error: %s", mqServer.c_str(), port, exchangeName.c_str(), amqpErrorMessage.c_str());
		throw ProcIOException(errBuffer);
	}

	amqp_bytes_t queue = amqp_bytes_malloc_dup(queue_reply->queue);
    if (queue.bytes == NULL) {
        throw ProcIOException("Failed AMQP queue declare: out of memory!");
    }

    string bindKey = hostname + "." + identifier + "." + subidentifier + ".*";
    amqp_queue_bind(conn, 1, queue, amqp_cstring_bytes(exchangeName.c_str()), amqp_cstring_bytes(bindKey.c_str()), amqp_empty_table);
    _amqp_eval_status(amqp_get_rpc_reply(conn));
    if (amqpError) {
        char errBuffer[1024];
        snprintf(errBuffer, 1024, "Failed AMQP queue bind on %s:%d, exchange %s; Error: %s", mqServer.c_str(), port, exchangeName.c_str(), amqpErrorMessage.c_str());
        throw ProcIOException(errBuffer);
    }
    amqp_basic_consume(conn, 1, queue, amqp_empty_bytes, 0, 1, 0, amqp_empty_table);
    _amqp_eval_status(amqp_get_rpc_reply(conn));
    if (amqpError) {
        char errBuffer[1024];
        snprintf(errBuffer, 1024, "Failed AMQP queue bind on %s:%d, exchange %s; Error: %s", mqServer.c_str(), port, exchangeName.c_str(), amqpErrorMessage.c_str());
        throw ProcIOException(errBuffer);
    }
	amqp_bytes_free(queue);
    return true;
}
*/
#endif

#ifdef USE_HDF5
Hdf5Group::Hdf5Group(Hdf5Io &h5File, const string &groupName) {
    if (H5Lexists(h5File.file, groupName.c_str(), H5P_DEFAULT) == 1) {
        group = H5Gopen2(h5File.file, groupName.c_str(), H5P_DEFAULT);
        set = true;
    } else if (h5File.writable()) {
        group = H5Gcreate(h5File.file, groupName.c_str(), H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
        set = true;
    }
    if (group < 0) {
        throw IoException("Failed to access group: " + groupName);
    }
}

Hdf5Io::Hdf5Io(const string& _filename, IoMode _mode):
    IoMethod(), filename(_filename), mode(_mode)
{
	file = -1;
	strType_exeBuffer = -1;
	strType_buffer = -1;
	strType_idBuffer = -1;
    strType_variable = -1;

	if (mode == IoMode::MODE_WRITE) {
    	file = H5Fopen(filename.c_str(), H5F_ACC_CREAT | H5F_ACC_RDWR, H5P_DEFAULT);
	} else if (mode == IoMode::MODE_READ) {
        /* open read-only using the STDIO driver 
        hid_t faplist_id = H5Pcreate (H5P_FILE_ACCESS);
        H5Pset_fapl_stdio(faplist_id);*/
    	file = H5Fopen(filename.c_str(), H5F_ACC_RDONLY, H5P_DEFAULT);
	}
	if (file < 0) {
		throw IoException("Failed to open HDF5 file: " + filename);
	}
    root = H5Gopen2(file, "/", H5P_DEFAULT);
    if (root < 0) throw IoException("Failed to open root group in file: " + filename);
    initializeTypes();
}
	
Hdf5Io::~Hdf5Io() {
    herr_t status;
    //XXX TODO: close all open datasets
    if (strType_exeBuffer > 0) status = H5Tclose(strType_exeBuffer);
    if (strType_buffer > 0) status = H5Tclose(strType_buffer);
    if (strType_variable > 0) status = H5Tclose(strType_variable);
    if (root > 0) status = H5Gclose(root);
    if (file > 0) status = H5Fclose(file);
}

template <class pmType>
size_t Hdf5Io::write(const string &dsName, pmType *start, pmType *end) {
    auto it = currentDatasets.find(dsName);
    if (it == currentDatasets.end()) {
        return 0;
    }
    shared_ptr<Dataset> baseDs = it->second;
    shared_ptr<Hdf5Dataset<pmType> > dataset = dynamic_pointer_cast<Hdf5Dataset<pmType> >(baseDs);
    return dataset->write(start, end);
}

template <class pmType>
size_t Hdf5Io::read(const string &dsName, pmType *start, size_t count) {
    auto it = currentDatasets.find(dsName);
    if (it == currentDatasets.end()) {
        return 0;
    }
    shared_ptr<Dataset> baseDs = it->second;
    shared_ptr<Hdf5Dataset<pmType> > dataset = dynamic_pointer_cast<Hdf5Dataset<pmType> >(baseDs);
    return dataset->read(start, count);
}

bool Hdf5Io::setContext(const Context& _context) {
    context = _context;
    group = nullptr;
    currentDatasets.clear();

    auto it = groups.find(context);
    if (it != groups.end()) {
        group = it->second;
    } else {
        group = make_shared<Hdf5Group>(*this, context.hostname);
        groups[context] = group;
    }
    DatasetContext dsKey(context, "");
    for (const pair<string, shared_ptr<DatasetFactory> >& dset: registeredDatasets) {
        dsKey.datasetName = dset.first;
        auto it = dsMap.find(dsKey);
        if (it != dsMap.end()) {
            currentDatasets[dset.first] = it->second;
        } else {
            shared_ptr<Dataset> ptr = (*(dset.second))();
            currentDatasets[dset.first] = ptr;
            addDatasetContext(ptr, context, dset.first);
        }
    }
    return true;
}

void Hdf5Io::initializeTypes() {
    herr_t status; 

    /* setup data structure types */
    strType_exeBuffer = H5Tcopy(H5T_C_S1);
    status = H5Tset_size(strType_exeBuffer, EXEBUFFER_SIZE);
    if (status < 0) {
        throw IoException("Failed to set strType_exeBuffer size");
    }
    
    strType_buffer = H5Tcopy(H5T_C_S1);
    status = H5Tset_size(strType_buffer, BUFFER_SIZE);
    if (status < 0) {
        throw IoException("Failed to set strType_buffer size");
    }

	strType_idBuffer = H5Tcopy(H5T_C_S1);
	status = H5Tset_size(strType_idBuffer, IDENTIFIER_SIZE);
	if (status < 0) {
		throw IoException("Failed to set strType_idBuffer size");
	}

    strType_variable = H5Tcopy(H5T_C_S1);
    status = H5Tset_size(strType_variable, H5T_VARIABLE);
    if (status < 0) {
        throw IoException("Failed to set strType_variable size");
    }

}

/*
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

unsigned int ProcHDF5IO::get_nnetstat() {
	if (hdf5Segment == nullptr) return 0;
	return hdf5Segment->netstatSize;
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
*/

/* metadata_set_string
   writes string-type metadata into the root group of the HDF5 file */
bool Hdf5Io::metadataSetString(const char *ident, const char *value) {
    hid_t attr = -1;
    hid_t ds = H5Screate(H5S_SCALAR);
    herr_t status;
    /* first create the attribute if it doesn't exist */
    if (H5Aexists(root, ident) == 0) {
        attr = H5Acreate2(root, ident, strType_variable, ds, H5P_DEFAULT, H5P_DEFAULT);
    } else {
		attr = H5Aopen(root, ident, H5P_DEFAULT);
    }
    status = H5Awrite(attr, strType_variable, &value);
    H5Aclose(attr);
    H5Sclose(ds);
    return status >= 0;
}

bool Hdf5Io::metadataSetUint(const char *ident, unsigned long value) {
    hid_t attr = -1;
    hid_t ds = H5Screate(H5S_SCALAR);
    herr_t status;
    if (H5Aexists(root, ident) == 0) {
        attr = H5Acreate2(root, ident, H5T_NATIVE_ULONG, ds, H5P_DEFAULT, H5P_DEFAULT);
    } else {
        attr = H5Aopen(root, ident, H5P_DEFAULT);
    }
    status = H5Awrite(attr, H5T_NATIVE_ULONG, &value);
    H5Aclose(attr);
    H5Sclose(ds);
    return status >= 0;
}

bool Hdf5Io::metadataGetString(const char *ident, char **value) {
    hid_t attr = -1;
    hid_t type = -1;
    herr_t status;

    if (H5Aexists(root, ident) == 0) {
        return false;
    }
    attr = H5Aopen(root, ident, H5P_DEFAULT);
    type = H5Aget_type(attr);
    status = H5Aread(attr, type, value);
    H5Tclose(type);
    H5Aclose(attr);
    return status > 0;
}

bool Hdf5Io::metadataGetUint(const char *ident, unsigned long *value) {
    hid_t attr = -1;
    hid_t type = -1;
    herr_t status;

    if (H5Aexists(root, ident) == 0) {
        return false;
    }
    attr = H5Aopen(root, ident, H5P_DEFAULT);
    type = H5Aget_type(attr);
    status = H5Aread(attr, type, value);
    H5Tclose(type);
    H5Aclose(attr);
    return status > 0;
}

#endif

/*
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


ProcRecordType ProcTextIO::read_stream_record(procdata* procData, procstat* procStat, procfd* procFD, netstat* net) {
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
            } else if (strcmp(sPtr, "netstat") == 0) {
                sPtr = ptr + 1;
                return read_netstat(net) ? TYPE_NETSTAT : TYPE_INVALID;
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

bool ProcTextIO::read_netstat(netstat* net) {
    int pos = 0;
    int readBytes = -1;
    bool done = false;
    net->inode = 0;
    while (!done) {
        if (sPtr == NULL || ptr == ePtr) {
            int bytes = fill_buffer();
            if (bytes == 0) break;
        }
        if ((*ptr == ',' || *ptr == '\n') && (readBytes < 0 || readBytes == (ptr-sPtr))) {
            if (*ptr == '\n') done = true;
            *ptr = 0;
            switch (pos) {
                case 0: net->recTime = strtoul(sPtr, &ptr, 10); break;
                case 1: net->recTimeUSec = strtoul(sPtr, &ptr, 10); break;
                case 2: net->local_address = strtoul(sPtr, &ptr, 10); break;
                case 3: net->local_port = strtoul(sPtr, &ptr, 10); break;
                case 4: net->remote_address = strtoul(sPtr, &ptr, 10); break;
                case 5: net->remote_port = strtoul(sPtr, &ptr, 10); break;
                case 6: net->state = strtoul(sPtr, &ptr, 10); break;
                case 7: net->tx_queue = strtoul(sPtr, &ptr, 10); break;
                case 8: net->rx_queue = strtoul(sPtr, &ptr, 10); break;
                case 9: net->tr = strtoul(sPtr, &ptr, 10); break;
                case 10: net->ticks_expire = strtoul(sPtr, &ptr, 10); break;
                case 11: net->retransmit = strtoul(sPtr, &ptr, 10); break;
                case 12: net->uid = strtoul(sPtr, &ptr, 10); break;
                case 13: net->timeout = strtoul(sPtr, &ptr, 10); break;
                case 14: net->inode = strtoul(sPtr, &ptr, 10); break;
                case 15: net->refCount = strtoul(sPtr, &ptr, 10); break;
                case 16: net->type = strtoul(sPtr, &ptr, 10); break;
            }
            pos++;
            sPtr = ptr + 1;
        }
        ptr++;
    }
    return net->inode != 0;
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

unsigned int ProcTextIO::write_netstat(netstat* start_ptr, int cnt) {
    unsigned int nBytes = 0;
    for (int i = 0; i < cnt; i++) {
        netstat* net = &(start_ptr[i]);
        ptr += fprintf(filePtr,
            "%lu,%lu,%lu,%u,%lu,%u,%u,%lu,%lu,%u,%lu,%lu,%lu,%lu,%lu,%u,%d\n",
            net->recTime, net->recTimeUSec, net->local_address,
            net->local_port, net->remote_address, net->remote_port,
            net->state, net->tx_queue, net->rx_queue, net->tr,
            net->ticks_expire, net->retransmit, net->uid,
            net->timeout, net->inode, net->refCount, net->type
        );
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
*/

}
