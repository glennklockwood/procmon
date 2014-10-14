class netstat: public ProcmonDataset {
    unsigned long local_address;
    unsigned int local_port;
    unsigned long remote_address;
    unsigned int remote_port;
    unsigned short state;
    unsigned long tx_queue;
    unsigned long rx_queue;
    unsigned short tr;
    unsigned long ticks_expire;
    unsigned long retransmit;
    unsigned long uid;
    unsigned long timeout;
    unsigned long inode;
    unsigned int refCount;
    int type; // 0 = tcp, 1 = udp

    virtual bool equivRecord(const netstat& other) const {
        return inode == other.inode && local_address == other.local_address && remote_address == other.remote_address && local_port == other.local_port && remote_port == other.remote_port;
    }

    virtual bool isBad() const {
        if (type < 0 || type > 1) return true;
        if (inode == 0) return true;
        return false;
    }

    virtual bool isChanged(const netstat& other) const {
        if (local_address != other.local_address) return true;
        if (local_port    != other.local_port)    return true;
        if (remote_address != other.remote_address) return true;
        if (remote_port != other.remote_port) return true;
        if (state != other.state) return true;
        if (inode != other.inode) return true;
        return false;
    }
};

class netstatInterface : public ProcmonDatasetInterface {
    public:
    inline virtual static const size_t getMagicNumber() {
        return 0xfffae0c39;
    }
};



namespace ProcmonParser {

}

namespace pmio2 {
class AmqpNetstat : public Dataset {
    public:
    AmqpNetstat(shared_ptr<AmqpIo> _amqp, const Context &context):
        Dataset(_amqp, context, "netstat"),
        amqp(_amqp)
    {
    }

    template <typename Iterator>
    size_t write(Iterator begin, Iterator end) {
    }

    size_t read(vector<netstat> &buffer, int nRecords

bool ProcAMQPIO::_read_netstat(netstat *startPtr, int nRecords, char* buffer, int nBytes) {
	char* ptr = buffer;
	char* ePtr = buffer + nBytes;
	char* sPtr = ptr;

    int pos = 0;
	int idx = 0;
	int readBytes = -1;
	bool done = false;
	netstat* net = startPtr;
    while (idx < nRecords && ptr < ePtr) {
        if ((*ptr == ',' || *ptr == '\n') && (readBytes < 0 || readBytes == (ptr-sPtr))) {
			if (done) {
				net = &(startPtr[++idx]);
				pos = 0;
				readBytes = -1;
				done = false;
			}
            if (*ptr == '\n') {
				done = true;
			}
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
    return idx != nRecords;
}
template<>

