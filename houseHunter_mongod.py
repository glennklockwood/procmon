
import sys
import re

from multiprocessing import Queue, Lock, current_process
import multiprocessing
import time

from BaseHTTPServer import BaseHTTPRequestHandler
from BaseHTTPServer import HTTPServer
import urlparse
from xml.parsers import expat

from pymongo import MongoClient


def note(format, *args):
    sys.stderr.write('[%s]\t%s\n' % (current_process().name, format % args))
       
       
class MongoDataServer(HTTPServer):
    def __init__(self, config, requestHandler):
        HTTPServer.__init__(self, (config['address'], config['port']), requestHandler)
        self.config = config
        self.client = MongoClient('genepool12.nersc.gov', 27017)
        self.db = client.procmon
        self.db.authenticate('usgweb', '23409yasfh39@knvDD')
        self.house = db['houseHunter']
        
    def serve(self):
        try:
            while True:
                self.handle_request()
        except KeyboardInterrupt:
            pass

class RequestHandler(BaseHTTPRequestHandler):
    #def log_message(self, format, *args):
    #    note(format, args, "")
        
    def do_GET(self):
        
        parsed_request = urlparse.urlparse(self.path)

        #prolog = '\r\n'.join(message_parts)
        message = ''
        messageComplete = False
        
        #addrStr = "addr: %s" % self.client_address[0]
        #self.wfile.write(addrStr)
        
        query = parsed_request.query.strip()
        queryList = query.split("&")
        queryHash = {}

        for queryItem in queryList:
            data = queryItem.split("=")
            if len(data) == 1:
                queryHash[data[0].strip()] = 1
            else:
                queryHash[data[0].strip()] = '='.join(data[1:]).strip()

        self.send_response(200)
        self.end_headers()

        self.wfile.write(message)
        self.wfile.write('\n')
        
    
def serve_forever(server):
    note('starting server')
    try:
        server.serve()
    except KeyboardInterrupt:
        pass

def runpool(config, processQueues):
    server = MongoDataServer(config, RequestHandler)
    
    for i in range(config['nHTTPServerProcesses']):
        q = Queue()
        l = Lock()
        p = multiprocessing.Process(target=serve_forever, args=( MongoDataServer(config, RequestHandler), ))
        p.start()

    serve_forever(server)

import argparse

def main(argv):
    argParser = argparse.ArgumentParser(description='Query genepool SGE queue status (safely) - cached qstat server')
    argParser.add_argument("-I", "--ip", nargs=1, help="Binding IP address", default=["genepool-shadow.nersc.gov"], type=str)
    argParser.add_argument("-p", "--port", nargs=1, help="Port", default=[8241], type=int)
    argParser.add_argument("-t", "--threads", nargs=1, help="number of server threads (+1 for task management)", default=[4], type=int)

    argData = argParser.parse_args(argv[1:])
    config = {}
    config['address'] = argData.ip[0]
    config['port'] = int(argData.port[0])
    config['nHTTPServerProcesses'] = int(argData.threads[0])

    defaultConfig = {
        'address': '128.55.54.38',
        'port': 8241,
        'nHTTPServerProcesses': 4,
    }
    runpool(config, processQueues)


if __name__ == '__main__':
    main(sys.argv)
