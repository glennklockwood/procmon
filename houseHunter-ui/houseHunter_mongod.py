import os
import sys
import re
import json
import cgi
import bson

from multiprocessing import current_process
import multiprocessing

import datetime
import time

from BaseHTTPServer import BaseHTTPRequestHandler
from BaseHTTPServer import HTTPServer
import urlparse
from xml.parsers import expat

from pymongo import MongoClient


def note(format, *args):
    sys.stderr.write('[%s]\t%s\n' % (current_process().name, format % args))

def date_range(start_date, end_date):
    for n in range(int((end_date - start_date).days)+1):
        yield start_date + datetime.timedelta(n)
       
class MongoDataServer(HTTPServer):
    def __init__(self, config, requestHandler):
        print config
        HTTPServer.__init__(self, (config['address'], config['port']), requestHandler)
        self.config = config
        
    def serve(self):
        self.client = MongoClient('genepool12.nersc.gov', 27017)
        self.db = self.client.procmon
        self.db.authenticate('usgweb', '23409yasfh39@knvDD')
        self.house = self.db['houseHunter']
        self.cache = {}
        try:
            while True:
                self.handle_request()
        except KeyboardInterrupt:
            pass


class RequestHandler(BaseHTTPRequestHandler):
    #def log_message(self, format, *args):
    #    note(format, args, "")
        
    def run_details_query(self, qtype, user, project, date, query):
        if date is None:
            return '{"Result":"FAIL"}' 
        sortField = 'exePath'
        sortOrder = 1
        startRec = 0
        rangeRec = None
        if 'jtSorting' in query:
            (sortField, sortOrder) = query['jtSorting'].split('%20')
            if sortOrder == 'ASC':
                sortOrder = 1
            else:
                sortOrder = -1
        if 'jtStartIndex' in query:
            startRec = int(query['jtStartIndex'])
        if 'jtPageSize' in query:
            rangeRec = int(query['jtPageSize'])
                
        query = {'date': date}
        if qtype == 'script':
            ttype = 'scripts'
        else:
            ttype = 'executables'
        if user is not None:
            if qtype == 'script':
                ttype = 'scriptUser'
            else:
                ttype = 'execUser'
            query['username'] = user
        if project is not None:
            if qtype == 'script':
                ttype = 'scriptProject'
            else:
                ttype = 'execProject'
            query['project'] = project
        query['type'] = ttype
        cursor = self.server.house.find(query).sort(sortField, sortOrder)
        nRec = cursor.count()
        if rangeRec is None:
            rangeRec = nRec
        cdata = cursor[startRec:startRec+rangeRec]
        data = []
        for value in cdata:
            value['_id'] = str(value['_id'])
            data.append(value)
        retval = {'Result':'OK','Records':data,'TotalRecordCount':nRec}
        return json.dumps(retval)

    def run_summ_query(self, qtype, user, project, date, query):
        if date is None:
            return "{'Result':'FAIL'}"

        sortField = 'exe'
        sortOrder = 1
        startRec = 0
        rangeRec = None
        keyVal = 'username'
        if 'jtSorting' in query:
            (sortField, sortOrder) = query['jtSorting'].split('%20')
            if sortOrder == 'ASC':
                sortOrder = 1
            else:
                sortOrder = -1
        if 'jtStartIndex' in query:
            startRec = int(query['jtStartIndex'])
        if 'jtPageSize' in query:
            rangeRec = int(query['jtPageSize'])

        if qtype == 'user':
            exeType = 'execUser'
            scriptType = 'scriptUser'
            keyVal = 'username'
        else:
            exeType = 'execProject'
            scriptType = 'scriptProject'
            keyVal = 'project'
        reducer = bson.code.Code("""
            function(curr, result) {
                if (curr.type == "%s") {
                    result.exe++
                } else {
                    result.script++
                }
            }
            """ % exeType)
        condition = {
            'type': {'$in':[exeType,scriptType]},
            'date': date
        }
        if user is not None:
            condition['username'] = user
        if project is not None:
            condition['project'] = project
        cursor = self.server.house.group(key = {keyVal:1}, condition = condition, initial = {"exe":0, "script":0}, reduce=reducer)
        nRec = len(cursor)
        if rangeRec is None:
            rangeRec = nRec
        cursor = sorted(cursor, key=lambda x:x[sortField])
        if sortOrder == -1:
            cursor = cursor[::-1]
        data = cursor[startRec:startRec+rangeRec]
        retval = {'Result':'OK','Records':data,'TotalRecordCount':nRec}
        return json.dumps(retval)

    def time_summary(self, qtype, ident, qdate):
        if qtype not in ('user','project',) or ident is None or qdate is None:
            return '{}'
        latest_date = datetime.date.today() - datetime.timedelta(1)
        earliest_date = datetime.date(2013, 6, 1)
        qdate = datetime.datetime.strptime(qdate, '%Y%m%d').date()
        start_date = qdate - datetime.timedelta(days=7)
        end_date = qdate + datetime.timedelta(days=7)
        if start_date < earliest_date:
            start_date = earliest_date
        if end_date > latest_date:
            end_date = latest_date
        (exeType,scriptType,qcol) = ('execUser','scriptUser','username')
        if qtype == 'project':
            (exeType,scriptType,qcol) = ('execProject','scriptProject','project')
        reducer = bson.code.Code("""
            function(curr, result) {
                if (curr.type == "%s") {
                    result.exe++;
                } else {
                    result.script++;
                }
            }
            """ % exeType)
        condition = {
            qcol: ident,
            'date': {'$gte':start_date.strftime("%Y%m%d"), '$lte':end_date.strftime("%Y%m%d")},
            'type': {'$in': [exeType, scriptType] },
        }
        records = self.server.house.group(key={'date':1}, condition=condition, initial={'exe':0,'script':0}, reduce=reducer)
        ret = {
            'dates': [x.strftime('%Y%m%d') for x in date_range(start_date, end_date)],
            'exe': [],
            'script': [],

        }
        retprep = {}
        for record in records:
            retprep[record['date']] = {'exe': record['exe'], 'script': record['script']}
        for date in date_range(start_date, end_date):
            fdate = date.strftime('%Y%m%d')
            exe = 0
            script = 0
            if fdate in retprep:
                exe = retprep[fdate]['exe']
                script = retprep[fdate]['script']
            ret['exe'].append(exe)
            ret['script'].append(script)
            
        print ret
        return json.dumps(ret)

    def run_query(self, query):
        currtime = time.mktime(time.gmtime())
        www_project = None
        www_user = None
        www_date = None
        print 'run_query: %s' % (query)
        if 'project' in query and query['project'] != 'Any':
            www_project = query['project']
        if 'user' in query and query['user'] != 'Any':
            www_user = query['user']
        if 'date' in query and query['date'] != 'None':
            www_date = query['date']

        if 'get_users' in query:
            if 'get_users' in self.server.cache and currtime < self.server.cache['get_users'][0]:
                return self.server.cache['get_users'][1]
            t_users = self.server.house.distinct('username')
            users = ['Any']
            for user in t_users:
                if user is not None and len(user) > 0:
                    users.append(user)
            message =  json.dumps(users)
            self.server.cache['get_users'] = (currtime+3600,message)
            return message
        if 'get_projects' in query:
            if 'get_projects' in self.server.cache and currtime < self.server.cache['get_projects'][0]:
                return self.server.cache['get_projects'][1]
            t_projects = self.server.house.distinct('project')
            projects = ['Any']
            for project in t_projects:
                if project is not None and len(project) > 0:
                    projects.append(project)
            message =  json.dumps(projects)
            self.server.cache['get_projects'] = (currtime+3600,message)
            return message
        if 'get_summary' in query:
            if www_date is None:
                return "{}"
            types = []
            if www_project is None and www_user is None:
                types = ['users','projects','executables','scripts']
            elif www_user is not None:
                types = ['users','execUser','scriptUser']
            elif www_project is not None:
                types = ['projects','execProject','scriptProject']
            ret_val = {'exec_count':'N/A', 'projects_count':'N/A','users_count':'N/A','scripts_count':'N/A'}
            for ttype in types:
                label = ttype
                if ttype.find('exec') >= 0:
                    label = 'exec'
                if ttype.find('script') >= 0:
                    label = 'scripts'
                label += '_count'
                query = {'date': www_date, 'type': ttype}
                if www_user is not None:
                    query['username'] = www_user
                if www_project is not None:
                    query['project'] = www_project
                ret_val[label] = self.server.house.find(query).count()
            return json.dumps(ret_val)
        if 'get_user_detailed_summary' in query:
            return self.run_summ_query('user', www_user, www_project, www_date, query)
        if 'get_project_detailed_summary' in query:
            return self.run_summ_query('project', www_user, www_project, www_date, query)

        if 'get_exec' in query:
            return self.run_details_query('exec', www_user, www_project, www_date, query)
        if 'get_script' in query:
            return self.run_details_query('script', www_user, www_project, www_date, query)
        if 'get_time_summary' in query:
            qtype = 'user'
            ident = www_user
            if www_project is not None:
                qtype = 'project'
                ident = www_project
            return self.time_summary(qtype, ident, www_date)
            
    def do_GET(self):
        
        parsed_request = urlparse.urlparse(self.path)

        #prolog = '\r\n'.join(message_parts)
        message = ''
        messageComplete = False
        
        #addrStr = "addr: %s" % self.client_address[0]
        #self.wfile.write(addrStr)
        
        print parsed_request
        query = parsed_request.query.strip()
        queryList = query.split("&")
        queryHash = {}

        for queryItem in queryList:
            if len(queryItem) == 0:
                continue
            data = queryItem.split("=")
            if len(data) == 1:
                queryHash[data[0].strip()] = 1
            else:
                queryHash[data[0].strip()] = '='.join(data[1:]).strip()


        path = parsed_request.path
        if path != "/houseHunter.cgi":
            self.send_response(404)
            self.end_headers()
        else:
            self.send_response(200)
            self.send_header('Access-Control-Allow-Origin','http://portal.nersc.gov')
            message = self.run_query(queryHash)
            self.send_header('Content-type','application/json')
            self.end_headers()
            self.wfile.write(message)
            self.wfile.write('\n')

    def do_POST(self):
        form = cgi.FieldStorage(fp=self.rfile, headers=self.headers, environ={'REQUEST_METHOD':'POST', 'CONTENT_TYPE':self.headers['Content-Type'], })
        parsed_request = urlparse.urlparse(self.path)
        query = parsed_request.query.strip()
        queryList = query.split("&")
        queryHash = {}

        for queryItem in queryList:
            if len(queryItem) == 0:
                continue
            data = queryItem.split("=")
            if len(data) == 1:
                queryHash[data[0].strip()] = 1
            else:
                queryHash[data[0].strip()] = '='.join(data[1:]).strip()

        for queryItem in form.keys():
            queryHash[queryItem] = form.getfirst(queryItem)

        path = parsed_request.path
        path = os.path.split(path)
        print 'POST: %s' % str(path)
        if path[0] != '/poster':
            self.send_response(404)

        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin','http://portal.nersc.gov')
        self.send_header('Content-type','application/json')
        self.end_headers()
        queryHash[path[-1]] = 1
        message = self.run_query(queryHash)
        self.wfile.write(message)
        self.wfile.write('\n')
    
def serve_forever(server):
    note('starting server')
    try:
        server.serve()
    except KeyboardInterrupt:
        pass

def runpool(config):
    server = MongoDataServer(config, RequestHandler)
    
    for i in range(config['nHTTPServerProcesses'] - 1):
        p = multiprocessing.Process(target=serve_forever, args=( server, ))
        p.start()

    serve_forever(server)

import argparse

def main(argv):
    argParser = argparse.ArgumentParser(description='Query genepool SGE queue status (safely) - cached qstat server')
    argParser.add_argument("-I", "--ip", nargs=1, help="Binding IP address", default=["genepool04.nersc.gov"], type=str)
    argParser.add_argument("-p", "--port", nargs=1, help="Port", default=[8242], type=int)
    argParser.add_argument("-t", "--threads", nargs=1, help="number of server threads (+1 for task management)", default=[2], type=int)

    argData = argParser.parse_args(argv[1:])
    config = {}
    config['address'] = argData.ip[0]
    config['port'] = int(argData.port[0])
    config['nHTTPServerProcesses'] = int(argData.threads[0])

    defaultConfig = {
        'address': '128.55.71.23',
        'port': 8242,
        'nHTTPServerProcesses': 4,
    }
    
    runpool(config)


if __name__ == '__main__':
    main(sys.argv)
