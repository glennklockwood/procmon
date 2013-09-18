from flask import Flask, jsonify, make_response, request, current_app
from functools import update_wrapper
from datetime import timedelta
from flask.ext.pymongo import PyMongo
import bson
import datetime
import time

app = Flask(__name__)
app.config["MONGO_HOST"] = '128.55.56.19'
app.config['MONGO_PORT'] = 27017
app.config['MONGO_DBNAME'] = 'procmon'
app.config['MONGO_USERNAME'] = 'usgweb'
app.config['MONGO_PASSWORD'] = '23409yasfh39@knvDD'
app.debug = True

mongo = PyMongo(app)

__all__ = ['make_json_app']
def make_json_app(import_name, **kwargs):
    def make_json_error(ex):
        response = jsonify(message=str(ex))
        response.status_code = (ex.code if isinstance(ex, HTTPException) else 500)
        return response
    app = Flask(import_name, **kwargs)
    for code in default_exceptions.iterkeys():
        app.error_handler_spec[None][code] = make_json_error
    return app

def crossdomain(origin=None, methods=None, headers=None, max_age=21600,
    attach_to_all=True, automatic_options=True):

    if methods is not None:
        methods = ', '.join(sorted(x.upper()) for x in methods)
    if headers is not None and not isinstance(headers, basestring):
        headers = ', '.join(x.upper() for x in headers)
    if not isinstance(origin, basestring):
        origin = ', '.join(origin)
    if isinstance(max_age, timedelta):
        max_age = max_age.total_seconds()
    
    def get_methods():
        if methods is not None:
            return methods
        options_resp = current_app.make_default_options_response()
        return options_resp.headers['allow']

    def decorator(f):
        def wrapped_function(*args, **kwargs):
            if automatic_options and request.method == 'OPTIONS':
                resp = current_app.make_default_options_response()
            else:
                resp = make_response(f(*args, **kwargs))
            if not attach_to_all and request.method != 'OPTIONS':
                return resp

            h = resp.headers
            h['Access-Control-Allow-Origin'] = origin
            h['Access-Control-Allow-Methods'] = get_methods()
            h['Access-Control-Max-Age'] = str(max_age)
            if headers is not None:
                h['Access-Control-Allow-Headers'] = headers
            return resp
        f.provide_automatic_options = False
        return update_wrapper(wrapped_function, f)
    return decorator

def date_range(start_date, end_date):
    for n in range(int((end_date - start_date).days)+1):
        yield start_date + datetime.timedelta(n)

def parse_datatable_query(query, columns):
    start = 0
    length = 50
    sort = []
    if 'iDisplayStart' in query:
        start = int(query['iDisplayStart'])
    if 'iDisplayLength' in query:
        length = int(query['iDisplayLength'])
    if 'iSortCol_0' in query and 'iSortingCols' in query:
        for idx in xrange(int(query['iSortingCols'])):
            sval = 'iSortCol_%d' % idx
            if sval not in query:
                continue
            sval = int(query[sval])
            col = 'bSortable_%d' % sval
            if col in query and query[col] == 'true':
                col = 'sSortDir_%d' % idx
                if col in query:
                    if query[col] == 'asc':
                        sort.append((columns[sval], 1,))
                    else:
                        sort.append((columns[sval], -1,))
    if len(sort) == 0:
        sort.append((columns[0], 1,))
    ret = {'start': start, 'length': length, 'sort': sort}
    print ret
    return ret

def run_details_query(collection, qtype, user, project, host, date, queryHash):
    coll = mongo.db[collection]
    if date is None:
        return {}
    
    columns = ('exePath','nobs','walltime','cpu_time',)
    ttype = 'executables'
            
    query = {'date': date}
    if qtype == 'script':
        ttype = 'scripts'
        columns = ('scripts','exePath','nobs','walltime','cpu_time',)

    qdata = parse_datatable_query(queryHash, columns)

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
    if host is not None:
        if qtype == 'script':
            ttype = 'scriptHost'
        else:
            ttype = 'execHost'
        query['host'] = host
    query['type'] = ttype
    cursor = coll.find(query).sort(qdata['sort'])
    nRec = cursor.count()
    cdata = cursor[qdata['start']:qdata['start']+qdata['length']]
    data = []
    for value in cdata:
        row = []
        for col in columns:
            row.append(value[col])
        data.append(row)
    retval = {'aaData': data, 'iTotalRecords': nRec, 'iTotalDisplayRecords': nRec}
    if 'sEcho' in queryHash:
        retval['sEcho'] = int(queryHash['sEcho'])
    return retval

def run_summ_query(collection, qtype, user, project, host, date, queryHash):
    if date is None:
        return {}

    coll = mongo.db[collection]
    keyVal = 'username'
    if qtype == 'user':
        exeType = 'execUser'
        scriptType = 'scriptUser'
        keyVal = 'username'
    elif qtype == 'project':
        exeType = 'execProject'
        scriptType = 'scriptProject'
        keyVal = 'project'
    elif qtype == 'host':
        exeType = 'execHost'
        scriptType = 'scriptHost'
        keyVal = 'host'
    else:
        return {}
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
    columns = (keyVal, 'exe', 'script')
    qdata = parse_datatable_query(queryHash, columns)
    if user is not None:
        condition['username'] = user
    if project is not None:
        condition['project'] = project
    if host is not None:
        condition['host'] = host
    cursor = coll.group(key = {keyVal:1}, condition = condition, initial = {"exe":0, "script":0}, reduce=reducer)
    nRec = len(cursor)
    cursor = sorted(cursor, key=lambda x:x[qdata['sort'][0][0]])
    if qdata['sort'][0][1] == -1:
        cursor = cursor[::-1]
    cdata = cursor[qdata['start']:qdata['start']+qdata['length']]
    data = []
    for value in cdata:
        row = []
        for col in columns:
            row.append(value[col])
        data.append(row)
    retval = {'aaData': data, 'iTotalRecords': nRec, 'iTotalDisplayRecords': nRec}
    if 'sEcho' in queryHash:
        retval['sEcho'] = int(queryHash['sEcho'])
    return retval

def time_summary(collection, qtype, ident, qdate):
    if qtype not in ('user','project','host',None,) or qdate is None:
        return {}
    coll = mongo.db[collection]
    latest_date = datetime.date.today() - datetime.timedelta(1)
    earliest_date = datetime.date(2013, 6, 1)
    qdate = datetime.datetime.strptime(qdate, '%Y%m%d').date()
    start_date = qdate - datetime.timedelta(days=7)
    end_date = qdate + datetime.timedelta(days=7)
    if start_date < earliest_date:
        start_date = earliest_date
    if end_date > latest_date:
        end_date = latest_date
    (exeType,scriptType,qcol) = ('executables','scripts',None)
    if qtype == 'user':
        (exeType,scriptType,qcol) = ('execUser','scriptUser','username')
    if qtype == 'project':
        (exeType,scriptType,qcol) = ('execProject','scriptProject','project')
    if qtype == 'host':
        (exeType,scriptType,qcol) = ('execHost','scriptHost','host')

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
        'date': {'$gte':start_date.strftime("%Y%m%d"), '$lte':end_date.strftime("%Y%m%d")},
        'type': {'$in': [exeType, scriptType] },
    }
    if qcol is not None:
        condition[qcol] = ident
    records = coll.group(key={'date':1}, condition=condition, initial={'exe':0,'script':0}, reduce=reducer)
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
        
    return ret
    
def get_userlist():
    t_users = mongo.db.summary.find({'type':'users'})[0]['obj']
    users = ['Any']
    for user in t_users:
        if user is not None and len(user) > 0:
            users.append(user)
    return users

def get_projectlist():
    t_projects = mongo.db.summary.find({'type':'projects'})[0]['obj']
    projects = ['Any']
    for project in t_projects:
        if project is not None and len(project) > 0:
            projects.append(project)
    return projects

def get_hostlist():
    t_hosts = mongo.db.summary.find({'type':'hosts'})[0]['obj']
    hosts = ['Any']
    for host in t_hosts:
        if host is not None and len(host) > 0:
            hosts.append(host)
    return hosts

def get_summary(collection, date=None, user=None, project=None, host=None):
    if date is None:
        return {}
    if collection is None:
        return {}
    coll = mongo.db[collection]
    types = []
    if project is None and user is None and host is None:
        types = ['users','projects','executables','scripts','hosts']
    elif user is not None:
        types = ['users','execUser','scriptUser']
    elif project is not None:
        types = ['projects', 'execProject', 'scriptProject']
    elif host is not None:
        types = ['hosts', 'execHost', 'scriptHost']
    ret_val = {'exec_count':'N/A', 'projects_count': 'N/A', 'users_count':'N/A', 'scripts_count':'N/A', 'hosts_count':'N/A'}
    for ttype in types:
        label = ttype
        if ttype.find('exec') >= 0:
            label = 'exec'
        if ttype.find('script') >= 0:
            label = 'scripts'
        label += '_count'
        query = {'date': date, 'type': ttype}
        if user is not None:
            query['username'] = user
        if project is not None:
            query['project'] = project
        if host is not None:
            query['host'] = host
        ret_val[label] = coll.find(query).count()
    return ret_val    

        
@app.route("/get_mongo_data", )
@crossdomain(origin='https://portal-auth.nersc.gov')
def serve_query():
    query_hash = {}
    for key in request.args:
        query_hash[key] = request.args[key]
    if request.method == "POST":
        for key in request.form:
            query_hash[key] = request.form[key]
    
    date = query_hash['date'] if 'date' in query_hash else None
    user = query_hash['user'] if 'user' in query_hash else None
    host = query_hash['host'] if 'host' in query_hash else None
    project = query_hash['project'] if 'project' in query_hash else None
    if user == "Any":
        user = None
    if project == "Any":
        project = None
    if host == "Any":
        host = None
    collection = query_hash['collection'] if 'collection' in query_hash else None

    if collection not in ('houseHunter','firehose',):
        return jsonify({})

    if 'get_summary' in query_hash:
        return jsonify(get_summary(collection, date, user, project, host))

    if 'get_users' in query_hash:
        userlist = get_userlist()
        return jsonify({"users":userlist})

    if 'get_projects' in query_hash:
        projectlist = get_projectlist()
        return jsonify({"projects":projectlist})

    if 'get_hosts' in query_hash:
        hostlist = get_hostlist()
        return jsonify({"hosts":hostlist})

    if 'get_user_detailed_summary' in query_hash:
        return jsonify(run_summ_query(collection, 'user', user, project, host, date, query_hash))
    if 'get_project_detailed_summary' in query_hash:
        return jsonify(run_summ_query(collection, 'project', user, project, host, date, query_hash))
    if 'get_host_detailed_summary' in query_hash:
        return jsonify(run_summ_query(collection, 'host', user, project, host, date, query_hash))
    if 'get_exec' in query_hash:
        return jsonify(run_details_query(collection, 'exec', user, project, host, date, query_hash))
    if 'get_script' in query_hash:
        return jsonify(run_details_query(collection, 'script', user, project, host, date, query_hash))
    if 'get_time_summary' in query_hash:
        qtype = None
        ident = None
        if user is not None:
            qtype = 'user'
            ident = user
        if project is not None:
            qtype = 'project'
            ident = project
        if host is not None:
            qtype = 'host'
            ident = host
        return jsonify(time_summary(collection, qtype, ident, date))

    return jsonify({})

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=4043, debug=True)
