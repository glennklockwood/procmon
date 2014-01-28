from functools import update_wrapper
from datetime import timedelta
import threading
import datetime
import time
import flask
import pika
from Queue import Queue
import logging
import json
import random

sq_lock = threading.Lock()
session_queues = {}

app = flask.Flask(__name__)
app.debug = True
app.secret_key = 'T\xf1\xbd\xac \xc0Zy\xd9xgG\xa7Z\xc84Q\xa2\xd4d6>\xe7\x1c'
log = logging.getLogger('werkzeug')

class amqp_bridge(threading.Thread):
    def __init__(self, identifier, subidentifier, queue):
        threading.Thread.__init__(self)
        self.identifier = identifier
        self.subidentifier = subidentifier
        self.q = queue

    def run(self):
        log.warning('Staring thread')
        credentials = pika.PlainCredentials('procmon','nomcorp')
        log.warning('about to connect')
        self.amqp_conn = pika.BlockingConnection(pika.ConnectionParameters('genepool-mq.nersc.gov', 5672, 'jgi', credentials))
        self.amqp_conn.add_timeout(60, self.on_timeout)
        self.amqp_channel = self.amqp_conn.channel()
        log.warning('declaring exchange')
        self.amqp_channel.exchange_declare(exchange="procmon", exchange_type="topic")
        log.warning('declaring queue')
        queue_res = self.amqp_channel.queue_declare(exclusive=True)
        self.amqp_qname = queue_res.method.queue
        log.warning('binding queue')
        routing_key = '*.%s.%s.*' % (self.identifier, self.subidentifier)
        log.warning('routing_key: %s' % routing_key)
        self.amqp_channel.queue_bind(exchange="procmon", queue=self.amqp_qname, routing_key=routing_key)
        log.warning('setting up consumer')
        self.amqp_channel.basic_consume(self.recv_message, queue=self.amqp_qname, no_ack=True)
        log.warning('entering consumer loop')
        try:
            self.amqp_channel.start_consuming()
        except:
            self.q.put(None)

    def on_timeout(self):
        self.amqp_conn.close()
    
    def recv_message(self, channel, method_frame, header_frame, body):
        log.warning('got message: %s' % method_frame.routing_key)
        (host,ident,subident,msg_type) = method_frame.routing_key.split('.')
        msg_data = None
        if msg_type == "procstat":
            msg_data = procmon.parse_procstat(body)
        elif msg_type == "procdata":
            msg_data = procmon.parse_procdata(body)
        elif msg_type == "procfd":
            msg_data = procmon.parse_procfd(body)

        self.q.put({'host':host,'id':ident,'subid':subident,'type':msg_type,'data':msg_data,})
            
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
        options_resp = flask.current_app.make_default_options_response()
        return options_resp.headers['allow']

    def decorator(f):
        def wrapped_function(*args, **kwargs):
            if automatic_options and flask.request.method == 'OPTIONS':
                resp = current_app.make_default_options_response()
            else:
                resp = flask.make_response(f(*args, **kwargs))
            if not attach_to_all and flask.request.method != 'OPTIONS':
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

def stream_procdata(queue):
    while True:
        try:
            x = queue.get(block=False)
            yield "data: '%s'\n\n" % "gotta message"
        except:
            time.sleep(1)

@app.route("/login",)
def login():
    session_hash = '%030x' % random.randrange(16**30)
    flask.session['session_hash'] = session_hash
    return flask.Response("session_hash:'%s'\n\n" % session_hash)

@app.route("/procmon", )
@crossdomain(origin='http://portal.nersc.gov')
def setup_procdata():
    query_hash = {}
    for key in flask.request.args:
        query_hash[key] = flask.request.args[key]
    if flask.request.method == "POST":
        for key in flask.request.form:
            query_hash[key] = flask.request.form[key]
    
    jobid = query_hash['jobid'] if 'jobid' in query_hash else None
    taskid = query_hash['taskid'] if 'taskid' in query_hash else None
    session_hash = query_hash['session_hash'] if 'session_hash' in query_hash else None

    if taskid is None: taskid = '*'
    # setup AMQP connection
    queue = None

    with sq_lock:
        if session_hash not in session_queues:
            queue = Queue()
            amqp_thread = amqp_bridge(jobid, taskid, queue)
            amqp_thread.start()
            session_queues[session_hash] = (queue, amqp_thread,)
        else:
            log.warning('got session: %s' % session_hash)
            (queue, amqp_thread,) = session_queues[session_hash]
            if not amqp_thread.is_alive():
                return flask.Response(status=204)
    return flask.Response(stream_procdata(queue), mimetype="text/event-stream")


        

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=4043, debug=True, threaded=True)
