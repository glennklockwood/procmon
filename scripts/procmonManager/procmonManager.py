#!/usr/bin/env python

import os
import sys
import subprocess
from datetime import datetime, timedelta
import time
import socket
import threading
import json
import traceback
import errno
import re
import shutil
import argparse
from ConfigParser import SafeConfigParser

procmonInstallBase = ''
if 'PROCMON_DIR' in os.environ:
    procmonInstallBase = os.environ['PROCMON_DIR']

def split_args(arg_str, splitRegex):
    items = re.split(splitRegex, arg_str)
    ret_items = []
    for item in items:
        item = item.strip()
        if len(item) > 0:
            ret_items.append(item)
    return ret_items

def split_path(arg_str):
    return split_args(arg_str, '[:\n]')

def is_True(arg_str):
    return arg_str == "True"

def split_comma(arg_str):
    return split_args(arg_str, '[,\s\n]')

def send_email(config, subject, message):
    if not config.use_email:
        return
    import smtplib
    message = """From: %s
To: %s
Subject: MESSAGE FROM PROCMON: %s

%s
""" % (config.email_originator, ", ".join(config.email_list), subject, message)

    try:
        smtp_message = smtplib.SMTP('localhost')
        smtp_message.sendmail(config.email_originator, config.email_list, message)
    except smtplib.SMTPException:
        print "Error: failed to send email!"

def get_exception():
    exc_type, exc_value, exc_traceback = sys.exc_info()
    str_exc = traceback.format_exc()
    str_tb = '\n'.join(traceback.format_tb(exc_traceback))
    str_stack2 = '\n'.join(traceback.format_stack())
    return '%s\n%s\n%s\n' % (str_exc, str_tb, str_stack2)

def start_procMuxer(config, group=None, id=None, prefix=None, pidfile=None):
    args = [config.procMuxerPath, '-c', '60', '-d']
    if prefix is not None:
        args.extend(['-O',prefix])
    if group is not None:
        args.extend(['-g',group])
    if id is not None:
        args.extend(['-i',str(id)])
    if pidfile is not None:
        args.extend(['-p',pidfile])

    print "starting proxmuxer with args: ", args
    return subprocess.call(args, stdin=None, stdout=None, stderr=None)

def get_muxer_pid(config, muxer_id):
    pidfilename = "%s/%s/%d" % (config.base_pid_path, config.group, muxer_id)
    current_pid = None
    if os.path.exists(pidfilename):
        try:
            fd = open(pidfilename, 'r')
            for line in fd:
                current_pid = int(line.strip())
        except:
            pass
    return current_pid

def is_muxer_running(config, muxer_id):
    current_pid = get_muxer_pid(config, muxer_id)

    if type(current_pid) is int:
        procfile = "/proc/%d/status" % current_pid
        return os.path.exists(procfile)
    return False

def get_current_files(config, muxer_id):
    current_pid = get_muxer_pid(config, muxer_id)
    files = []
    if type(current_pid) is int:
        for fdnum in xrange(3,10):
            fdpath = "/proc/%d/fd/%d" % (current_pid, fdnum)
            if os.path.exists(fdpath):
                filename = os.readlink(fdpath)
                if re.search('%s.*procMuxer\.%d' % (config.group, muxer_id), filename):
                    files.append(filename)
            else:
                break
    return files

def mkdir_p(path):
    try:
        os.makedirs(path, 0755)
    except OSError as e:
        if e.errno == errno.EEXIST and os.path.isdir(path): pass
        else: raise

def register_jamo(config, fname, ftype, sources = None):
    md_final = None
    tape_archival = [1]
    local_purge_days = 180

    if ftype == "procmon_badrecords_h5":
        tape_archival = []
        local_purge_days = 7
    if ftype == "procmon_stripe_h5":
        tape_archival = []
        local_purge_days = 7
    if ftype == "procmon_reduced_h5":
        tape_archival = [1]
        local_purge_days = 180

    retval = subprocess.call(['/bin/setfacl', '-m', 'user:%s:rw-' % jamo_user, fname])
    if retval != 0:
        send_email(config, "failed to set acl", fname)
        return None

    md_proc = subprocess.Popen([config.metadata_path, '-i', fname], stdout=subprocess.PIPE)
    (stdout, stderr) = md_proc.communicate()
    if md_proc.returncode == 0:
        metadata = json.loads(stdout)
        if 'recording_start' in metadata:
            tmp_dt = datetime.fromtimestamp(int(metadata['recording_start']))
            metadata['recording_start'] = tmp_dt.strftime("%Y-%m-%dT%H:%M:%S.%f")
        if 'recording_stop' in metadata:
            tmp_dt = datetime.fromtimestamp(int(metadata['recording_stop']))
            metadata['recording_stop'] = tmp_dt.strftime("%Y-%m-%dT%H:%M:%S.%f")
        del metadata['hosts']
        if sources is not None:
            metadata['source'] = {'metadata_id': sources}
        md_final = {}
        md_final['procmon'] = metadata
    else:
        send_email(config, "failed to read file stats", fname)
        return None
    
    posted = None
    if sources is None:
        sources = []
    with config.sdm_lock:
        posted = config.sdm.post('api/metadata/file',
                file=fname,
                file_type=ftype,
                local_purge_days=local_purge_days,
                backup_services=tape_archival,
                inputs=sources,
                metadata=md_final,
        )
    if posted is None or 'metadata_id' not in posted:
        send_email(config, "failed to register with jamo", "%s\n%s\n" % (fname, ftype, ))
    return posted


def reduce_files(config, timeobj, filenames):
    """Runs the reducer on the files.  Then moves files to final destination,
       sets proper permissions, then registers the files with JAMO"""

    product_output = "%s/%s/processing/%s.%s.h5" % (config.base_prefix, config.group, config.h5_prefix, timeobj.strftime("%Y%m%d%H%M%S"))
    bad_output = "%s/%s/processing/bad_%s.%s.h5" % (config.base_prefix, config.group, config.h5_prefix, timeobj.strftime("%Y%m%d%H%M%S"))
    reducer_args = [config.reducer_path, '-o', product_output, '-b', bad_output]
    for f in filenames:
        reducer_args.extend(['-i', f])
    retval = subprocess.call(reducer_args, stdin=None, stdout=None, stderr=None)
    if retval != 0:
        send_email(config, "reducer failed!", "retcode: %d\ncmd: %s" % (retval, " ".join(reducer_args)))
        return 1
    (currpath, product_fname) = os.path.split(product_output)
    (currpath, bad_fname) = os.path.split(bad_output)

    final_product = '%s/%s' % (config.h5_path, product_fname)
    final_badoutput = '%s/%s' % (config.target_scratch, bad_fname)

    sources = []
    for f in filenames:
        (somepath,fname) = os.path.split(f)
        newpath = "%s/%s" % (config.target_scratch, fname)
        try:
            shutil.move(f, newpath);
            os.chmod(newpath, 0444)
        except:
            send_email(config, "reducer failed to move file", "%s\n%s\n%s\n" % (f, newpath, get_exception()))
            return 1
        if config.use_jamo:
            response = register_jamo(config, newpath, "procmon_stripe_h5")
            if 'metadata_id' in response:
                sources.append(response['metadata_id'])


    try:
        shutil.move(product_output, final_product)
        os.chmod(final_product, 0444)
    except:
        send_email(config, "reducer failed to move file", "%s\n%s\n%s\n" % (product_output, final_product, get_exception()))
        return 1

    if config.use_jamo:
        register_jamo(config, final_product, "procmon_reduced_h5", sources)

    try:
        shutil.move(bad_output, final_badoutput)
        os.chmod(final_badoutput, 0444)
    except:
        send_email(config, "reducer failed to move file", "%s\n%s\n%s" % (bad_output, final_badoutput, get_exception()))
        return 1

    if config.use_jamo:
        register_jamo(config, final_badoutput, "procmon_badrecords_h5", sources)
        

def main_loop(config):
    # create pid directory
    mkdir_p("%s/%s" % (config.base_pid_path, config.group))

    # create working directory
    mkdir_p("%s/%s" % (config.base_prefix, config.group))
    mkdir_p("%s/%s/processing" % (config.base_prefix, config.group))
    mkdir_p("%s/%s/collecting" % (config.base_prefix, config.group))
    os.chdir("%s/%s" % (config.base_prefix, config.group))

    file_prefix = "%s/%s/collecting/procMuxer" % (config.base_prefix, config.group)

    last_rotation = None

    send_email(config, "%s starting" % config.group, "starting management of %s ProcMuxer group on %s" % (config.group, socket.gethostname()))
    ## enter into perpetual loop
    reduce_threads = {}
    while True:
        ## check if the muxers are running, if not, restart them
        for muxer_id in xrange(config.num_procmuxers):
            if not is_muxer_running(config, muxer_id):
                start_procMuxer(config, group=config.group, id=muxer_id,
                        prefix="%s.%d" % (file_prefix, muxer_id),
                        pidfile="%s/%s/%d" % (config.base_pid_path, config.group, muxer_id)
                )

        ## if more than an hour has elapsed since the last successful
        ## rotation of log files, then check 
        if (not last_rotation) or ((datetime.now() - last_rotation).total_seconds() > 3600):
            ## get list of currently open files
            open_filenames = []
            for muxer_id in xrange(config.num_procmuxers):
                files = get_current_files(config, muxer_id)
                for f in files:
                    (path,fname) = os.path.split(f)
                    if fname: open_filenames.append(fname)

            ## get list of files in collecting, filter out current files and non-targets
            ## put into candidate_files
            files = os.listdir('%s/%s/collecting' % (config.base_prefix, config.group))
            open_files = []
            candidate_files = []
            for f in files:
                fmatch = re.match('procMuxer\.(\d+)\.(\d+).h5', f)
                if not fmatch: continue

                muxer = fmatch.group(1)
                file_dt = datetime.strptime(fmatch.group(2), "%Y%m%d%H%M%S")
                file_dt = datetime(file_dt.year, file_dt.month, file_dt.day, file_dt.hour)
                if f not in open_filenames:
                    candidate_files.append( (f,file_dt,) )
                else:
                    open_files.append( (f, file_dt,) )
                    
            # put any files from candidate list which have same hour as a file in open list
            # into the final_candidate_files hash
            premature_files = []
            final_candidate_files = {}
            for (cf,cf_dt) in candidate_files:
                matched = False
                for (of, of_dt) in open_files:
                    if of_dt == cf_dt: matched = True

                if not matched:
                    if cf_dt not in final_candidate_files:
                        final_candidate_files[cf_dt] = []
                    final_candidate_files[cf_dt].append(cf)
                else:
                    premature_files.append( (cf, cf_dt,) )
                        
            # get list of file times in order
            times = sorted(final_candidate_files.keys())
            
            for fc_time in times:
                ## move the files
                processing_files = []
                for fname in final_candidate_files[fc_time]:
                    old_filename = "%s/%s/collecting/%s" % (config.base_prefix, config.group, fname)
                    new_filename = "%s/%s/processing/%s" % (config.base_prefix, config.group, fname)
                    os.rename(old_filename, new_filename)
                    processing_files.append(new_filename)

                ## create a thread to manage the reduction of the files
                reduce_thread = threading.Thread(target=reduce_files, args=(config, fc_time, processing_files,))
                reduce_thread.start()
#reduce_threads[fc_time] = reduce_thread
                last_rotation = fc_time
            
        time.sleep(20)

def read_configuration(args):
    global procmonInstallBase

    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('-f', '--config', help="Specify configuration file instead of default at $PROCMON_DIR/etc/procmonManager.conf", default='%s/etc/procmonManager.conf' % procmonInstallBase, metavar="FILE")
    args, remaining_args = parser.parse_known_args()
    defaults = {
        "num_procmuxers": 2,
        "procMuxerPath": "%s/sbin/ProcMuxer" % procmonInstallBase,
        "reducer_path": "%s/sbin/PostReducer" % procmonInstallBase,
        "metadata_path": "%s/sbin/CheckH5" % procmonInstallBase,
        "base_pid_path": "/tmp/pid",
        "base_prefix": "/tmp",
        "h5_path": "%s/var/procmon" % procmonInstallBase,
        "h5_prefix": "procmon",
        "daemonize": False,
        "target_scratch": None,
        "email_list": None,
        "email_originator": None,
        "use_email": False,
        "use_jamo": False,
        "jamo_url": None,
        "jamo_token": None,
        "jamo_user": None,
    }
    if args.config and os.path.exists(args.config):
        config = SafeConfigParser()
        config.read([args.config])
        new_defaults = dict(config.items("procmonManager"))
        for key in new_defaults:
            if key in defaults:
                defaults[key] = new_defaults[key]

    parser = argparse.ArgumentParser(parents=[parser])
    parser.set_defaults(**defaults)
    parser.add_argument("--num_procmuxers", help="Number of procMuxers (listeners) to run", type=int)
    parser.add_argument("--group", help="Management group of muxers", type=str)
    parser.add_argument("--procMuxerPath", help="Path to ProcMuxer", type=str)
    parser.add_argument("--reducer_path", help="Path to PostReducer", type=str)
    parser.add_argument("--metadata_path", help="Path to CheckH5", type=str)
    parser.add_argument("--base_pid_path", help="Directory for pidfiles", type=str)
    parser.add_argument("--base_prefix", help="Local storage for data collection and processing", type=str)
    parser.add_argument("--h5_path", help="Search path for h5 files", type=str)
    parser.add_argument("--h5_prefix", help="Prefix for h5 file names (e.g., h5-path/<prefix>.YYYYMmddhHMMSS.h5)")
    parser.add_argument("--target_scratch", help="Path for scratch products", type=str)
    parser.add_argument("--email_list", help="Comma seperated list of people to email about procmonManager", type=split_comma)
    parser.add_argument("--email_originator", help="'From' email address", type=str)
    parser.add_argument("--use_jamo", help="Use Jamo (or Not)", type=is_True)
    parser.add_argument("--jamo_url", help="URL for JAMO", type=str)
    parser.add_argument("--jamo_token", help="Token for JAMO", type=str)
    parser.add_argument("--jamo_user", help="username for jamo user", type=str)
    parser.add_argument("--use_email", help="Use Email for warnings/errors (or Not)", type=is_True)
    parser.add_argument("--daemonize", help="Daemonize the manager process", type=is_True)
    args = parser.parse_args(remaining_args)
    return args

def daemonize():
    pid = None
    sid = None
    if os.getppid() == 1:
        # already daemonized
        return

    pid = os.fork()
    if pid < 0:
        sys.stderr.write("Failed to fork! Bailing out.\n");
        sys.exit(1)
    elif pid > 0:
        # this is the parent, exit out
        sys.exit(0)
    os.umask(022)
    os.chdir("/")
    devnull = open(os.devnull, "rw")
    for fd in (sys.stdin, sys.stdout, sys.stderr):
        fd.close()
        fd = devnull

if __name__ == "__main__":
    config = read_configuration(sys.argv[1:])
    print config
    if config.use_jamo:
        import sdm_curl
        config.sdm      = sdm_curl.Curl(config.jamo_url, appToken=config.jamo_token)
        config.sdm_lock = threading.Lock()
    if config.daemonize:
        daemonize()
    try:
        main_loop(config)
    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        str_exc = traceback.format_exc()
        str_tb = '\n'.join(traceback.format_tb(exc_traceback))
        str_stack2 = '\n'.join(traceback.format_stack())
        print '%s\n%s\n%s\n' % (str_exc, str_tb, str_stack2)
        send_email(config, 'PROCMON FAILURE', '%s\n%s\n%s\n' % (str_exc, str_tb, str_stack2))
    