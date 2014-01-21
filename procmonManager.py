#!/usr/bin/env python

import os
import sys
import subprocess
from datetime import datetime, timedelta
import time
import socket
import sdm_curl ## for JAMO
import threading
import json
import traceback
import errno
import re
import shutil

num_procmuxers = 3
group = "procman_prod"
procMuxerPath     = "%s/genepool/procmon/ProcMuxer" % (os.environ['HOME'])
reducer_path      = "%s/genepool/procmon/PostReducer" % (os.environ['HOME'])
metadata_path     = "%s/genepool/procmon/CheckH5" % (os.environ['HOME'])
system_name       = "genepool"
base_pid_path     = "/scratch/procmon/pids"
base_prefix       = "/scratch/procmon"
target_production = "/global/projectb/statistics/procmon/genepool"
target_scratch    = "/global/projectb/scratch/dmj/procmon_raw_data"
email_list        = ('dmj@nersc.gov',)
email_originator  = 'procmon@nersc.gov'

jamo_url          = "https://sdm2.jgi-psf.org"
jamo_url_dev      = "https://sdm-dev.jgi-psf.org:8034"
jamo_token        = "5M7B1LX84WGL2YX1385YDKBU5EN4PF1Q"
jamo_token_dev    = "IST8JHUB9CAP0DSEWGGDIV042SYLJ1IY"
jamo_user         = "jgi_dna"
sdm               = sdm_curl.Curl(jamo_url, appToken=jamo_token)
sdm_lock          = threading.Lock()

def send_email(subject, message):
    import smtplib
    message = """From: %s
To: %s
Subject: MESSAGE FROM PROCMON: %s

%s
""" % (email_originator, ", ".join(email_list), subject, message)

    try:
        smtp_message = smtplib.SMTP('localhost')
        smtp_message.sendmail(email_originator, email_list, message)
    except smtplib.SMTPException:
        print "Error: failed to send email!"

def get_exception():
    exc_type, exc_value, exc_traceback = sys.exc_info()
    str_exc = traceback.format_exc()
    str_tb = '\n'.join(traceback.format_tb(exc_traceback))
    str_stack2 = '\n'.join(traceback.format_stack())
    return '%s\n%s\n%s\n' % (str_exc, str_tb, str_stack2)

def start_procMuxer(group=None, id=None, prefix=None, pidfile=None):
    args = [procMuxerPath, '-c', '60', '-d']
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

def get_muxer_pid(muxer_id):
    pidfilename = "%s/%s/%d" % (base_pid_path, group, muxer_id)
    current_pid = None
    if os.path.exists(pidfilename):
        try:
            fd = open(pidfilename, 'r')
            for line in fd:
                current_pid = int(line.strip())
        except:
            pass
    return current_pid

def is_muxer_running(muxer_id):
    current_pid = get_muxer_pid(muxer_id)

    if type(current_pid) is int:
        procfile = "/proc/%d/status" % current_pid
        return os.path.exists(procfile)
    return False

def get_current_files(muxer_id):
    current_pid = get_muxer_pid(muxer_id)
    files = []
    if type(current_pid) is int:
        for fdnum in xrange(3,10):
            fdpath = "/proc/%d/fd/%d" % (current_pid, fdnum)
            if os.path.exists(fdpath):
                filename = os.readlink(fdpath)
                if re.search('%s.*procMuxer\.%d' % (group, muxer_id), filename):
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

def register_jamo(fname, ftype, sources = None):
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
        send_email("failed to set acl", fname)
        return None

    md_proc = subprocess.Popen([metadata_path, '-i', fname], stdout=subprocess.PIPE)
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
        send_email("failed to read file stats", fname)
        return None
    
    posted = None
    if sources is None:
        sources = []
    with sdm_lock:
        posted = sdm.post('api/metadata/file',
                file=fname,
                file_type=ftype,
                local_purge_days=local_purge_days,
                backup_services=tape_archival,
                inputs=sources,
                metadata=md_final,
        )
    if posted is None or 'metadata_id' not in posted:
        send_email("failed to register with jamo", "%s\n%s\n" % (fname, ftype, ))
    return posted


def reduce_files(timeobj, filenames):
    """Runs the reducer on the files.  Then moves files to final destination,
       sets proper permissions, then registers the files with JAMO"""

    product_output = "%s/%s/processing/procmon_%s.%s.h5" % (base_prefix, group, system_name, timeobj.strftime("%Y%m%d%H%M%S"))
    bad_output = "%s/%s/processing/bad_procmon_%s.%s.h5" % (base_prefix, group, system_name, timeobj.strftime("%Y%m%d%H%M%S"))
    reducer_args = [reducer_path, '-o', product_output, '-b', bad_output]
    for f in filenames:
        reducer_args.extend(['-i', f])
    retval = subprocess.call(reducer_args, stdin=None, stdout=None, stderr=None)
    if retval != 0:
        send_email("reducer failed!", "retcode: %d\ncmd: %s" % (retval, " ".join(reducer_args)))
        return 1
    (currpath, product_fname) = os.path.split(product_output)
    (currpath, bad_fname) = os.path.split(bad_output)

    final_product = '%s/%s' % (target_production, product_fname)
    final_badoutput = '%s/%s' % (target_scratch, bad_fname)

    sources = []
    for f in filenames:
        (somepath,fname) = os.path.split(f)
        newpath = "%s/%s" % (target_scratch, fname)
        try:
            shutil.move(f, newpath);
            os.chmod(newpath, 0444)
        except:
            send_email("reducer failed to move file", "%s\n%s\n%s\n" % (f, newpath, get_exception()))
            return 1
        response = register_jamo(newpath, "procmon_stripe_h5")
        if 'metadata_id' in response:
            sources.append(response['metadata_id'])


    try:
        shutil.move(product_output, final_product)
        os.chmod(final_product, 0444)
    except:
        send_email("reducer failed to move file", "%s\n%s\n%s\n" % (product_output, final_product, get_exception()))
        return 1

    register_jamo(final_product, "procmon_reduced_h5", sources)

    try:
        shutil.move(bad_output, final_badoutput)
        os.chmod(final_badoutput, 0444)
    except:
        send_email("reducer failed to move file", "%s\n%s\n%s" % (bad_output, final_badoutput, get_exception()))
        return 1

    register_jamo(final_badoutput, "procmon_badrecords_h5", sources)
        

def main_loop(args):
    # create pid directory
    mkdir_p("%s/%s" % (base_pid_path, group))

    # create working directory
    mkdir_p("%s/%s" % (base_prefix, group))
    mkdir_p("%s/%s/processing" % (base_prefix, group))
    mkdir_p("%s/%s/collecting" % (base_prefix, group))
    os.chdir("%s/%s" % (base_prefix, group))

    file_prefix = "%s/%s/collecting/procMuxer" % (base_prefix, group)

    last_rotation = None

    send_email("%s starting" % group, "starting management of %s ProcMuxer group on %s" % (group, socket.gethostname()))
    ## enter into perpetual loop
    reduce_threads = {}
    while True:
        ## check if the muxers are running, if not, restart them
        for muxer_id in xrange(num_procmuxers):
            if not is_muxer_running(muxer_id):
                start_procMuxer(group=group, id=muxer_id,
                        prefix="%s.%d" % (file_prefix, muxer_id),
                        pidfile="%s/%s/%d" % (base_pid_path, group, muxer_id)
                )

        ## if more than an hour has elapsed since the last successful
        ## rotation of log files, then check 
        if (not last_rotation) or ((datetime.now() - last_rotation).total_seconds() > 3600):
            ## get list of currently open files
            open_filenames = []
            for muxer_id in xrange(num_procmuxers):
                files = get_current_files(muxer_id)
                for f in files:
                    (path,fname) = os.path.split(f)
                    if fname: open_filenames.append(fname)

            ## get list of files in collecting, filter out current files and non-targets
            ## put into candidate_files
            files = os.listdir('%s/%s/collecting' % (base_prefix, group))
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
                    old_filename = "%s/%s/collecting/%s" % (base_prefix, group, fname)
                    new_filename = "%s/%s/processing/%s" % (base_prefix, group, fname)
                    os.rename(old_filename, new_filename)
                    processing_files.append(new_filename)

                ## create a thread to manage the reduction of the files
                reduce_thread = threading.Thread(target=reduce_files, args=(fc_time, processing_files,))
                reduce_thread.start()
#reduce_threads[fc_time] = reduce_thread
                last_rotation = fc_time
            
        time.sleep(20)

if __name__ == "__main__":
    try:
        main_loop(sys.argv)
    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        str_exc = traceback.format_exc()
        str_tb = '\n'.join(traceback.format_tb(exc_traceback))
        str_stack2 = '\n'.join(traceback.format_stack())
        print '%s\n%s\n%s\n' % (str_exc, str_tb, str_stack2)
        send_email('PROCMON FAILURE', '%s\n%s\n%s\n' % (str_exc, str_tb, str_stack2))
    
