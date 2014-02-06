import os
import re
from datetime import datetime, timedelta

class H5Cache:
    def __init__(self, basePath, h5prefix, cache_timeout=300):
        self.localcache = []
        self.__cache_time = None
        self.__cache_timeout = cache_timeout
        self.__basePath = basePath
        self.__h5prefix = h5prefix
        self.__filesystem_query_setup(basePath, h5prefix)

    def query(self, start, end):
        ret = []
        if (datetime.now() - self.__cache_time).total_seconds() > self.__cache_timeout:
            self.__filesystem_query_setup(self.__basePath, self.__h5prefix)
        for rec in self.localcache:
            if ((rec['recording_start'] <= start and rec['recording_stop'] >= start) or
                    (rec['recording_start'] <= end and rec['recording_stop'] >= end) or
                    (rec['recording_start'] >= start and rec['recording_stop'] <= end)):

                ret.append(rec)

        #if len(ret) == 0:
        #    start = start.strftime("%Y-%m-%dT%H:%M:%S")
        #    end = end.strftime("%Y-%m-%dT%H:%M:%S")
        #    return self.__remote_query(start, end)
        return ret
            
#def __remote_query(self, start, end):
#        ret = []
#        remote_files = sdm.post('api/metadata/query',data={'file_type':'procmon_reduced_h5',
#            '$or': [
#                {'metadata.procmon.recording_start':{'$lte':start},'metadata.procmon.recording_stop':{'$gte':start}},
#                {'metadata.procmon.recording_start':{'$lte':end},'metadata.procmon.recording_stop':{'$gte':end}},
#                {'metadata.procmon.recording_start':{'$gte':start},'metadata.procmon.recording_stop':{'$lte':end}},
#            ]
#        })
#        for record in remote_files:
#            newrec = {}
#            newrec['recording_start'] = datetime.strptime(record['metadata']['procmon']['recording_start'], "%Y-%m-%dT%H:%M:%S.%f")
#            newrec['recording_stop'] = datetime.strptime(record['metadata']['procmon']['recording_stop'], "%Y-%m-%dT%H:%M:%S.%f")
#            newrec['data'] = record
#            newrec['path'] = '%s/%s' % (record['file_path'],record['file_name'])
#            self.localcache.append(newrec)
#            ret.append(newrec)
#        return ret

    def __filesystem_query_setup(self, basePath, h5prefix):
        # invalidate cache
        self.localcache = []

        if type(basePath) == str:
            basePath = [basePath]
        for path in basePath:
            files = os.listdir(path)
            procmonMatcher = re.compile(r'%s.([0-9]+).h5' % h5prefix)
            for filename in files:
                procmonMatch = procmonMatcher.match(filename)
                if procmonMatch is not None:
                    l_date = procmonMatch.groups()[0]
                    date = datetime.strptime(l_date, '%Y%m%d%H%M%S')
                    newrec = {}
                    newrec['recording_start'] = date
                    newrec['recording_stop'] = date + timedelta(hours=1)
                    newrec['path'] = '%s/%s' % (path, filename)
                    newrec['data'] = None
                    self.localcache.append(newrec)

        revsorted = sorted(self.localcache, key=lambda k: k['recording_start'])[::-1]
        for (idx,item) in enumerate(revsorted):
            if idx == 0:
                continue
            item['recording_stop'] = revsorted[idx-1]['recording_start'] - timedelta(seconds=1)
        self.__cache_time = datetime.now()
