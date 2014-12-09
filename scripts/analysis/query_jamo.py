import sdm_curl
from datetime import datetime
from datetime import timedelta
jamo_url          = "https://sdm2.jgi-psf.org"
jamo_token        = "5M7B1LX84WGL2YX1385YDKBU5EN4PF1Q"
sdm = sdm_curl.Curl(jamo_url, jamo_token)



def remote_query(sdm, year, month, day):
    ret = []
    remote_files = sdm.post('api/metadata/query',data={'file_type':'procmon_reduced_h5', 'file_name':{'$regex':'procmon_genepool.%04d%02d%02d.*' % (year, month, day)}})
#
#'$or': [
#                {'metadata.procmon.recording_start':{'$lte':start},'metadata.procmon.recording_stop':{'$gte':start}},
#                {'metadata.procmon.recording_start':{'$lte':end},'metadata.procmon.recording_stop':{'$gte':end}},
#                {'metadata.procmon.recording_start':{'$gte':start},'metadata.procmon.recording_stop':{'$lte':end}},
#        ]
#    })
    restore_list = []
    for rfile in remote_files:
        if rfile['file_status'] == "PURGED":
            print "getting read to request restore of %s" % rfile['file_name']
            restore_list.append(rfile['_id'])
    if len(restore_list) > 0:
        ret = sdm.post('api/tape/grouprestore',data={'files':restore_list,'days':30})
        print ret
            


year=2014
month=2
for day in xrange(1,32):
    remote_query(sdm, year, month, day)
    print 
