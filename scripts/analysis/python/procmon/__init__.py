import numpy as np
import re
from StringIO import StringIO
from procmon import H5Cache
from procmon import H5Parser

__all__ = ['H5Cache','Scriptable','H5Parser']

procdata_dt = np.dtype([
        ('pid', np.uint),
        ('ppid', np.uint),
        ('recTime', np.uint64),
        ('recTimeUSec', np.uint64),
        ('startTime', np.uint64),
        ('startTimeUSec', np.uint64),
        ('execName', 'S256'),
        ('cmdArgBytes', np.uint64),
        ('cmdArgs', 'S1024'),
        ('exePath', 'S1024'),
        ('cwdPath', 'S1024'),
])

procfd_dt = np.dtype([
        ('pid', np.uint),
        ('ppid', np.uint),
        ('recTime', np.uint64),
        ('recTimeUSec', np.uint64),
        ('startTime', np.uint64),
        ('startTimeUSec', np.uint64),
        ('path', 'S1024'),
        ('fd', np.uint),
        ('mode', np.uint),
])

procobs_dt = np.dtype([
        ('pid', np.uint),
        ('recTime', np.uint64),
        ('recTimeUSec', np.uint64),
        ('startTime', np.uint64),
        ('startTimeUSec', np.uint64),
])

procstat_dt = np.dtype([
        ('pid', np.uint),
        ('recTime', np.uint64),
        ('recTimeUSec', np.uint64),
        ('startTime', np.uint64),
        ('startTimeUSec', np.uint64),
        ('state', 'S1'),
        ('ppid', np.uint),
        ('pgrp', np.int),
        ('session', np.int),
        ('tty', np.int),
        ('tpgid', np.int),
        ('flags', np.uint),
        ('utime', np.uint64),
        ('stime', np.uint64),
        ('priority', np.int64),
        ('nice', np.int64),
        ('numThreads', np.int64),
        ('vsize', np.uint64),
        ('rss', np.uint64),
        ('rsslim', np.uint64),
        ('vmpeak', np.uint64),
        ('rsspeak', np.uint64),
        ('signal', np.uint64),
        ('blocked', np.uint64),
        ('sigignore', np.uint64),
        ('sigcatch', np.uint64),
        ('cpusAllowed', np.int),
        ('rtPriority', np.uint),
        ('policy', np.uint),
        ('guestTime', np.uint),
        ('delayacctBlkIOTicks', np.uint64),
        ('io_rchar', np.uint64),
        ('io_wchar', np.uint64),
        ('io_syscr', np.uint64),
        ('io_syscw', np.uint64),
        ('io_readBytes', np.uint64),
        ('io_writeBytes', np.uint64),
        ('io_cancelledWriteBytes', np.uint64),
        ('m_size', np.uint64),
        ('m_resident', np.uint64),
        ('m_share', np.uint64),
        ('m_text', np.uint64),
        ('m_data', np.uint64),
        ('realUid', np.uint64),
        ('effUid', np.uint64),
        ('realGid', np.uint64),
        ('effGid', np.uint64),
])

def parse_procstat(message):
        idx = message.find('\n')
        cfd = StringIO(message[(idx+1):])
        return np.loadtxt(cfd, dtype=procstat_dt, delimiter=',')

def parse_procdata(message):
        idx = message.find('\n')
        rows = None
        while (True):
            if idx >= len(message)-1:
                break
            nidx = message.find(',', idx+1)
            pid = int(message[idx+1:nidx])
            idx = nidx+1
            nidx = message.find(',', idx)
            ppid = int(message[idx:nidx])
            idx = nidx+1
            nidx = message.find(',', idx)
            recTime = int(message[idx:nidx])
            idx = nidx+1
            nidx = message.find(',', idx)
            recTimeUSec = int(message[idx:nidx])
            idx = nidx+1
            nidx = message.find(',', idx)
            startTime = int(message[idx:nidx])
            idx = nidx+1
            nidx = message.find(',', idx)
            startTimeUSec = int(message[idx:nidx])
            idx = nidx+1
            nidx = message.find(',', idx)
            readbytes = int(message[idx:nidx])
            idx = nidx+1
            nidx = idx + readbytes
            execName = message[idx:nidx]
            idx = nidx+1
            nidx = message.find(',', idx)
            readbytes = int(message[idx:nidx])
            cmdArgBytes = readbytes
            idx = nidx+1
            nidx = idx + readbytes
            cmdArgs = message[idx:nidx]
            idx = nidx+1
            nidx = message.find(',', idx)
            readbytes = int(message[idx:nidx])
            idx = nidx+1
            nidx = idx + readbytes
            exePath = message[idx:nidx]
            idx = nidx+1
            nidx = message.find(',', idx)
            readbytes = int(message[idx:nidx])
            idx = nidx+1
            nidx = idx + readbytes
            cwdPath = message[idx:nidx]
            idx = nidx
            row = np.array((pid, ppid, recTime, recTimeUSec, startTime, startTimeUSec, execName, cmdArgBytes, cmdArgs, exePath, cwdPath,), dtype=procdata_dt)
            rows = row if rows is None else np.append(rows, row)
        return rows

def parse_procfd(message):
        idx = message.find('\n')
        rows = None
        while (True):
            if idx >= len(message)-1:
                break
            nidx = message.find(',', idx+1)
            pid = int(message[idx+1:nidx])
            idx = nidx+1
            nidx = message.find(',', idx)
            ppid = int(message[idx:nidx])
            idx = nidx+1
            nidx = message.find(',', idx)
            recTime = int(message[idx:nidx])
            idx = nidx+1
            nidx = message.find(',', idx)
            recTimeUSec = int(message[idx:nidx])
            idx = nidx+1
            nidx = message.find(',', idx)
            startTime = int(message[idx:nidx])
            idx = nidx+1
            nidx = message.find(',', idx)
            startTimeUSec = int(message[idx:nidx])
            idx = nidx+1
            nidx = message.find(',', idx)
            readbytes = int(message[idx:nidx])
            idx = nidx+1
            nidx = idx + readbytes
            thepath = message[idx:nidx]
            idx = nidx+1
            nidx = message.find(',', idx)
            fdnum = int(message[idx:nidx])
            idx = nidx+1
            nidx = message.find('\n', idx)
            fdmode = int(message[idx:nidx])
            idx = nidx
            row = np.array((pid, ppid, recTime, recTimeUSec, startTime, startTimeUSec, thepath, fdnum, fdmode,), dtype=procfd_dt)
            rows = row if rows is None else np.append(rows, row)
        return rows

def merge_procs(procstat, procdata):
    joined = procdata.join(procstat, rsuffix='_ps')
    del joined['cmdArgBytes']
    del joined['pgrp']
    del joined['startTime_ps']
    del joined['startTimeUSec_ps']
    del joined['recTime_ps']
    del joined['recTimeUSec_ps']
    del joined['pid_ps']
    del joined['ppid_ps']
    del joined['session']
    del joined['tty']
    del joined['tpgid']
    del joined['flags']
    del joined['signal']
    del joined['blocked']
    del joined['sigignore']
    del joined['sigcatch']
    del joined['cpusAllowed']
    del joined['rtPriority']
    del joined['policy']
    del joined['guestTime']
    del joined['effUid']
    del joined['effGid']
    return joined


if __name__ == "__main__":
    myrecs = """nRecords=17
19681,1379310290,679874,1379310262,410000,S,19669,19681,19681,0,-1,4202752,10,3,25,5,1,29302784,8273920,18446744073709551615,29302784,8273920,0,0,128,8405504,0,0,0,0,1,5054431,790988,3185,324,2048,24576,0,29302784,8273920,2072576,1490944,6447104,13662,13662,13662,13662
19790,1379310290,679874,1379310263,359999,R,1,19790,19790,0,-1,4202560,4,9,25,5,1,21897216,1490944,18446744073709551615,21897216,1490944,0,0,0,8407554,0,0,0,0,0,0,0,0,0,0,0,0,21897216,1490944,897024,634880,4173824,13662,13662,13662,13662
19902,1379310290,679874,1379310267,920000,S,19681,19681,19681,0,-1,4202496,0,0,25,5,1,9342976,1327104,18446744073709551615,9342976,1327104,0,65536,4,65538,0,0,0,0,0,4644,0,9,0,0,0,0,9342976,1327104,1077248,888832,262144,13662,13662,13662,13662
19903,1379310290,679874,1379310267,930000,S,19902,19681,19681,0,-1,4202496,5,1,25,5,1,33812480,8142848,18446744073709551615,33812480,8142848,0,0,128,0,0,0,0,0,0,396947,464,145,1,0,0,0,33812480,8142848,2187264,1490944,6176768,13662,13662,13662,13662
19904,1379310290,679874,1379310268,19999,S,19903,19681,19681,0,-1,4202496,17,3,25,5,1,50864128,16703488,18446744073709551615,50864128,16703488,0,0,134,0,0,0,0,0,0,1301863,31732,483,18,0,49152,12288,50864128,16703488,2396160,1490944,15585280,13662,13662,13662,13662
19913,1379310290,679874,1379310268,509999,S,19904,19681,19681,0,-1,4202496,0,0,25,5,1,9334784,1314816,18446744073709551615,9334784,1314816,0,65536,4,65538,0,0,0,0,0,4644,0,9,0,0,0,0,9334784,1314816,1069056,888832,253952,13662,13662,13662,13662
19914,1379310290,679874,1379310268,509999,S,19913,19681,19681,0,-1,4202496,0,0,25,5,1,9338880,1347584,18446744073709551615,9338880,1347584,0,65536,4,65538,0,0,0,0,0,11459,93,32,3,0,4096,0,9338880,1347584,1101824,888832,258048,13662,13662,13662,13662
19917,1379310290,679874,1379310268,519999,S,19914,19681,19681,0,-1,4202496,0,0,25,5,1,19165184,2060288,18446744073709551615,19165184,2060288,0,0,134,0,0,0,0,0,0,12073,0,17,0,0,0,0,19165184,2060288,1642496,1490944,630784,13662,13662,13662,13662
19918,1379310290,679874,1379310268,529999,S,19917,19681,19681,0,-1,4202496,19,2,25,5,1,49123328,16457728,18446744073709551615,49360896,16678912,0,0,134,0,0,0,0,0,0,1163226,21783,393,11,0,36864,4096,49123328,16457728,2801664,1490944,13844480,13662,13662,13662,13662
19919,1379310290,679874,1379310268,759999,S,19918,19681,19681,0,-1,4202496,0,0,25,5,1,9338880,1318912,18446744073709551615,9338880,1318912,0,65536,4,65538,0,0,0,0,0,4644,0,9,0,0,0,0,9338880,1318912,1069056,888832,258048,13662,13662,13662,13662
19920,1379310290,679874,1379310268,769999,S,19919,19681,19681,0,-1,4202496,0,0,25,5,1,9334784,1347584,18446744073709551615,9334784,1347584,0,65536,4,65538,0,0,0,0,0,8477,109,22,2,0,4096,0,9334784,1347584,1101824,888832,253952,13662,13662,13662,13662
19922,1379310290,679874,1379310268,769999,S,19920,19681,19681,0,-1,4202496,0,0,25,5,1,19165184,2064384,18446744073709551615,19165184,2064384,0,0,134,0,0,0,0,0,0,13001,0,17,0,0,0,0,19165184,2064384,1642496,1490944,630784,13662,13662,13662,13662
19923,1379310290,679874,1379310268,779999,S,19922,19681,19681,0,-1,4202496,10,1,25,5,1,31961088,10948608,18446744073709551615,31961088,10948608,0,0,134,0,0,0,0,0,0,1042907,9754,318,3,0,12288,0,31961088,10948608,1961984,1490944,9175040,13662,13662,13662,13662
19926,1379310290,679874,1379310268,930000,S,19923,19681,19681,0,-1,4202496,0,0,25,5,1,9338880,1318912,18446744073709551615,9338880,1318912,0,65536,4,65538,0,0,0,0,0,4644,0,9,0,0,0,0,9338880,1318912,1069056,888832,258048,13662,13662,13662,13662
19927,1379310290,679874,1379310268,930000,S,19926,19681,19681,0,-1,4202496,0,0,25,5,1,12308480,8949760,18446744073709551615,12840960,9482240,0,0,0,8,0,0,0,0,0,1056291,296,9,2,0,0,0,12308480,8949760,450560,868352,11362304,13662,13662,13662,13662
19928,1379310290,679874,1379310268,940000,S,19927,19681,19681,0,-1,4202560,0,0,25,5,1,12308480,8949760,18446744073709551615,12840960,9482240,0,2147221231,0,8,0,0,0,0,0,296,0,2,0,0,0,0,12308480,8949760,450560,868352,11362304,13662,13662,13662,13662
19929,1379310290,679874,1379310268,940000,R,19928,19681,19681,0,-1,4202560,2078,10,25,5,1,12308480,8949760,18446744073709551615,12840960,9482240,0,0,0,8,0,0,0,0,0,299892736,0,286,0,0,0,0,12308480,8949760,450560,868352,11362304,13662,13662,13662,13662
"""
    print parse_procstat(myrecs)
    myrecs = """nRecords=5
779,755,1379312091,832703,1379310805,339999,15,pipe:[44881263],3,41280
1017,1,1379312091,832703,1379310805,740000,17,socket:[44880846],3,41408
1279,1278,1379312091,832703,1379310806,220000,146,/global/projectb/sandbox/fungal/data/Trichoderma_longibrachiatum/Trilo3/splitModels.fgenesh1_pg.fid/Trichoderma_longibrachiatum.fgenesh1_pg.000145,3,41280
1279,1278,1379312091,832703,1379310806,220000,151,/global/projectb/sandbox/fungal/data/Trichoderma_longibrachiatum/Trilo3/Blastp/fgenesh1_pg/raw/Trichoderma_longibrachiatum.fgenesh1_pg.000145_vs_NR.out,4,41152
1279,1278,1379312091,832703,1379310806,220000,151,/global/projectb/sandbox/fungal/data/Trichoderma_longibrachiatum/Trilo3/Blastp/fgenesh1_pg/raw/Trichoderma_longibrachiatum.fgenesh1_pg.000145_vs_NR.out,5,41152
"""
    print myrecs
    print parse_procfd(myrecs)

    mydatarecs = """nRecords=6
7621,7567,1379315848,423463,1379211434,779999,12,run_randd.sh,163,/bin/bash|./run_randd.sh|jgi_gene_count.pl|-n|24|-s|r|-l|counts.log|--unique|--trim_max_align|--norm_gene|--norm_sample|-m|normal_counts.txt|config.txt|counts.txt,9,/bin/bash,109,/global/projectb/sandbox/rnaseq/projects/Cochliobolus_sativus_and_Brachypodium_distachyon_1018004/gene_counts
7730,1,1379315848,423463,1379211438,700000,7,procmon,109,/opt/uge/jgiutil/procmon|-f|30|-W|10|-i|30|--initialfrequency|2|-d|-p|7621|-g|37100|-G|37300|-I|6899036|-S|1,36,/genepool/GridEngine/jgiutil/procmon,1,/
7859,7621,1379315848,423463,1379211440,109999,4,perl,183,perl|/usr/common/jgi/jgibio/prod/DEFAULT/bin/jgi_gene_count.pl|-n|24|-s|r|-l|counts.log|--unique|--trim_max_align|--norm_gene|--norm_sample|-m|normal_counts.txt|config.txt|counts.txt,65,/house/tooldirs/jgitools/Linux_x86_64/perl5/5.10.1/bin/perl5.10.1,109,/global/projectb/sandbox/rnaseq/projects/Cochliobolus_sativus_and_Brachypodium_distachyon_1018004/gene_counts
25704,7859,1379315848,423463,1379314751,940000,2,sh,1024,sh|-c|bwa sampe ./Cocsa1_and_Bdistachyon_transcripts.noblank.fasta /global/projectb/sandbox/rnaseq/projects/Cochliobolus_sativus_and_Brachypodium_distachyon_1018004/gene_counts/7275.4.65877.CTTGTA.fastq.trim36.OSZO.Cocsa1_and_Bdistachyon_transcripts.noblank.fasta.bam.tmp.sai1 /global/projectb/sandbox/rnaseq/projects/Cochliobolus_sativus_and_Brachypodium_distachyon_1018004/gene_counts/7275.4.65877.CTTGTA.fastq.trim36.OSZO.Cocsa1_and_Bdistachyon_transcripts.noblank.fasta.bam.tmp.sai2 /global/projectb/sandbox/rnaseq/projects/Cochliobolus_sativus_and_Brachypodium_distachyon_1018004/gene_counts/7275.4.65877.CTTGTA.fastq.trim36.r1 /global/projectb/sandbox/rnaseq/projects/Cochliobolus_sativus_and_Brachypodium_distachyon_1018004/gene_counts/7275.4.65877.CTTGTA.fastq.trim36.r2 2> /dev/null | samtools view - -bS -o /global/projectb/sandbox/rnaseq/projects/Cochliobolus_sativus_and_Brachypodium_distachyon_1018004/gene_counts/7275.4.65877.CTTGTA.fastq.trim36.OSZO.Cocsa1_and_Bdistachyon_transcripts.noblank.fasta.bam  2> /,9,/bin/bash,109,/global/projectb/sandbox/rnaseq/projects/Cochliobolus_sativus_and_Brachypodium_distachyon_1018004/gene_counts
25705,25704,1379315848,423463,1379314752,0,3,bwa,773,bwa|sampe|./Cocsa1_and_Bdistachyon_transcripts.noblank.fasta|/global/projectb/sandbox/rnaseq/projects/Cochliobolus_sativus_and_Brachypodium_distachyon_1018004/gene_counts/7275.4.65877.CTTGTA.fastq.trim36.OSZO.Cocsa1_and_Bdistachyon_transcripts.noblank.fasta.bam.tmp.sai1|/global/projectb/sandbox/rnaseq/projects/Cochliobolus_sativus_and_Brachypodium_distachyon_1018004/gene_counts/7275.4.65877.CTTGTA.fastq.trim36.OSZO.Cocsa1_and_Bdistachyon_transcripts.noblank.fasta.bam.tmp.sai2|/global/projectb/sandbox/rnaseq/projects/Cochliobolus_sativus_and_Brachypodium_distachyon_1018004/gene_counts/7275.4.65877.CTTGTA.fastq.trim36.r1|/global/projectb/sandbox/rnaseq/projects/Cochliobolus_sativus_and_Brachypodium_distachyon_1018004/gene_counts/7275.4.65877.CTTGTA.fastq.trim36.r2,77,/house/tooldirs/jgitools/Linux_x86_64/misc_bio/bwa/versions/bwa-0.5.9/bin/bwa,109,/global/projectb/sandbox/rnaseq/projects/Cochliobolus_sativus_and_Brachypodium_distachyon_1018004/gene_counts
25706,25704,1379315848,423463,1379314752,0,8,samtools,224,samtools|view|-|-bS|-o|/global/projectb/sandbox/rnaseq/projects/Cochliobolus_sativus_and_Brachypodium_distachyon_1018004/gene_counts/7275.4.65877.CTTGTA.fastq.trim36.OSZO.Cocsa1_and_Bdistachyon_transcripts.noblank.fasta.bam,84,/house/tooldirs/jgitools/Linux_x86_64/misc_bio/samtools/versions/0.1.19/bin/samtools,109,/global/projectb/sandbox/rnaseq/projects/Cochliobolus_sativus_and_Brachypodium_distachyon_1018004/gene_counts
"""
    print parse_procdata(mydatarecs)
