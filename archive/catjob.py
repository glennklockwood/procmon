#!/usr/bin/env python
import os
import sys
from datetime import datetime

fields = ("timestamp","timedelta","pid","state","ppid","pgrp","session","tty","ttygid","flags","minorFaults","cminorFaults","majorFaults","cmajorFaults","utimeTicks","stimeTicks","cutimeTicks","cstimeTicks","priority","nice","numThreads","itrealvalue","starttime","vsize","rss","rsslim","vpeak","rsspeak","startcode","endcode","startstack","kesp","keip","signal","blocked","sigignore","sigcatch","wchan","nswap","cnswap","exitSignal","processor","cpusAllowed","rtpriority","policy","guestTimeTicks","cguestTimeTicks","blockIODelayTicks","io_rchar","io_wchar","io_syscr","io_syscw","io_readBytes","io_writeBytes","io_cancelledWriteBytes","m_size","m_resident","m_share","m_text","m_data","ticksPerSec","execName","execPath","cwd")
fieldHash = { x:idx for (idx,x) in enumerate(fields) }

def usage(ret):
	print "catjob [procmon log file]"
	sys.exit(ret)

def parseTimestamp(str_ts):
	ts = datetime.strptime(str_ts, "%Y-%m-%d %H:%M:%S.%f")
	return ts

def parseStartTimestamp(str_ts):
	ts = datetime.fromtimestamp(float(str_ts))
	return ts

def formatBytes(byteVal):
	exp = 0
	fmt = float(byteVal)
	unitString = " kMGTPEZY"
	while fmt > 1000:
		fmt /= 1024
		exp += 1
	
	return "%1.2f%cB" % (fmt, unitString[exp])
	

class procMonData:
	def __init__(self, proclog):
		self.data = {}
		self.aggregateSum_accumulator = {}
		self.aggregateSum = {}
		self.aggregateVal = {}
		self.proclogFilename = proclog
		self.roots = []
		self.earliestStart = None
		self.maxObsTime = None


	def storeAggregate(self, key, aggKey, value):
		if aggKey not in self.aggregateVal or self.aggregateVal[aggKey][0] < value:
			self.aggregateVal[aggKey] = (value,key,)
		if aggKey not in self.aggregateSum_accumulator:
			self.aggregateSum_accumulator[aggKey] = value
		else:
			self.aggregateSum_accumulator[aggKey] += value

	def calcRate(self, key, value, currTime, rateKey, countKey, storeAggregateCounter=False):
		if countKey not in self.data[key]:
			self.data[key][countKey] = [ 0, ]
		lastValue = self.data[key][countKey][-1]
		delta = value - lastValue
		timeDelta = (currTime - self.data[key]["lastObsTime"]).total_seconds()
		rate = None
		if timeDelta > 0:
			rate = delta / timeDelta

		if rateKey not in self.data[key]:
			self.data[key][rateKey] = []

		self.data[key][rateKey].append(rate)
		self.data[key][countKey].append(value)

		self.storeAggregate(key, rateKey, rate)
		if storeAggregateCounter:
			self.storeAggregate(key, countKey, value)

	def counter(self, key, value, countKey):
		if countKey not in self.data[key]:
			self.data[key][countKey] = [ 0, ]

		self.data[key][countKey].append(value)
		self.storeAggregate(key, countKey, value)

	def parseProcLog(self):
		fp = None
		try:
			fp = open(self.proclogFilename, "r")
		except Exception, e:
			print "Failed to open file %s with error %s" % (proclog, e)
			sys.exit(2)
	
		#2013-02-26 17:58:28.466,0.003013,32489,S,32429,32489,32489,0,-1,4202752,2115,22781,0,0,2,1,14,6,25,5,1,0,1361930308.060,9338880,1323008,18446744073709551615,9338880,1323008,4194304,5082140,140736078770832,140736078768344,47785072724080,0,0,4,65536,0,0,0,17,2,0,0,0,0,0,1,885515,31283,922,63,4096,40960,0,9338880,1323008,1073152,888832,258048,100,(bash),/bin/bash,/global/u2/j/jasmynp
		conversion = (parseTimestamp, float, int, str, int, int, int, int, int, int, int, int, int, int, int, int, int, int, int, int, int, int, parseStartTimestamp, int, int, int, int, int, int, int, int, int, int, int, int, int, int, int, int, int, int, int, int, int, int, int, int, int, int, int, int, int, int, int, int, int, int, int, int, int, int, str, str, str)
	
		processHash = self.data
		self.earliestStart = None
		self.maxObsTime = None
	
		lastObsTime = None
		obsIdx = 0
		for line in fp:
			str_data = line.strip().split(",")
			data = [ conversion[x](str_data[x]) for x in xrange(len(str_data)) ]

			if lastObsTime is None or lastObsTime != data[0]:
				obsIdx += 1
				for key in self.aggregateSum_accumulator:
					if key not in self.aggregateSum or self.aggregateSum[key][0] < self.aggregateSum_accumulator[key]:
						self.aggregateSum[key] = (self.aggregateSum_accumulator[key],lastObsTime,)
					self.aggregateSum_accumulator[key] = 0.

				lastObsTime = data[0]
			if self.maxObsTime is None or self.maxObsTime < data[0]:
				self.maxObsTime = data[0]

			key = (data[2], data[fieldHash["starttime"]])
			if key not in processHash:
				processHash[key] = {}
				processHash[key]["starttime"] = key[1]
				processHash[key]["firstObsIdx"] = obsIdx;
				processHash[key]["ppid"] = data[fieldHash["ppid"]]
				processHash[key]["pid"] = data[fieldHash["pid"]]
				processHash[key]["lastObsTime"] = key[1] # set to starttime to get the rates initialized correctly
				processHash[key]["execName"] = data[fieldHash["execName"]]
				processHash[key]["execPath"] = data[fieldHash["execPath"]]
				processHash[key]["cwd"] = data[fieldHash["cwd"]]
	
			allCPUs = float(data[fieldHash["utimeTicks"]] + data[fieldHash["stimeTicks"]]) / data[fieldHash["ticksPerSec"]]
			self.calcRate(key, allCPUs, data[0], "cpuRate", "cpuCount")
			self.calcRate(key, data[fieldHash["io_readBytes"]], data[0], "ioFsReadRate", "ioFsRead")
			self.calcRate(key, data[fieldHash["io_writeBytes"]], data[0], "ioFsWriteRate", "ioFsWrite")
			self.calcRate(key, data[fieldHash["io_rchar"]], data[0], "ioReadRate", "ioRead")
			self.calcRate(key, data[fieldHash["io_wchar"]], data[0], "ioWriteRate", "ioWrite")
			self.counter(key, data[fieldHash["numThreads"]], "threads")

			blockIOTime = float(data[fieldHash["blockIODelayTicks"]]) / data[fieldHash["ticksPerSec"]]
			self.calcRate(key, blockIOTime, data[0], "blockedIORate", "blockedIOCount")

			processHash[key]["vmemPeak"] = data[fieldHash["vpeak"]]
			processHash[key]["rssPeak"] = data[fieldHash["rsspeak"]]
	
			processHash[key]["lastObsTime"] = data[0]
			processHash[key]["obsDuration"] = processHash[key]["lastObsTime"] - processHash[key]["starttime"]

		for process in processHash:
			#print "checking process: ", process
			ppid = processHash[process]["ppid"]
			processHash[process]["parent"] = None
			if self.earliestStart is None or processHash[process]["starttime"] < self.earliestStart:
				self.earliestStart = processHash[process]["starttime"]
			for parentProcess in processHash:
				if ppid == parentProcess[0] and processHash[parentProcess]["starttime"] <= processHash[process]["starttime"]:
					processHash[process]["parent"] = parentProcess
					if "children" in processHash[parentProcess]:
						processHash[parentProcess]["children"].append(process)
					else:
						processHash[parentProcess]["children"] = [ process, ]
					break
			if processHash[process]["parent"] is None:
				print "found root: ", process
				self.roots.append(process)

		self.duration = (self.maxObsTime - self.earliestStart).total_seconds()
	
		
	def displayJob(self):
		for process in self.roots:
			self.displayProcess(process, 0)

	def displayProcess(self, process, ancestorIndent):
		prefix = ""
		proc = self.data[process]
		for i in xrange(ancestorIndent):
			prefix += " "

		startSec = (proc["starttime"] - self.earliestStart).total_seconds()
		duration = (proc["lastObsTime"] - proc["starttime"]).total_seconds()
		startPct = (startSec / self.duration)*100
		durationPct = (duration / self.duration)*100
		print "%s+-- \x1b[31;1m%s %s\x1b[0m  (%d) Start: %1.1fs (%1.2f%%);  Duration: %1.1fs (%s%1.2f%%%s); MaxThreads: %d" % (prefix, proc["execName"], proc["execPath"], proc["pid"], startSec, startPct, duration, "\x1b[31;1m" if durationPct >= 5 else "", durationPct, "\x1b[0m" if durationPct >= 5 else "", max(proc["threads"]))
		print "%s \--- CPU Time: %ss; Max CPU %%: %1.2f%%;  Mean CPU %%: %1.2f%%" % (prefix, proc["cpuCount"][-1], 100*max(proc["cpuRate"]), 100*sum(proc["cpuRate"])/len(proc["cpuRate"]) if len(proc["cpuRate"]) > 0 else float('nan'))
		print "%s |--- Virtual Memory Peak: %s; RSS Peak %s" % (prefix, formatBytes(proc["vmemPeak"]), formatBytes(proc["rssPeak"]))
		print "%s |--- IO read(): %s; IO write(): %s; FS Read*: %s; FS Write*: %s; Time Blocked on IO: %1.2fs (%1.2f%%)" % (prefix, formatBytes(proc["ioRead"][-1]), formatBytes(proc["ioWrite"][-1]), formatBytes(proc["ioFsRead"][-1]), formatBytes(proc["ioFsWrite"][-1]), proc["blockedIOCount"][-1], (100 * (proc["blockedIOCount"][-1] / duration)))
		if "children" in proc:
			proc["children"] = sorted(proc["children"], key=lambda child: child[1])
			for child in proc["children"]:
				self.displayProcess(child, ancestorIndent + 1)
		


if __name__ == "__main__":
	proclog = "procmon.log"
	idx = 1
	posIdx = 0
	while idx < len(sys.argv):
		if sys.argv[idx] in ("-h", "--help"):
			usage(0)
		elif posIdx == 0:
			proclog = sys.argv[idx]
			posIdx += 1

		idx += 1
	
	procData = procMonData(proclog)
	procData.parseProcLog()
	procData.displayJob()
