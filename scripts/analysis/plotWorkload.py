import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import h5py
import sys
import os
import workloadAnalysis_pd
import copy
import re

cutoff=0.005

def pieLabel(val):
	if val < cutoff*100:
		return ''
	return '%0.2f%%' % val

def formatLabel(val, unit):
	mem = ' KMGTPE'
	if unit == "bytes":
		div = 0
		while val > 1024:
			val /= 1024.
			div += 1
		if div >= len(mem):
			return "limit"
		return "%0.1f%sB" % (val, mem[div])
	return val

def main(args):
	config = workloadAnalysis_pd.Config(args).config
	dateRange = config.files[1]

	print config.files
	ref_data = {}
	fd = h5py.File(config.files[0], 'r')
	#for dataset in fd:
	#	print dataset
	#	dset = fd[dataset][:]
	#	for col in dset.dtype.names:
	#		if col.endswith("_count") or col.endswith("_sum"):
	#			thesum = np.sum(dset[col])
	#			if col not in ref_data:
	#				ref_data[col] = thesum
	#			if float(ref_data[col]) != float(thesum):
	#				print "ALARM! WARNING! ERROR! %s for %s doesn't match! " % (col, dataset) ,thesum, ref_data[col], type(thesum), type(ref_data[col])

	for analysis in config.analyses:
		name = analysis['name']
		rootdir = 'plots_%s' % name
		try:
			os.mkdir(rootdir)
		except OSError, e:
			pass  # it's ok if dir already exists

		for (axis_idx,axis_keys) in enumerate(analysis['axes']):
			axisdir = analysis['axisNames'][axis_idx]
			print "starting axis %s" % axisdir
			try:
				os.mkdir(os.path.join(rootdir, axisdir))
			except OSError, e:
				pass

			dataset = '%s_%s' % (name, analysis['axisNames'][axis_idx])
			dset = fd[dataset][:]
			ignore_rows = np.zeros(dset.size, dtype=bool)
			for key in axis_keys:
				ignore_rows |= dset[key] == "category_ignore"
			ignored = dset[ignore_rows]
			dset = dset[~ignore_rows]
			for key in axis_keys:
				bykeydir = 'by_%s' % key
				other_keys = copy.copy(axis_keys)
				other_keys.remove(key)
				mykeys = np.unique(dset[key])
				if key.endswith("Detail"):
					findCategories = np.vectorize(lambda x: bool(re.match('^category_.*', x)))
					mykeys = mykeys[ findCategories(mykeys) ]

				makeLabels = np.vectorize(lambda x: ':'.join(list(x)).replace('category_',''))
				try:
					os.mkdir(os.path.join(rootdir, axisdir, bykeydir))
				except OSError, e:
					pass

				for s in config.summaries:
					sname = s['identifier']
					displayname = sname.replace("/","_")
					dset.sort(order='%s_sum' % sname)
					dset = dset[::-1]
					units = s['units']

					for mykey in mykeys:
						pngname = os.path.join(rootdir, axisdir, bykeydir, '%s_%s.png' % (mykey.replace("/","_"), displayname))
						if os.path.exists(pngname):
							continue
						mask = dset[key] == mykey

						labels = makeLabels(dset[mask][other_keys])
						sdata = dset[mask]['%s_sum' % sname] / np.sum(dset[mask]['%s_sum' % sname])
						labels[ sdata < cutoff ] = ''
						bigmask = sdata > 0.025

						fullLabels = []
						idx = 0
						while idx < labels.size:
							fullLabels.append( '%s\nN=%d\nVal=%0.2e%s' % (labels[idx], dset[mask]['%s_count' % sname][idx], dset[mask]['%s_sum' % sname][idx], units))
							idx += 1
						fullLabels = np.array(fullLabels)
						fullLabels[~bigmask] = labels[~bigmask]

						cmap = plt.cm.brg
						colors = cmap(np.linspace(0., 1., len(sdata)))[::-1]
						colors = cmap(np.hstack((0,np.cumsum(sdata)))[0:-1])
						title = "%s:%s:%s:%s:%s" % (name, axisdir, key, mykey, sname)
						fig = plt.figure(figsize=(10,10))
						ax = fig.add_subplot(111)
						try:
							pie = ax.pie(sdata, colors=colors, labels=fullLabels, autopct=pieLabel, labeldistance=1.05, pctdistance=0.85)
							for wedge in pie[0]:
								wedge.set_edgecolor('white')
							#ax.set_title(title)
							fig.text(0.5, 0.95, title, fontsize=18, ha='center')
							fig.text(0.5, 0.92, "Date Range: %s, Total Processes: %d" % (dateRange, np.sum(dset[mask]['%s_count' % sname])), fontsize=12, ha='center')
							fig.savefig(pngname)
						except ValueError:
							print "failing to make %s, bad values: " % title, sdata
						plt.close()
			try:
				os.mkdir(os.path.join(rootdir, axisdir, 'histograms'))
			except OSError, e:
				pass

			allow_mask = np.ones(dset.size, dtype=bool)
			for key in axis_keys:
				key_mask = np.zeros(dset.size, dtype=bool)
				if key.endswith('Detail'):
					findCategories = np.vectorize(lambda x: bool(re.match('^category_.*', x)))
					key_mask = findCategories(dset[key])
				else:
					key_mask = np.ones(dset.size, dtype=bool)
				allow_mask &= key_mask
			hist_data = dset[allow_mask]
			kidx = 0
			print "starting histograms!"
			while kidx < hist_data.size:
				key = ':'.join(list(hist_data[axis_keys][kidx])).replace('category_','')
				#dim1 = len(config.summaries)/2 + len(config.summaries)%2
				#dim2 = 2
				for (sidx, s) in enumerate(config.summaries):
					histfile = os.path.join(rootdir, axisdir, 'histograms', '%s_%s.png' % (key.replace('/','_'), s['identifier']))
					if os.path.exists(histfile):
						continue
					fig = plt.figure(figsize=(7,5))
					labels = [ formatLabel(x, s['units']) for x in s['bins'] ]
					ax = fig.add_subplot(1,1,1)
					indices = np.arange(len(s['bins'])-1)
					ax.bar(indices, hist_data['%s_histogram' % s['identifier']][kidx], 0.35)
					title = '%s: %s histogram' % (key, s['identifier'])
					fig.text(0.5, 0.95, title, fontsize=15, ha='center')
					fig.text(0.5, 0.90, "Date Range: %s, Total Processes: %d" % (dateRange, np.sum(hist_data['%s_histogram' % s['identifier']][kidx])), fontsize=11, ha='center')
					fig.text(0.5, 0.07, "%s (%s)" % (s['identifier'], s['units']))
					fig.text(0.05, 0.5, "Count", rotation=90)
					ax.set_xticks(np.arange(len(s['bins'])))
					ax.set_xticklabels(labels, fontsize=9, rotation=45)
					fig.subplots_adjust(bottom=0.2, top=0.84)
					fig.savefig(histfile)
					plt.close()
				kidx += 1



	fd.close()

if __name__ == "__main__":
	main(sys.argv[1:])
