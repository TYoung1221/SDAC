import SDidentifier
import SDreader
import h5py as h5
import xarray as xr
import pyfits
import numpy as np
import adios as ad
import json

'''def get_num_of_workers(sc):
	b = sc._conf.getAll()
	for v in b[0][1]:
		if v.isdigit() == True:
			return v'''

def get_num_of_workers(sc):
	b = sc._conf.getAll()
	for v in b:
		if "spark.master" in v[0]:
			return filter(lambda x: x.isdigit(), v[1])

def sd_reader(sc, SDfile, chunk, partitions=None):
	num_workers = int(get_num_of_workers(sc))
	SD_setting = SDidentifier.file_identifier(SDfile, num_workers, chunk)
        RDDs = sc.parallelize(xrange(SD_setting[3]), partitions).map(lambda v: read_chunk(v, SD_setting, SDfile))
	return RDDs

class SD_Reader(object):
    def __init__(self, filename):
        self.filename = filename

    def __getitem__(self, item):
		sd_format = SDidentifier.SDidentifier(self.filename)
		if sd_format == "h5":
			with h5.File(self.filename) as sdfile:
				dataset = sdfile.keys()
				sd_file = sdfile[dataset[0]]
				return sd_file[item]
		
		elif sd_format == "nc":
			with xr.open_dataset(self.filename) as sdfile:
				dataset = sdfile.keys()
				sd_file = sdfile[dataset[0]]
				return sd_file.values[item]
		
		elif sd_format == "sgy":
			sdfile = sg.read(self.filename)
			for i in xrange(item[0]):
				return sdfile[i].data[item[1]]
		
		elif sd_format == "fits":
			with pyfits.open(self.filename) as sdfile:
				sd_file = sdfile[0].data
				return sd_file[item]
		
		elif sd_format == "adios":
			with ad.file(self.filename) as sdfile:
				dataset = sdfile.keys()
				sd_file = sdfile.var[dataset[3]].read()
				return sd_file[item] 
                
                elif sd_format == "csv":
                        sdfile = np.genfromtxt(self.filename, delimiter = ",")
                        return sdfile[item]

                elif sd_format == "json":
                        jfile = open(self.filename).read()
                        jdata = json.loads(jfile)
                        sdfile = np.array(jdata)
                        return sdfile[item]

    def displayFilePath(self):
        print "filename is %s" % self.filename
	

def read_chunk(index, SDlist, SDfile):
	chunk = tuple(slice(x) for x in SDlist[0])
	if index > 0:
		chunk_tmp = list(chunk)
		chunk_tmp[SDlist[2]] = slice(SDlist[1]*index, SDlist[1]*(index + 1), None)
		chunk = tuple(chunk_tmp)
	if SDlist[4] == "h5":
		with h5.File(SDfile) as sdfile:
			dataset = sdfile.keys()
			sd_file = sdfile[dataset[0]]
			return sd_file[chunk]
			
	elif SDlist[4] == "nc":
		with xr.open_dataset(SDfile) as sdfile:
			dataset = sdfile.keys()
			sd_file = sdfile[dataset[0]]
			return sd_file.values[chunk]

	elif SDlist[4] == "sgy":
		sdfile = sg.read(SDfile)
		for i in xrange(chunk[0]):
			return sdfile[i].data[chunk[1]]

	elif SDlist[4] == "fits":
		with pyfits.open(SDfile) as sdfile:
			sd_file = sdfile[0].data
			return sd_file[chunk]	
	
	elif SDlist[4] == "adios":
		with ad.file(SDfile) as sdfile:
			dataset = sdfile.keys()
			sd_file = sdfile[dataset[3]]
			return sd_file[chunk]
	
	elif SDlist[4] == "csv":
               	sd_file = np.genfromtxt(self.filename, delimiter = ",")
                return sd_file[chunk]

	elif sd_format == "json":
                jfile = open(self.filename).read()
                jdata = json.loads(jfile)
                sd_file = np.array(jdata)
                return sd_file[chunk]

def max(rdd):
	subrdd = rdd.map(lambda v: np.amax(v))
	return subrdd.takeOrdered(1, key = lambda x: -x)

def min(rdd):
	subrdd = rdd.map(lambda v: np.amin(v))
	return subrdd.takeOrdered(1, key = lambda x: x)

def mean(rdd):
	subrdd = rdd.map(lambda v: np.mean(v))
	subrdd1 = subrdd.map(lambda v: np.mean(v))
	return subrdd1.take(1)

def kmeans(rdd):
	parsedData = rdd.map(lambda v: np.hstack(v))
	from pyspark.mllib.clustering import KMeans
	clusters = KMeans.train(parsedData, 2, maxIterations=10, runs=10, initializationMode='random')

	from math import sqrt

	def error(point): 
		center = clusters.centers[clusters.predict(point)] 
		return sqrt(sum([x**2 for x in (point - center)]))

	WSSSE = (parsedData.map(lambda point:error(point)).reduce(lambda x, y: x + y))
	return str(WSSSE)
