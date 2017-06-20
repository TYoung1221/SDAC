import xarray as xr
import h5py
from obspy import read
import pyfits
import adios as ad


hdf5_format = "Hierarchical Data Format"

ascii_format = "ASCII text"

binary_format = "data"

fits_format = "FITS"

ERROR = "not identified"


def netCDFreader(netCDF_path):
	try:
		cdfFile = xr.open_dataset(netCDF_path)
		keys = cdfFile.dims.keys()
		dimension = cdfFile.dims.values()
	except:
		print 'directory does not exist'
		### not so sure right now 
	return dimension

def hdf5_reader(hdf5_path):
	try:
		h5f = h5py.File(hdf5_path, "r")
		dataset = h5f.keys()
		dimension = h5f[dataset[0]][:].shape
	except:
		print 'directory not exist'
				
##	if h5f[dataset[0]].chunks is None:
##		print "HDF5 datasets have no chunk"
##	else:
##		print "chunk size is:", h5f[dataset[0]].chunks
		
	return dimension

def sgy_reader(sgy_path):
	try:
		sgy = read(sgy_path, unpack_trace_headers=True)
		dimension = [sgy.count() - 1, sgy[0].data.shape[0]]
	except:
		print 'directory does not exist'
	return dimension

def fits_reader(fits_path):
	try:
		fits = pyfits.open(fits_path)
		dimension = list(fits[0].data.shape)
	except:
		print 'directory does not exist'	
	return dimension

def adios_reader(adios_path):
	try:
		ad1 = ad.file(adios_path)
		dataset = ad1.keys()
		dimension = ad1.var[dataset[3]].read().shape
	except:
		print 'directory does not exist'
	return dimension

def CSV_reader(csv_path):
    import csv
    try:
        with open(csv_path, 'rb') as csvfile:
            cfile = list(csv.reader(csvfile, delimiter=','))
            dimension = [len(cfile), len(cfile[0])]
    except:
        print 'directory does not exist'

    return dimension

def JSON_reader(json_path):
    import json
    try:
        with open(json_path) as jsonfile:
            jdata = json.loads(jsonfile.read())
            dimension = [len(jdata), len(jdata[0])]
    except:
        print 'directory does not exist'

    return dimension

def auto_chunk(num_workers, dimensions):
	chunk_lst = list(dimensions)
	if all(item % int(num_workers) != 0 for item in dimensions):
		min_dim = min(dimensions, key=lambda x:abs(x - num_workers))
		index = dimensions.index(min_dim)
                off = min_dim/num_workers
		if min_dim >= num_workers:
			chunk_lst[index] = off + 1
                        chunk_num = dimensions[index]/off
		else:
			chunk_lst[index] = 1
                        chunk_num = dimensions[index]/chunk_lst[index]

	else:
		min_dim = min(filter(lambda x: x % num_workers == 0, dimensions))
		index = dimensions.index(min_dim)
		chunk_lst[index] = min_dim/num_workers
                chunk_num = dimensions[index]/chunk_lst[index]

	return chunk_lst, chunk_lst[index], index, chunk_num
