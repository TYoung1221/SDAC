import magic
import os
import SDreader

sd_dimensions = []

def file_identifier(sd_path, num_workers, chunk_flag):
	file_format = magic.from_file(sd_path)
	file_extension = os.path.splitext(sd_path)[1]
	if SDreader.hdf5_format in file_format:
		if file_extension == ".nc":
			sd_dimensions = SDreader.netCDFreader(sd_path)
			sdformat = "nc"
		
		elif file_extension == ".h5":
			sd_dimensions = SDreader.hdf5_reader(sd_path)
			sdformat = "h5"
		
		else:
			print "File format is not supported"
			return SDreader.ERROR
	elif SDreader.ascii_format in file_format:
		if file_extension == ".csv":
                        sd_dimensions = SDreader.CSV_reader(sd_path)
                        sdformat = "csv"

                elif file_extension == ".json":
                        sd_dimensions = SDreader.JSON_reader(sd_path)
                        sdformat = "json"
                else:
                        print "File format is not supported"
                        return SDreader.ERROR	
                #print "File format is not supported"
	
	elif SDreader.binary_format == file_format:
		if file_extension == ".sgy":
			sd_dimensions = SDreader.sgy_reader(sd_path)
			sdformat = "sgy"
		if file_extension == ".bp":
			sd_dimensions = SDreader.adios_reader(sd_path)
			sdformat = "adios"
	
	elif SDreader.fits_format in file_format:
		if file_extension == ".fits":
			sd_dimensions = SDreader.fits_reader(sd_path)
			sdformat = "fits"
	
	chunk_list, offset, offset_id, chunk_num = SDreader.auto_chunk(num_workers, sd_dimensions)
	if chunk_flag == True:
		return [chunk_list, offset, offset_id, chunk_num, sdformat]
	else:
		return [chunk_flag, offset, offset_id, chunk_num, sdformat]

def SDidentifier(sd_path):
	file_format = magic.from_file(sd_path)
	file_extension = os.path.splitext(sd_path)[1]
	if SDreader.hdf5_format in file_format:
		if file_extension == ".nc":
			sd_dimensions = SDreader.netCDFreader(sd_path)
			sdformat = "nc"

		elif file_extension == ".h5":
			sd_dimensions = SDreader.hdf5_reader(sd_path)
			sdformat = "h5"

		else:
			print "File format is not supported"
			return SDreader.ERROR
						
	elif SDreader.ascii_format in file_format:
		if file_extension == ".csv":
                        sd_dimensions = SDreader.CSV_reader(sd_path)
                        sdformat = "csv"

                elif file_extension == ".json":
                        sd_dimensions = SDreader.JSON_reader(sd_path)
                        sdformat = "json"
                else:
                        print "File format is not supported"
                        return SDreader.ERROR
		#print "File format is not supported"

	elif SDreader.binary_format == file_format:
		if file_extension == ".sgy":
			sd_dimensions = SDreader.sgy_reader(sd_path)
			sdformat = "sgy"
		if file_extension == ".bp":
			sd_dimensions = SDreader.adios_reader(sd_path)
			sdformat = "adios"

	elif SDreader.fits_format in file_format:
		if file_extension == ".fits":
			sd_dimensions = SDreader.fits_reader(sd_path)
			sdformat = "fits"
	
	return sdformat
