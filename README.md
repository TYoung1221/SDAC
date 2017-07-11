# SDAC
Scientific Data Auto Chunk

# About
This is a module atop Spark framework to bridge the gap between large scale scientific data (HDF5, NetCDF, Adios, SEGY and FITS)and Spark RDDs. And integrated with an auto-chunk method to optimize the Spark native partitioning way to improve the task granularity in distributed environment.

# How to use in Pyspark
1.
```
export PYTHONPATH=$PYTHONPATH:path_to_SDAC/src
```
2.
```
from pyspark import SparkContext
import os,sys
import SDAC

SDfile = "your_directory/input.h5"
SDAC_rdd = SDAC.sd_reader(sc, SDfile, chunk = True)
SDAC_rdd.cache()
```
