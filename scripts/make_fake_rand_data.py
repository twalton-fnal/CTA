#!/usr/local/bin python

import os, sys, signal, string, re, shutil, math, subprocess, json

from array import array
from functools import wraps

import datetime as dt
import multiprocessing as mp
import itertools 

PWD = str(os.environ.get('PWD'))


#------ helper functions  -----------------------------------

DIRSIZE = 0
def _UpdateDirSize(a) :
    global DIRSIZE
    DIRSIZE = a


"""
initializer for multiprocessing
"""
def _PoolInit(l):
    global lock
    lock = l



"""
get the size of files in directory
"""
def _GetTopDirectorySize() :

    count_files_tb = 0.
    if not os.path.isdir(TOPDIR) : return count_files_tb

    subdirs = os.listdir(TOPDIR)
    for subdir in subdirs : 
        fnames = os.listdir("%s/%s/" % (TOPDIR,subdir))
        for fname in fnames :
            fstats           = os.stat( "%s/%s/%s" % (TOPDIR,subdir,fname) )
            created_filesize = fstats.st_size
            cfsize_tb        = float(created_filesize) * 1e-12
            count_files_tb  += cfsize_tb

    return count_files_tb
   


"""
parse the Nova analysis dataset
"""
def _GetNovaData() :

    print( "\tEnter creating fake data based on NOvA analysis dataset\n\n")

    fake_file_sizes = []
    total_file_sizes = 0

    # open file and get file sizes
    tdir  = "/exp/dune/app/users/twalton/CTA"
    fnova = open("%s/nova.files.txt" % tdir, "r")
    lnova = fnova.readlines()

    min_file_size = 100000000000000
    max_file_size = 0

    min_filename  = ""
    max_filename  = ""  

    im = 0
    fm = 0

    for l, lline in enumerate(lnova) :
        if l%50000 == 0 :
           print( "\t\tAt line %d : [%s]" % (l,lline.strip()) )

        lwords = lline.strip().split("\t")

        fake_file_sizes.append([lwords[0],int(lwords[2]),int(lwords[3])] )
        total_file_sizes += int(lwords[2])

        if int(lwords[2]) < min_file_size :
           min_file_size = int(lwords[2])
           min_filename  = lwords[0]
           im = l

        if int(lwords[2]) > max_file_size :
           max_file_size = int(lwords[2])
           max_filename  = lwords[0]
           fm = l

    fnova.close()

    print( "\n\tCompleted reading lines [%d], with total files giving [%d] size (byte)" % ( len(lnova),total_file_sizes ) )

    file_size_tb = total_file_sizes * 1e-12
    min_file_size_mb = min_file_size 
    max_file_size_mb = max_file_size
    
    print( "\t\tdataset in TB [%.3f]" % (file_size_tb) )
    print( "\t\tmin and max file sizes [(%d) %s::%.3f bytes, (%s) %s::%.3f bytes]\n" % (im,min_filename,min_file_size_mb,fm,max_filename,max_file_size_mb) )

    return fake_file_sizes



"""
get the nova data bins
size is in KB
"""
def _OrganizeNovaDataFiles( data ) :

    bins = []

    for idata in data :
        dsize = int(round( float(idata[1]) * 1e-3 ))
        dlen  = len( str(dsize) ) - 1
        r     = -1 * dlen
        dsize = int(round(dsize,r))

        """
        if dsize < 10000 : 
           continue

        if dsize > 80000 : 
           continue
        """

        if dsize < 100 :
           continue


        if dsize >= 500 :
           continue


        if not dsize in bins :
           bins.append(dsize)

    bins.sort()
    return bins        



"""
create dataset with fake nova-like data
"""
def _CreateNovaFakeData( data ) :

    print( "\tEnter CreateNovaFakeData\n" )

    nbytes = _GetTopDirectorySize()

    print( "\t\tInital byte count [%.3f TB] " % nbytes )
    print( "\t\tcpu count=[%d]\n" % mp.cpu_count() )

    l = mp.Lock()

    try :
      pool   = mp.Pool(processes=mp.cpu_count(),initializer=_PoolInit,initargs=(l,))
      result = pool.map(_CreateGenericDataTask,enumerate(data))
    finally :
      pool.close()
      pool.join()


    print( "\tExit CreateNovaFakeData\n" )
 
    count_files_tb = _GetTopDirectorySize()
    return count_files_tb


"""
create fake generic data
"""
def _CreateGenericDataTask( (i,idata) ) :

    print( "\t\tat line [%d] : [%d] directory and max file sizes [%.3f,%.3f]" % (i,idata,DIRSIZE,MAX_FILES_TB) )

    max_files = 2000

    # create the subdirectory
    subpath = "%s/%s/%s" % (TOPDIR,idata,dt.datetime.now().strftime("%Y-%m-%d-%H-%M-%S"))
    if not os.path.isdir(subpath) :
       print( "\t\tcreating the directory [%s]\n" % subpath )
       os.makedirs(subpath)
    else :
       print( "\t\tThe directory [%s] exists!" % subpath)

    value = float(idata) / 1000
    data_byte = int(value * 1024)
    

    # create the files
    for m in range(0,max_files) :
        date      = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename  = "rand_dev_file_%s_%s_%d" % (idata,date,m)
        fpath     = "%s/%s" % (subpath,filename)
        command   = "dd if=/dev/urandom of=%s bs=%d count=1024" % (fpath,data_byte)
        process   = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        pipe      = process.communicate()[0].split()
 

    # get the directory size
    count_files_mb = 0.
    fnames = os.listdir("%s" % (subpath))
    for fname in fnames :
        fstats           = os.stat( "%s/%s" % (subpath,fname) )
        created_filesize = fstats.st_size
        cfsize_mb        = float(created_filesize) * 1e-6
        count_files_mb  += cfsize_mb

    print("\t\tdirectory size [%.3f]" % count_files_mb)

    return True



#-----------------------------------------------------------------------------
#
# main function
#
#-----------------------------------------------------------------------------
if __name__ == '__main__' :

   print( "Create fake data based on NOvA analysis dataset\n\n")

   # get the nova data
   nova_data = _GetNovaData()

   # organize the nova data
   sort_file_sizes = _OrganizeNovaDataFiles(nova_data)

   global TOPDIR
   TOPDIR = "/pnfs/dune/scratch/users/twalton/UpgradedCTA/RandomTestGenericData/"  #RandomTestGenericTest
   if not os.path.isdir(TOPDIR) :
      print( "\tCreating the top directory [%s]" % TOPDIR )
      os.makedirs(TOPDIR)


   # max number of files to create
   global MAX_FILES_TB
   MAX_FILES_TB   = 6.0

   # create dataset with fake nova-like data
   count_files_tb = _CreateNovaFakeData(sort_file_sizes)  


   print( "\tSuccessful created a fake dataset size [%.3f]" % count_files_tb ) 
   print( "\nCompleted creating fake data based on NOvA analysis dataset\n\n")
