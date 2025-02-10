#!/usr/bin/env python3

import os, sys, signal, string, re, shutil, math, subprocess, json
import copy
import datetime as dt
import multiprocessing as mp

from multiprocessing.pool import ThreadPool as Pool
from time import sleep

from argparse import ArgumentParser as ap

PWD = str(os.environ.get('PWD'))
USER = str(os.environ.get('USER'))


"""
initializer for multiprocessing
"""
def _PoolInit(l):
    global lock
    lock = l



"""
eos cp files
"""
def _CopyFilesFromdCacheToEOSTask( i, filesInDir ) :

    print( "\t\tat line [%d], copying the file directory to storagedev201.fnal.gov" % (i) )

    env   = EOS_ENV
    tdir  = OUTDIR
    sub   = "%s-%s" % (dt.datetime.now().strftime("%Y-%m-%d-%H-%M-%S"),i)

    # create the directory
    eos  = "eos mkdir -p %s/%s" % (tdir,sub)
    cmd  = "%s %s" % (env,eos)
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    stdout, stderr = proc.communicate()
    print("\t\t\tcreated the directory using the command [%s]" % cmd )

    # copy the files into directory 
    for f, fname in enumerate(filesInDir) :
        date      = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename  = "%s_%d_%d" % (fname.split("/")[-1],i,f)
        fullpath  = "%s/%s/%s" % (tdir,sub,filename)
        eos       = "eos cp %s %s" % (fname,fullpath)
        cmd       = "%s %s" % (env,eos)
        proc      = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        stdout, stderr = proc.communicate()

        if f%500 == 0 :
           print( "\t\tcmd [%s]" % cmd )
           print( "\t\tstdout [%s]" % stdout )

        if proc.returncode != 0 :
           print( "============== error code start =========================")
           print( "\t\tAt line [%d-%d], warning::[%s]" % (i,f,stderr) )
           print( "\t\tAt line [%d-%d], cmd;;[%s]" % (i,f,cmd) )
           print( "============== error code end ===========================")

    return True



"""
dcache cp files
"""
def _CopyFilesFromdCacheTodCacheTask( i, filesInDir ) :

    print( "\t\tat line [%d], copying the file directory to storagedev201.fnal.gov" % (i) )

    env   = DCACHE_ENV
    tdir  = OUTDIR
    sub   = "%s-%s" % (dt.datetime.now().strftime("%Y-%m-%d-%H-%M-%S"),i)

    # create the directory
    dcache = "mkdir -p %s/%s" % (tdir,sub)
    cmd    = "%s %s" % (env,dcache)
    proc   = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    stdout, stderr = proc.communicate()
    print("\t\t\tcreated the directory using the command [%s]" % cmd )

    # copy the files into directory 
    for f, fname in enumerate(filesInDir) :
        date      = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename  = "%s_%d_%d" % (fname.split("/")[-1],i,f)
        fullpath  = "%s/%s/%s" % (tdir,sub,filename)
        dcache    = "cp %s %s" % (fname,fullpath)
        cmd       = "%s %s" % (env,dcache)
        proc      = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        stdout, stderr = proc.communicate()

        if f%500 == 0 :
           print( "\t\tcmd [%s]" % cmd )
           print( "\t\tstdout [%s]" % stdout )

        if proc.returncode != 0 :
           print( "============== error code start =========================")
           print( "\t\tAt line [%d-%d], warning::[%s]" % (i,f,stderr) )
           print( "\t\tAt line [%d-%d], cmd;;[%s]" % (i,f,cmd) )
           print( "============== error code end ===========================")

    return True



"""
copy files from dCache scratch to EOS or dCache pool disk
"""
def _CopyFilesFromdCacheScratch(disk) :

    print( "\tEnter copy files from dCache scratch to disk type [%s]" % disk )

    #-----------------------------------------
    # get the sub directories
    #-----------------------------------------
    subdirs = os.listdir(TOPDIR)
    print( "\t\tthe number of subdirectories [%d]" % len(subdirs) )

    fake_files = []
    count      = 0

    #-----------------------------------------
    # loop over sub directories
    #-----------------------------------------
    for subdir in subdirs :
        subpath = "%s/%s" % (TOPDIR,subdir)

        if not os.path.isdir(subpath) :
           print( "\t\tWarning, the directory [%s] does not exist. Skipping." % subpath )
           continue

        sfiles = os.listdir(subpath)
        fnames = []

        for s in sfiles :
            fpath = "%s/%s" % (subpath,s)
            fnames.append( fpath )

        count += len(sfiles)
        fake_files.append(fnames)

    print( "\t\tCopying files [%d] to disk type [%s] that is mounted on the storage dev machine" % (count,disk) )

    #-------------------------
    # copy and write data
    #-------------------------
    if len(fake_files) == 0 :
       sys.exit( "\tThere are not any files to copy" )

    l = mp.Lock()
    print( "\t\tcpu count [%d]" % mp.cpu_count() )

    try :
      pool   = mp.Pool(processes=mp.cpu_count(),initializer=_PoolInit,initargs=(l,))
      if disk == "eos" :
         result = pool.starmap(_CopyFilesFromdCacheToEOSTask,enumerate(fake_files))
      elif disk == "dcache" :
         result = pool.starmap(_CopyFilesFromdCacheTodCacheTask,enumerate(fake_files))
    finally :
      pool.close()
      pool.join()

    
    print( "\tExit copy files from dCache scratch to disk type [%s]" % disk )



###############################################
# main function
###############################################
if __name__ == '__main__' :

   print( "\nEnter write nova fake data to tape\n" )

   # input arguments
   parser = ap()
   parser.add_argument('--disk', type=str, default="dCache", require=True, help="The disk location to copy files (eos or dCache) [default=%default]")
   parser.add_argument('--indir', type=str, default="/pnfs/dune/scratch/users/%s/CTA_LTO9/NovaFakeData/" % USER, help="The location of the data on dCache scratch [default: %default]")
   parser.add_argument('--dir', type=str, default="/pnfs/dune/scratch/users/%s/CTA_LTO9/NovaFakeData/" % USER, help="The location of the copied data on disk [default: %default]")
   args = parser.parse_args()

   # EOS environment variables
   global EOS_ENV
   EOS_ENV ="EOS_MGM_URL=root://storagedev201.fnal.gov XrdSecPROTOCOL=sss XrdSecSSSKT=/home/eos/cta_twalton.keytab"

   # dcache environment variables
   global DCACHE_ENV
   DCACHE_ENV =""

   # output directory 
   global OUTDIR
   OUTDIR = args.dir # "/eos/ctaeos/cta/users/twalton/spring2024/data/novafiles/run1"

   # input directory 
   global TOPDIR
   TOPDIR = args.indir #"/pnfs/dune/scratch/users/twalton/CTA/RandomFakeNovaData/"

   if not os.path.isdir(TOPDIR) :
      sys.exit( "\tThe directory [%s] does not exist. Cannot continue." % TOPDIR )

   # copy files from dCache scratch to eos or dCache disk
   _CopyFilesFromdCacheScratch(args.disk.lower())

   print( "Exit write nova fake data to tape\n" )
