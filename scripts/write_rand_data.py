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

# test

"""
initializer for multiprocessing
"""
def _PoolInit(l):
    global lock
    lock = l



"""
get all of the files
"""
def _GetAllFiles( topdir ) :
    fake_files = []
    subdirs    = os.listdir(topdir)
    for subdir in subdirs :
        subpath = "%s/%s" % (topdir,subdir)

        if not os.path.isdir(subpath) :
           continue

        sfiles = os.listdir(subpath)
        for s in sfiles :
            fpath = "%s/%s" % (subpath,s)
            fake_files.append(fpath)

    return fake_files



"""
remove files of size 0 bytes
"""
def removeZeroByteFilesOnEOS( tmpfiles ) :
    env    = EOS_ENV
    eosdir = OUTDIR 
    nfiles = 0

    for t, tmpfile in enumerate(tmpfiles) :
        subdir   = tmpfile.split("/")[-3]
        randfile = tmpfile.split("/")[-1]
        filepath = "%s/%s/%s" % (eosdir,subdir,randfile)
 
        if t == 0 :
           subdirpath = "%s/%s" % (eosdir,subdir)   
           eos  = "eos ls -lhy %s | wc -l" % (subdirpath)
           cmd  = "%s %s" % (env,eos)
           proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
           stdout, stderr = proc.communicate()
           if int(stdout.strip()) == 0 :
              print("\t\tempty directory, do not need to check for file removal")
              break

        eos  = "eos ls -lhy %s" % (filepath)
        cmd  = "%s %s" % (env,eos)
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        stdout, stderr = proc.communicate()

        if stdout.find("d0::t0") == -1 : continue
        byte = stdout.split(" ")[-5]
      
        if byte == "0" :
           eos  = "eos rm -f %s" % filepath
           cmd  = "%s %s" % (env,eos)
           proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
           nfiles += 1 
      
    print("\t\tnumber of files removed [%d]" % nfiles)
    return tmpfiles



"""
get files that have not been copied
"""
def _GetFilesNotCopyToEOS( tmpfiles ) :
    good_files = copy.deepcopy(tmpfiles)
    copy_files = _GetCopiedFilesAtEOS()

    for copy_file in copy_files :
        for g, good_file in enumerate(good_files) :
            if good_file.find(copy_file) != -1 :
               good_files.remove(good_file)
               break

    print( "\t\tnumber of files [%d] and non-copied files [%d]" % (len(tmpfiles),len(good_files)) )
    return good_files 



"""
the task for get files that have not been copied
"""
def _GetCopiedFilesAtEOS() :
    dir  = OUTDIR
    env  = EOS_ENV
    eos  = "eos ls %s" % (dir)
    cmd  = "%s %s" % (env,eos)
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    stdout, stderr = proc.communicate()

    subdirs = stdout.strip().split("\n")
    sfiles  = []

    for subdir in subdirs :
        eos  = "eos ls %s/%s" % (dir,subdir)
        cmd  = "%s %s" % (env,eos)
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)

        stdout, stderr = proc.communicate()
        tmp_files = stdout.strip().split("\n")
        sfiles.extend(tmp_files)

    return sfiles



"""
eos cp files
"""
def _CopyFilesFromdCacheScratchToEOSTask( i,ifile ) :
    incr = 500

    if i%incr == 0 :
       print( "\t\tAt line [%d], copying the file [%s] to storagedev201.fnal.gov" % (i,ifile) )

    max_files = MAX_FILES
    dir       = OUTDIR
    sub       = ""

    byte_dir  = ifile.split("/")[9] 
    full_dir  = "%s/%s" % (dir,byte_dir)
    
    env  = EOS_ENV
    eos  = "eos ls -yh %s | wc -l" % (full_dir)
    cmd  = "%s %s" % (env,eos)
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    stdout, stderr = proc.communicate()

    if int(stdout.strip()) == 0 :
       eos = "eos mkdir -p %s" % (full_dir)
       cmd  = "%s %s" % (env,eos)
       proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
       stdout, stderr = proc.communicate()
    elif int(stdout.strip()) > max_files :
       return True
      
    eos  = "eos cp %s %s" % (ifile,full_dir)
    cmd  = "%s %s" % (env,eos)
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    stdout, stderr = proc.communicate()

    if i%incr == 0 :
       print( "\t\tcmd [%s]" % cmd )
       print( "\t\tstdout [%s]" % stdout )

    if proc.returncode != 0 :
       print( "============== error code start =========================")
       print( "\t\tAt line [%d], warning::[%s]" % (i,stderr) )
       print( "\t\tAt line [%d], cmd;;[%s]" % (i,cmd) )
       print( "============== error code end ===========================")

    return True



"""
eos cp files
"""
def _CopyFilesFromdCacheScratchTodCacheTask( i,ifile ) :
    incr = 500

    if i%incr == 0 :
       print( "\t\tAt line [%d], copying the file [%s] to storagedev201.fnal.gov" % (i,ifile) )

    max_files = MAX_FILES
    tdir      = OUTDIR
    sub       = ""

    byte_dir  = ifile.split("/")[9] 
    full_dir  = "%s/%s" % (dir,byte_dir)
    
    env       = DCACHE_ENV
    dcache    = ""
    cmd       = "%s %s" % (env,dcache)
    proc      = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    stdout, stderr = proc.communicate()

    if int(stdout.strip()) == 0 :
       dcache = ""
       cmd    = "%s %s" % (env,dcache)
       proc   = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
       stdout, stderr = proc.communicate()
    elif int(stdout.strip()) > max_files :
       return True
      
    dcache = "cp %s %s" % (ifile,full_dir)
    cmd    = "%s %s" % (env,eos)
    proc   = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    stdout, stderr = proc.communicate()

    if i%incr == 0 :
       print( "\t\tcmd [%s]" % cmd )
       print( "\t\tstdout [%s]" % stdout )

    if proc.returncode != 0 :
       print( "============== error code start =========================")
       print( "\t\tAt line [%d], warning::[%s]" % (i,stderr) )
       print( "\t\tAt line [%d], cmd;;[%s]" % (i,cmd) )
       print( "============== error code end ===========================")

    return True



"""
check the status of the files that are copied to EOS disk
"""
def _CheckFilesStatusOnEOS( subdir ) :
    success = False
    status  = []

    eosdir  = OUTDIR
    eosenv  = EOS_ENV
    eoscmds = [   "eos ls -lhy %s/%s | wc -l" % (eosdir,subdir)
                , "eos ls -lhy %s/%s | grep -i \"d1::t1\" | wc -l" % (eosdir,subdir)
                , "eos ls -lhy %s/%s | grep -i \"d1::t0\" | wc -l" % (eosdir,subdir)
              ]

    for eoscmd in eoscmds :
        cmd = "%s %s" % (eosenv,eoscmd)
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        stdout, stderr = proc.communicate()
        status.append( int(stdout.strip()) )

    print( "\t\ttotal number of files in directory, on tape, not on tape [%d : %d : %d]" % (status[0],status[1],status[2]) )

    if status[0] == status[1] :
       success = True

    return success



"""
check the status of the files that are copied to dCache disk
"""
def _CheckFilesStatusOndCache( subdir ) :
    success = False
    status  = []

    ddir    = OUTDIR
    denv    = DCACHE_ENV

    print( "\t\ttotal number of files in directory, on tape, not on tape [%d : %d : %d]" % (status[0],status[1],status[2]) )

    if status[0] == status[1] :
       success = True

    return success



"""
copy files from dCache scratch to EOS or dCache pool disk
"""
def _CopyFilesFromdCacheScratch(disk,indir,skipdirs) :

    print( "\tEnter copying files from dCache scratch")

    #----------------------------------
    # get the files on dCache
    #----------------------------------
    topdir = indir
    if not os.path.isdir(topdir) :
       sys.exit( "\tThe directory [%s] does not exist. Cannot continue." % topdir )

    print( "\t\tGet the subdirectories in the folder [%d]" % indir )
    subdirs = os.listdir(topdir)
    subdirs = map(int, subdirs)
    subdirs.sort()
    subdirs.reverse()

    #------------------------------------------------
    # loop over subdirectories
    #------------------------------------------------
    for subdir in subdirs :
        subpath = "%s/%d" % (topdir,subdir)
        if not os.path.isdir(subpath) :
           print( "\t\t warning: the subdirectory does not exist [%s]" % subpath )
           continue
        else :
           print( "\t\tAt the subdirectory [%s]" % subpath )

        if len(skipdirs) != 0 :
           if subdir in skipdirs :
              print( "\t\t  skipping the subdirectory [%s]" % subdir )
              continue
 
        tmp_files = _GetAllFiles(subpath)
        print( "\t\t\tnumber of files [%d]" % len(tmp_files) )

        #--------------------------------------------
        # sanity checks for copying files
        #--------------------------------------------
        if disk == "eos" :
           tmp_files = removeZeroByteFilesOnEOS( tmp_files )
           print( "\t\t\tnumber of files is [%d]" % len(tmp_files) )

           fake_files = _GetFilesNotCopyToEOS(tmp_files)
           print( "\t\tcopying the remaining files [%d] to test stand machine" % len(fake_files) )
        elif disk == "dcache" :
             fake_files = tmp_files

        #-------------------------------------
        # check the number of files
        #-------------------------------------
        if len(fake_files) == 0 :
           print( "\t\tThe directory [%s] does not have any files to copy from scratch to disk." % subpath )
           continue
        else :
           print( "\t\tBegin copying the files at time: [%s]" % dt.datetime.now().strftime("%Y-%m-%d-%H-%M-%S") )

        #--------------------------
        # begin copying
        #--------------------------
        try :
            pool = mp.Pool(processes=mp.cpu_count(),initializer=_PoolInit,initargs=(l,))
            if disk == "eos" : 
               result = pool.starmap(_CopyFilesFromdCacheScratchToEOSTask,enumerate(fake_files))
            elif disk == "dcache" :
               result = pool.starmap(_CopyFilesFromdCacheScratchTodCacheTask,enumerate(fake_files))
        finally :
            pool.close()
            pool.join()

        print( "\t\tCompleted copying at time: [%s]" % dt.datetime.now().strftime("%Y-%m-%d-%H-%M-%S") )

        #------------------------------------------------------
        # wait before starting with the next bins
        #------------------------------------------------------
        seconds = 1800
        print( "\t\tWaitng for [%d] seconds to proceed with the next subdirectory." % seconds )
        sleep(1800)

        #-------------------------------------
        # check the status of copying
        #-------------------------------------
        success = False
        if disk == "eos" :
           success = _CheckFilesStatusOnEOS(subdir)
        elif disk == "dcache" :
           success = _CheckFilesStatusOndCache(subdir)

        if not success :
           sys.exit( "\t\tAll files are not on tape... exiting..\n\n" )
        else :
           print( "\t\tAll files are on tape.")

    print( "\tExit copying files from dCache scratch\n\n")




#-----------------------------------------------------------------------------
#
# main function
#
#-----------------------------------------------------------------------------
if __name__ == '__main__' :

   print( "\nEnter write random fake data to tape\n" )

   # input arguments
   parser = ap()
   parser.add_argument('--disk', type=str, default="dCache", require=True, help="The disk location to copy files (eos or dCache) [default=%default]")
   parser.add_argument('--indir', type=str, default="/pnfs/dune/scratch/users/%s/CTA_LTO9/SingleBinnedData/" % USER, help="The location of the data on dCache scratch [default: %default]")
   parser.add_argument('--dir', type=str, default="/pnfs/dune/scratch/users/%s/CTA_LTO9/SingleBinnedData/" % USER, help="The location of the copied data on disk [default: %default]")
   parser.add_argument('--skip', nargs='+', default=[], help="A list of directories to skip (eg. 10000)" )
   parser.add_argument('--nfiles', type=int, default=10000, help="The maximum number of files to copy from scratch to disk, where the default is %default")
   args = parser.parse_args()

   # EOS environment variables
   global EOS_ENV
   EOS_ENV ="EOS_MGM_URL=root://storagedev201.fnal.gov XrdSecPROTOCOL=sss XrdSecSSSKT=/home/eos/cta_twalton.keytab"

   # dcache environment variables
   global DCACHE_ENV
   DCACHE_ENV =""

   # output directory 
   global OUTDIR
   OUTDIR = args.dir # /eos/ctaeos/cta/users/twalton/spring2024/data/randomfiles/

   # check input directory
   if not os.path.isdir(args.indir) :
      sys.exit( "\tThe directory [%s] does not exist. Cannot continue." % args.indir )

   # set the maximum number of files
   global MAX_FILES
   MAX_FILES = args.nfiles

   # copy files to disk (CTA will write the files from disk to tape)
   _CopyFilesFromdCacheScratch(args.disk.lower(),args.indir,args.skip)

   print( "Exit write random fake data to tape\n" )
