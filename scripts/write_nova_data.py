import os, sys, signal, string, re, shutil, math, subprocess, json
import copy
import datetime as dt
import multiprocessing as mp

from multiprocessing.pool import ThreadPool as Pool
from time import sleep


PWD = str(os.environ.get('PWD'))


"""
initializer for multiprocessing
"""
def _PoolInit(l):
    global lock
    lock = l



"""
eos cp files
"""
def _CopyFilesFromdCacheTask( (i, filesInDir) ) :

    print( "\t\tat line [%d], copying the file directory to storagedev201.fnal.gov" % (i) )

    env  = "EOS_MGM_URL=root://storagedev201.fnal.gov XrdSecPROTOCOL=sss XrdSecSSSKT=/home/eos/cta_twalton.keytab"
  
    #run  = "run1" 
    run  = "run2"
    dir  = "/eos/ctaeos/cta/users/twalton/spring2024/data/novafiles/"
    sub  = "%s-%s" % (dt.datetime.now().strftime("%Y-%m-%d-%H-%M-%S"),i)

    # create the directory
    eos  = "eos mkdir -p %s/%s/%s" % (dir,run,sub)
    cmd  = "%s %s" % (env,eos)
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    stdout, stderr = proc.communicate()
    print("\t\t\tcreated directory using the command [%s]" % cmd )


    # copy the files into directory 
    for f, fname in enumerate(filesInDir) :
        date      = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        #filename  = "rand_nova_fake_file_%s_%d_%d" % (date,i,f)
        #filename  = fname.split("/")[-1]
        filename  = "%s_%d_%d" % (fname.split("/")[-1],i,f)
        fullpath  = "%s/%s/%s/%s" % (dir,run,sub,filename)
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
eos cp files
"""
def _CopyFilesFromdCache() :

    print( "\tEnter CopyFilesFromdCache")

    # get the sub directories
    subdirs = os.listdir(TOPDIR)
    print( "\t\tthe number of subdirectories [%d]" % len(subdirs) )

    fake_files = []
    count      = 0

    # loop over sub directories
    for subdir in subdirs :
        subpath = "%s/%s" % (TOPDIR,subdir)

        if not os.path.isdir(subpath) :
           print( "\tWarning, the directory [%s] does not exist. Skipping." % subpath )
           continue

        sfiles = os.listdir(subpath)
        fnames = []

        for s in sfiles :
            fpath = "%s/%s" % (subpath,s)
            fnames.append( fpath )

        count += len(sfiles)
        fake_files.append(fnames)

    print( "\t\tCopying files [%d] to dev machine" % count )


    # copy and write data
    if len(fake_files) == 0 :
       sys.exit( "\tThere are not any files to copy" )

    l = mp.Lock()
    print( "\t\tcpu count [%d]" % mp.cpu_count() )

    try :
      pool   = mp.Pool(processes=mp.cpu_count(),initializer=_PoolInit,initargs=(l,))
      result = pool.map(_CopyFilesFromdCacheTask,enumerate(fake_files))
    finally :
      pool.close()
      pool.join()

    print( "\tExit CopyFilesFromdCache")


###############################################
# main function
###############################################
if __name__ == '__main__' :

   print( "\nEnter write nova fake data to tape\n" )

   global TOPDIR
   TOPDIR = "/pnfs/dune/scratch/users/twalton/CTA/RandomFakeNovaData/"

   if not os.path.isdir(TOPDIR) :
      sys.exit( "\tThe directory [%s] does not exist. Cannot continue." % TOPDIR )

   _CopyFilesFromdCache()

   print( "Exit write nova fake data to tape\n" )
