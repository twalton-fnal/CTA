import os, sys, signal, string, re, shutil, math, subprocess, json

from array import array
from functools import wraps

import datetime as dt
import multiprocessing as mp
import itertools

PWD = str(os.environ.get('PWD'))


"""
initializer for multiprocessing
"""
def _PoolInit(l):
    global lock
    lock = l

"""
skip files that are on the test stand machine
"""
def _SkipCopying( ifile ) :


    env  = "EOS_MGM_URL=root://storagedev201.fnal.gov XrdSecPROTOCOL=sss XrdSecSSSKT=/home/eos/cta_twalton.keytab"
    eos  = "eos ls -yh %s" % (ifile)
    cmd  = "%s %s" % (env,eos)
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    pipe = proc.communicate()[0].split()     


    return False

"""
eos cp files
"""
def _CopyFilesFromdCacheTask( (i,ifile) ) :

    if i%500 == 0 :
       print( "\t\tAt line [%d], copying the file [%s] to storagedev201.fnal.gov" % (i,ifile) )

    dir = "/eos/ctaeos/cta/users/twalton/data/nova/"

    env  = "EOS_MGM_URL=root://storagedev201.fnal.gov XrdSecPROTOCOL=sss XrdSecSSSKT=/home/eos/cta_twalton.keytab"
    eos  = "eos cp %s %s" % (ifile,dir)
    cmd  = "%s %s" % (env,eos)

    """ 
    if i%500 == 0 :
       print("%s" % cmd)
    """

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    pipe = proc.communicate()[0].split()     

    if i%500 == 0 :
       print( "\t%s" % pipe )
 

"""
eos cp files
"""
def _CopyFilesFromdCache() :

    print( "\tEnter CopyFilesFromdCache")

    fake_files = []

    # get the files on dCache
    topdir = "/pnfs/dune/scratch/users/twalton/CTA/NovaTest/"

    if not os.path.isdir(topdir) :
       sys.exit( "\tThe directory [%s] does not exist. Cannot continue." % topdir )

    subdirs = os.listdir(topdir)
    for subdir in subdirs :
        if subdir.find("2023-11-09-15-29-16") != -1 :
           continue

        subpath = "%s/%s" % (topdir,subdir)

        if not os.path.isdir(subpath) :
           print( "\tWarning, the directory [%s] does not exist. Skipping." % subpath )

        sfiles = os.listdir(subpath)

        for s in sfiles :
            fpath = "%s/%s" % (subpath,s)
            fake_files.append(fpath)


    print( "\t\tCopying files [%d] to test stand machine" % len(fake_files) )

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


#-----------------------------------------------------------------------------
#
# main function
#
#-----------------------------------------------------------------------------
if __name__ == '__main__' :

   print( "\nEnter copy files from dCache" )

   _CopyFilesFromdCache()
    

   print( "Exit copy files from dCache\n" )
