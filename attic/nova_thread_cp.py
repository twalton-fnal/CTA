import os, sys, signal, string, re, shutil, math, subprocess, json
import copy
import datetime as dt
import multiprocessing as mp

from multiprocessing.pool import ThreadPool as Pool


PWD = str(os.environ.get('PWD'))


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

    subdirs = os.listdir(topdir)
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
get files that have not been copied
"""
def _GetFilesNotCopyToStorageMachine( tmpfiles ) :
    
    good_files = copy.deepcopy(tmpfiles)
    copy_files = _GetCopiedFiles()

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
def _GetCopiedFiles() :

    dir  = "/eos/ctaeos/cta/users/twalton/data/nova/"

    env  = "EOS_MGM_URL=root://storagedev201.fnal.gov XrdSecPROTOCOL=sss XrdSecSSSKT=/home/eos/cta_twalton.keytab"
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
def _CopyFilesFromdCacheTask( (i,ifile) ) :

    incr = 100

    if i%incr == 0 :
       print( "\t\tAt line [%d], copying the file [%s] to storagedev201.fnal.gov" % (i,ifile) )

 
    max_files = 2000
    dir       = "/eos/ctaeos/cta/users/twalton/data/nova/"
    sub       = ""

    env  = "EOS_MGM_URL=root://storagedev201.fnal.gov XrdSecPROTOCOL=sss XrdSecSSSKT=/home/eos/cta_twalton.keytab"
    eos  = "eos ls %s" % (dir)
    cmd  = "%s %s" % (env,eos)
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    stdout, stderr = proc.communicate()

    subdirs = stdout.strip().split("\n")
    for subdir in subdirs :
        eos  = "eos ls %s/%s | wc -l" % (dir,subdir)
        cmd  = "%s %s" % (env,eos)
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        stdout, stderr = proc.communicate()

        nfiles = int(stdout.strip())

        if nfiles < max_files :
           sub = subdir 
           break

    if len(sub) == 0 :
       sub  = dt.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
       eos  = "eos mkdir -p %s/%s" % (dir,sub)
       cmd  = "%s %s" % (env,eos)
       proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
       stdout, stderr = proc.communicate()
       print("\t\tCreated directory using the command [%s]" % cmd )


    if len(sub) == 0 : 
       sys.exit("Will not continue with invalid directory name [%s snd %s]" % (dir,sub))

 
    ddir = "%s/%s/" % (dir,sub)

    eos  = "eos cp %s %s" % (ifile,ddir)
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



"""
eos cp files
"""
def _CopyFilesFromdCache() :

    print( "\tEnter CopyFilesFromdCache")

    # get the files on dCache
    topdir = "/pnfs/dune/scratch/users/twalton/CTA/NovaTest/"

    if not os.path.isdir(topdir) :
       sys.exit( "\tThe directory [%s] does not exist. Cannot continue." % topdir )

    print( "\t\tgetting all of the files" )
    tmp_files  = _GetAllFiles(topdir)

    print( "\t\tremoving files found on test stand machine" )
    fake_files = _GetFilesNotCopyToStorageMachine(tmp_files)


    print( "\t\tcopying the remaining files [%d] to test stand machine" % len(fake_files) )

    if len(fake_files) == 0 :
       sys.exit( "\tThere are not any files to copy" )

    worker_nodes = 8

    pool   = Pool(processes=worker_nodes) 
    result = pool.map(_CopyFilesFromdCacheTask,enumerate(fake_files))

    print("\t\tfinished the copying")

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
