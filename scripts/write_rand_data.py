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
directories to skip
"""
def _SkipSubdirectories( dir ) :
   
    dlist = [ "100",    "200",    "300",   "400",   "500",   "600",   "700",   "800",   "900",   
              "1000",   "2000",   "3000",  "4000",  "5000",  "6000",  "7000",  "8000",  "9000",   
              "10000",  "20000",  "30000", "40000", "50000", "60000", "70000", "80000", "90000", 
              "100000", "200000"  ]

    if dir in dlist :
       return True
    
    return False



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
check the status of the files
"""
def _CheckFilesStatus( subdir ) :
    success = False

    status  = []

    eosdir  = "/eos/ctaeos/cta/users/twalton/spring2024/data/randomfiles/"
    eosenv  = "EOS_MGM_URL=root://storagedev201.fnal.gov XrdSecPROTOCOL=sss XrdSecSSSKT=/home/eos/cta_twalton.keytab"
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
remove files of size 0 bytes
"""
def removeZeroByteFiles( tmpfiles ) :

    env    = "EOS_MGM_URL=root://storagedev201.fnal.gov XrdSecPROTOCOL=sss XrdSecSSSKT=/home/eos/cta_twalton.keytab"
    eosdir = "/eos/ctaeos/cta/users/twalton/spring2024/data/randomfiles/"
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

    dir  = "/eos/ctaeos/cta/users/twalton/spring2024/data/randomfiles/"

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

    incr = 500

    if i%incr == 0 :
       print( "\t\tAt line [%d], copying the file [%s] to storagedev201.fnal.gov" % (i,ifile) )

    max_files = 12000 #5000
    dir       = "/eos/ctaeos/cta/users/twalton/spring2024/data/randomfiles/"
    sub       = ""

    byte_dir  = ifile.split("/")[9] 
    full_dir  = "%s/%s" % (dir,byte_dir)
    
    env  = "EOS_MGM_URL=root://storagedev201.fnal.gov XrdSecPROTOCOL=sss XrdSecSSSKT=/home/eos/cta_twalton.keytab"
    eos  = "eos ls -yh %s | wc -l" % (full_dir)
    cmd  = "%s %s" % (env,eos)
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    stdout, stderr = proc.communicate()

    if int(stdout.strip()) == 0 :
       eos = "eos mkdir -p %s" % (full_dir)
       cmd  = "%s %s" % (env,eos)
       proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
       stdout, stderr = proc.communicate()
       #print("\t\tCreated directory using the command [%s]" % cmd )
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
def _CopyFilesFromdCache() :

    print( "\tEnter CopyFilesFromdCache")

    # get the files on dCache
    topdir = "/dcache/dune/scratch/users/twalton/UpgradedCTA/RandomTestGenericData/"

    if not os.path.isdir(topdir) :
       sys.exit( "\tThe directory [%s] does not exist. Cannot continue." % topdir )

    print( "\t\tloop over the subdirectories" )
    subdirs = os.listdir(topdir)
    subdirs = map(int, subdirs)
    subdirs.sort()
    subdirs.reverse()
    
    for subdir in subdirs :
        subpath = "%s/%d" % (topdir,subdir)
        if not os.path.isdir(subpath) :
           print( "\t\t warning: the subdirectory does not exist [%s]" % subpath )
           continue

        print( "\n\t\tchecking to skip the directory" )
        if _SkipSubdirectories( "%d" % subdir ) :
           print( "\n\t\tskipping the subdir [%s]" % subdir )
           continue

        print( "\n\t\tgetting all of the files from subdirectory [%s]" % subpath )

        tmp_files  = _GetAllFiles(subpath)
  
        print( "\n\t\tremoving empty files" )
        removeZeroByteFiles( tmp_files )

        print( "\t\t\tnumber of files is [%d]" % len(tmp_files) )
        print( "\t\tremoving files found on test stand machine" )

        fake_files = _GetFilesNotCopyToStorageMachine(tmp_files)
        print( "\t\tcopying the remaining files [%d] to test stand machine" % len(fake_files) )

        if len(fake_files) == 0 :
           print( "\tThere are not any files to copy" )
           continue

        worker_nodes = 8

        print( "\t\ttime for starting [%s]" % dt.datetime.now().strftime("%Y-%m-%d-%H-%M-%S") )

        pool   = Pool(processes=worker_nodes) 
        result = pool.map(_CopyFilesFromdCacheTask,enumerate(fake_files))

        print("\t\tfinished the copying")

        pool.close()
        pool.join()

        print( "\t\ttime for ending [%s]" % dt.datetime.now().strftime("%Y-%m-%d-%H-%M-%S") )
        print( "\t\tstop copying for 30 minutes")
        sleep(1800)

        print( "\t\tcheck if all files are on tape" )
        success = _CheckFilesStatus(subdir)
        if not success :
           sys.exit( "All files are not on tape... exiting..\n\n" )
 

    print( "\tExit CopyFilesFromdCache")


#-----------------------------------------------------------------------------
#
# main function
#
#-----------------------------------------------------------------------------
if __name__ == '__main__' :

   print( "\nEnter write random fake data to tape\n" )

   _CopyFilesFromdCache()

   print( "Exit write random fake data to tape\n" )
