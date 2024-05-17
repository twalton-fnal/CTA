import os, sys, signal, string, re, shutil, math, subprocess, json
import copy
import datetime as dt
import multiprocessing as mp

from multiprocessing.pool import ThreadPool as Pool
from time import sleep



"""
get the directory names
"""
def _GetEosDirectories() :

    env  = "EOS_MGM_URL=root://storagedev201.fnal.gov XrdSecPROTOCOL=sss XrdSecSSSKT=/home/eos/cta_twalton.keytab"
    eos  = "eos ls %s" % (RANFILEDIR)
    cmd  = "%s %s" % (env,eos)
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)

    stdout, stderr = proc.communicate()
    subdirs = stdout.strip().split("\n")
    
    dirs = []
    for subdir in subdirs :
        dirs.append( "%s/%s" % (RANFILEDIR,subdir) )

    return dirs


"""
get the directory files
"""
def _GetFilesOnTape( directory ) :

    env  = "EOS_MGM_URL=root://storagedev201.fnal.gov XrdSecPROTOCOL=sss XrdSecSSSKT=/home/eos/cta_twalton.keytab"
    eos  = "eos ls -lhy %s" % (directory)
    cmd  = "%s %s" % (env,eos)
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)

    stdout, stderr = proc.communicate()

    fileinfo = stdout.strip().split("\n")    
    tfiles   = []

    for fi in fileinfo :
        if fi.find("d1::t1") == -1 : continue
        tfiles.append( "%s/%s" % (directory,fi.split(" ")[-1]) )

    return tfiles


  
"""
prepare the files for read
"""
def _PrepareFilesForRead( files ) :

    env    = "EOS_MGM_URL=root://storagedev201.fnal.gov XrdSecPROTOCOL=sss XrdSecSSSKT=/home/eos/cta_twalton.keytab"
  
    """
    fs_ids = []

    print( "\t\tGetting the file fs ids" )
    for i, ifile in enumerate(files) :
        eos  = "eos fileinfo %s | grep storagedev201 | awk '{print $2}'" % (ifile)
        cmd  = "%s %s" % (env,eos)
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)

        if i%250 == 0 :
           print( "\t\t\tat line [%d] and command [%s]" % (i,eos) )

        stdout, stderr = proc.communicate()
        fs_id = stdout.strip()
        fs_ids.append( (ifile,fs_id) )
    """

    print( "\t\tDropping the files" )
    for fs_id in fs_ids :
        #eos = "eos file drop %s %s" % (fs_id[0],fs_id[1])
        eos = eos stagerrm %s" 
        cmd = "%s %s" % (env,eos)
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)

        if i%250 == 0 :
           print( "\t\t\tat line [%d] and command [%s]" % (i,eos) )

        stdout, stderr = proc.communicate()
 
   
    print( "\t\tTell eos to read the files" )
    for ifile in files :
        eos = "xrdfs root://localhost prepare -s ${fn}"
        cmd = "%s %s" % (env,eos)


 




"""
organize the data by file size
"""
def _OrganizeDataByFileSize( dataPerSessionID ) :
    print( "\tEnter OrganizeDataByFileSize")

    data = {}

    for key, values in dataPerSessionID.items() :
        print( "\t\tkey is %s MB" % key )

        count = 0
        b_t   = 0.
        for sessionid, metrics in values.items() :
            for m in metrics :
                if int(m[0]) == 0 : continue
                b_t   += float(m[1])
                count += 1

        load = 0.
        if count != 0 :
           load = (b_t / count) * 1e-6
 
        data[key] = load

        print( "\t\t\tvalue is %.3f MB" % load )


    print( "\t\tnumber of saved file sizes [%d]" % len(data) )
    print( "\tExit OrganizeDataByFileSize\n")
    return data








##########################################################################
#  main block 
##########################################################################

if __name__ == '__main__' :

   print( "Enter Read Analysis of the Data\n" )

   global RANDFILEDIR
   RANFILEDIR = "/eos/ctaeos/cta/users/twalton/data/random"

   directories = _GetEosDirectories()
   print( "\tRetrive all directories [%d]" % len(directories) )


   for directory in directories :
       print( "\tGetting files from directory [%s]" % directory )
 
       filesOnTape = _GetFilesOnTape(directory) 
       print( "\tThe total number of files [%d]" % len(filesOnTape) )
  
       _PrepareFilesForRead(filesOnTape) 
       sys.exit()

       print( "\t\tstop reading for 30 minutes")
       sleep(2400)


   print( "Exit Read Analysis of the Data\n" )
   
