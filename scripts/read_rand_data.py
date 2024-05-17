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

    subdirs = map(int, subdirs)
    subdirs = sorted(subdirs,reverse=True)    

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
        if fi.find("d1::t1") != -1 : 
           tfiles.append( "%s/%s" % (directory,fi.split(" ")[-1]) )

    return tfiles


  
"""
prepare the files for read
"""
def _PrepareFilesForRead( files ) :

    env = "EOS_MGM_URL=root://storagedev201.fnal.gov XrdSecPROTOCOL=sss XrdSecSSSKT=/home/eos/cta_twalton.keytab"
 
    print( "\t\tDropping the files" )
    for i, ifile in enumerate(files) :
        #eos  = "eos stagerrm %s" % ifile 
        eos  = "xrdfs root://storagedev201.fnal.gov prepare -e %s" % ifile
        cmd  = "%s %s" % (env,eos)
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)

        if i%500 == 0 :
           print( "\t\t\tat line [%d] and command [%s]" % (i,eos) )

        stdout, stderr = proc.communicate()

 
    print( "\t\tCheck if the files are dropped from disk" )
    failed = []

    for i, ifile in enumerate(files) :
        eos  = "eos ls -lhy %s" % ifile
        cmd  = "%s %s" % (env,eos)
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)

        if i%500 == 0 :
           print( "\t\t\tat line [%d] and command [%s]" % (i,eos) )

        stdout, stderr = proc.communicate()
        fileinfo = stdout.strip()
        if fileinfo.find( "d0::t1" ) == -1 : failed.append( ifile )
       
 
    print( "\t\tNumber of files still on disk is [%d]" % len(failed) )
    if len(failed) == len(files) :
       sys.exit( "\tERROR: None of the files are removed from disk." )


    print( "\t\tTell eos to read the files" )
    for i, ifile in enumerate(files) :
        if ifile in failed : continue  

        eos  = "xrdfs root://storagedev201.fnal.gov prepare -s %s" % ifile
        cmd  = "%s %s" % (env,eos)
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)

        if i%500 == 0 :
           print( "\t\t\tat line [%d] and command [%s]" % (i,eos) )

        stdout, stderr = proc.communicate()
    
    print( "\tExit PrepareFilesForRead" )


 
##########################################################################
#  main block 
##########################################################################

if __name__ == '__main__' :

   print( "Enter Read Analysis of the Data\n" )

   global RANDFILEDIR
   RANFILEDIR = "/eos/ctaeos/cta/users/twalton/spring2024/data/randomfiles/"

   directories = _GetEosDirectories()
   print( "Retrive all directories [%d]" % len(directories) )


   for d, directory in enumerate(directories) :
       if d != 0 : continue

       print( "\tGetting files from directory [%s]" % directory )

       filesOnTape = _GetFilesOnTape(directory) 
       print( "\tThe total number of files [%d]" % len(filesOnTape) )
 
       _PrepareFilesForRead(filesOnTape) 

       print( "\t\tstop reading for 60 minutes")
       sleep(3600)


   print( "Exit Read Analysis of the Data\n" )
   
