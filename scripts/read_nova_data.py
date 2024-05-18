import os, sys, signal, string, re, shutil, math, subprocess, json
import copy
import datetime as dt
import multiprocessing as mp

from multiprocessing.pool import ThreadPool as Pool
from time import sleep

# use python 3

"""
get the directory names
"""
def _GetEosDirectories() :

    env  = "EOS_MGM_URL=root://storagedev201.fnal.gov XrdSecPROTOCOL=sss XrdSecSSSKT=/home/eos/cta_twalton.keytab"

    dnames   = []
    eospaths = []
    tmp      = []

    tmp.append( FILEDIR )

    cur_subdir = "novafiles"
    name       = "2024"

    while cur_subdir.find( name ) == -1 :

          for t in tmp :
              eos  = "eos ls %s" % (t)
              cmd  = "%s %s" % (env,eos)
              proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
              stdout, stderr = proc.communicate()
              edirs = stdout.decode("utf-8").split("\n")
              for e in edirs :
                  if len(e) == 0 : continue
                  eospaths.append( "%s/%s" % (t,e) )

          tmp.clear()
          tmp = eospaths[:]
          eospaths.clear()
          cur_subdir = tmp[0].split("/")[-1]

          if cur_subdir.find( name ) != -1 :
             dnames = tmp[:]
             break

    return dnames




"""
get the directory files
"""
def _GetFilesOnTape( directory ) :

    env  = "EOS_MGM_URL=root://storagedev201.fnal.gov XrdSecPROTOCOL=sss XrdSecSSSKT=/home/eos/cta_twalton.keytab"
    eos  = "eos ls -lhy %s" % (directory)
    cmd  = "%s %s" % (env,eos)
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)

    stdout, stderr = proc.communicate()

    fileinfo = stdout.decode("utf-8").split("\n")
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
    print( "\t\tPreparing the files for read" )

    for i, ifile in enumerate(files) :
        eos  = "xrdfs root://storagedev201.fnal.gov prepare -e %s" % ifile
        cmd  = "%s %s" % (env,eos)
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        if i%500 == 0 :
           print( "\t\t\tat line [%d] and command [%s]" % (i,eos) )
        stdout, stderr = proc.communicate()

        error = stderr.decode("utf-8").split("\n")[0]
        if len(error) != 0 :
           print( "\t\tERROR: at line [%s], the file [%s] cannot be evicted from disk, and error is %s" % (i,ifile,error) )
           continue

    for i, ifile in enumerate(files) : 
        eos  = "xrdfs root://storagedev201.fnal.gov prepare -s %s" % ifile
        cmd  = "%s %s" % (env,eos)
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        if i%500 == 0 :
           print( "\t\t\tat line [%d] and command [%s]" % (i,eos) )
        stdout, stderr = proc.communicate()

        error = stderr.decode("utf-8").split("\n")[0]
        if len(error) != 0 :
           print( "\t\tERROR: at line [%s], the file [%s] cannot be prepared for staging, and error is %s" % (i,ifile,error) )
           continue

    print( "\tExit PrepareFilesForRead" )


 
##########################################################################
#  main block 
##########################################################################

if __name__ == '__main__' :

   print( "Enter Read Analysis of the Nova Fake Data\n" )

   global DFILEDIR
   FILEDIR = "/eos/ctaeos/cta/users/twalton/spring2024/data/novafiles/"

   directories = _GetEosDirectories()
   print( "Retrive all directories [%d]" % len(directories) )

   for d, directory in enumerate(directories) :
       print( "\tGetting files from directory [%s]" % directory )

       filesOnTape = _GetFilesOnTape(directory) 
       print( "\tThe total number of files [%d]" % len(filesOnTape) )

       _PrepareFilesForRead(filesOnTape) 
       
   print( "Exit Read Analysis of the Nova Fake Data\n" )
   
