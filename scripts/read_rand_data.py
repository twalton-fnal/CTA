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
get the directory names on the EOS or dCache disk
"""
def _GetDirectories( disk ) :
    env  = EOS_ENV if disk == "eos" else DCACHE_ENV
    ldir = f"eos ls {INPUT_DIR}" if disk == "eos" else f"ls {INPUT_DIR}" 
    cmd  = "%s %s" % (env,ldir)
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)

    stdout, stderr = proc.communicate()
    subdirs = stdout.strip().split("\n")

    subdirs = map(int, subdirs)
    subdirs = sorted(subdirs,reverse=True)    

    dirs = []
    for subdir in subdirs :
        dirs.append( "%s/%s" % (INPUT_DIR,subdir) )

    return dirs


"""
get the directory files
"""
def _GetFilesOnTapeFromEOS( directory ) :
    env  = EOS_ENV
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
get the directory files
"""
def _GetFilesOnTapeFromdCache( directory ) :
    env       = DCACHE_ENV
    filenames = os.listdir(directory)
    tfiles    = []

    for filename in filenames :
        dcache = "cat %s/\".(get)(%s)(locality)\"" % (directory,filename)
        cmd    = "%s %s" % (env,eos)
        proc   = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        stdout, stderr = proc.communicate()

        if stdout.strip().find("NEARLINE") != -1 :
           tfiles.append( "%s/%s" % (directory,filename) )

    return tfiles


  
"""
prepare the files for read
"""
def _PrepareFilesForEOSRead( files ) :
    print( "\t\tEnter preparing the files for EOS read tests" )
    env = EOS_ENV

    for i, ifile in enumerate(files) :
        eos  = "xrdfs root://storagedev201.fnal.gov prepare -e %s" % ifile
        cmd  = "%s %s" % (env,eos)
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        stdout, stderr = proc.communicate()

        if i%500 == 0 : print( "\t\t\tat line [%d] and command [%s]" % (i,eos) )

    print( "\t\tCheck if the files are dropped from disk" )
    failed = []
    for i, ifile in enumerate(files) :
        eos  = "eos ls -lhy %s" % ifile
        cmd  = "%s %s" % (env,eos)
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        stdout, stderr = proc.communicate()
        fileinfo = stdout.strip()

        if fileinfo.find( "d0::t1" ) == -1 : failed.append( ifile )
        if i%500 == 0 : print( "\t\t\tat line [%d] and command [%s]" % (i,eos) )

    print( "\t\tNumber of files still on disk is [%d]" % len(failed) )
    if len(failed) == len(files) :
       sys.exit( "\tERROR: None of the files are removed from disk." )

    print( "\t\tRequesting reads from EOS" )
    for i, ifile in enumerate(files) :
        if ifile in failed : continue  

        eos  = "xrdfs root://storagedev201.fnal.gov prepare -s %s" % ifile
        cmd  = "%s %s" % (env,eos)
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        stdout, stderr = proc.communicate()

        if i%500 == 0 : print( "\t\t\tat line [%d] and command [%s]" % (i,eos) )

    print( "\tExit preparing the files for EOS read tests" )


 
##########################################################################
#  main block 
##########################################################################

if __name__ == '__main__' :

   print( "Enter the reading the data from tape\n" )

   # input arguments
   parser = ap()
   parser.add_argument('--disk', type=str, default="dCache", require=True, help="The disk location to copy files (eos or dCache) [default=%default]")
   parser.add_argument('--indir', type=str, default="/pnfs/dune/scratch/users/%s/CTA_LTO9/SingleBinnedData/" % USER, help="The location of the data on disk [default: %default]")
   parser.add_argument('--nfiles', type=int, default=10000, help="The maximum number of files to copy from scratch to disk, where the default is %default")
   args = parser.parse_args()

   # EOS environment variables
   global EOS_ENV
   EOS_ENV ="EOS_MGM_URL=root://storagedev201.fnal.gov XrdSecPROTOCOL=sss XrdSecSSSKT=/home/eos/cta_twalton.keytab"

   # dcache environment variables
   global DCACHE_ENV
   DCACHE_ENV =""

   # input directory
   global INPUT_DIR
   INPUT_DIR = args.indir #"/eos/ctaeos/cta/users/twalton/%s/data/randomfiles/" % config

   # get the directories on disk
   directories = _GetDirectories(args.disk.lower())
   print( "\tRetrieve the directories [%d] on disk type [%s]" % (len(directories),disk) )

   # loop over directories
   for d, directory in enumerate(directories) :

       # get the files on the tape
       filesOnTape = ""
       if args.disk.lower() == "eos" :
          filesOnTape = _GetFilesOnTapeFromEOS(directory) 
       elif args.disk.lower() == "dcache" :
          filesOnTape = _GetFilesOnTapeFromdCache(directory)
       print( "\t\tGot [%d] number of files [%d] from the directory : [%s]" % (len(filesOnTape),directory) )

       # prepare the files for reads
       if agrs.disk.lower() == "eos" :
          _PrepareFilesForEOSRead() 

       print( "\t\tstop reading for 60 minutes")
       sleep(3600)

   print( "Exit the reading the data from tape\n" )
   
