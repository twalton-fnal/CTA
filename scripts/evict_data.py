import os, sys, signal, string, re, shutil, math, subprocess, json
import copy
import datetime as dt


if __name__ == '__main__' :

   print( "\nEnter evict data from disk." )

   env    = "EOS_MGM_URL=root://storagedev201.fnal.gov XrdSecPROTOCOL=sss XrdSecSSSKT=/home/eos/cta_twalton.keytab"

   # get the subdirs
   topdir = "/eos/ctaeos/cta/users/twalton/data/nova"

   eos  = "eos ls %s" % topdir
   cmd  = "%s %s" % (env,eos)
   proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
   stdout, stderr = proc.communicate()

   subdirs = stdout.split("\n")
   fnames  = []

   # loop over subdirs and get files
   for subdir in subdirs :
       if len(subdir) == 0 : continue
       fullpath = "%s/%s" % (topdir,subdir)

       eos  = "eos ls %s" % fullpath
       cmd  = "%s %s" % (env,eos)
       proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
       stdout, stderr = proc.communicate()

       filenames = stdout.split("\n")

       for filename in filenames :
           if len(filename) == 0 : continue
           fnames.append( "%s/%s/%s" % (topdir,subdir,filename) )

   # loop over file names and evict
   for f, fname in enumerate(fnames) :
       if f%100 == 0 :
          print( "\t\tat line [%d] and evicting file [%s]" % (f,fname) )

       eos  = "xrdfs root://storagedev201.fnal.gov prepare -e %s" % fname
       cmd  = "%s %s" % (env,eos)
       proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
       stdout, stderr = proc.communicate()


   print( "Exit evict data from disk." )

