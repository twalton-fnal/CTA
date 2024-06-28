#!/usr/bin/env python

import os, sys, signal, string, re, shutil, math, subprocess, json
import copy
import datetime as dt
import matplotlib.pyplot as plt


if __name__ == '__main__' :

   print( "\nEnter making plots for files on EOS dev\n" )

   # eos environment
   env = "EOS_MGM_URL=root://storagedev201.fnal.gov XrdSecPROTOCOL=sss XrdSecSSSKT=/home/eos/cta_twalton.keytab"

   # get the subdirs
   config = "june2024" #"spring2024"
   topdir = "/eos/ctaeos/cta/users/twalton/%s/data/" % config

   eos    = "eos ls %s" % topdir
   cmd    = "%s %s" % (env,eos)
   proc   = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
   stdout, stderr = proc.communicate()

   subdirs = stdout.decode("utf-8").split("\n")


   # loop over directories
   for s, subdir in enumerate(subdirs) :
       if len(subdir) == 0 : continue
       spath = "%s/%s" % (topdir,subdir) 
       
       fnames   = []
       eospaths = []
       tmp      = []

       tmp.append( spath )

       name = ""
       if subdir == "novafiles" :
          name = "rand_nova_fake_file"
       elif subdir == "randomfiles" :
          name = "rand_dev_file"
       else :
          continue

       cur_subdir = subdir

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
                fnames = tmp[:]
                break


       print( "\tSubdir [%s], number of files [%d]" % (spath,len(fnames)) )

       month = "May"
       if config == "june2024" :
          month = "Jun"

       # get the byte information
       bdata = []
       for f, fname in enumerate(fnames) :
           eos  = "eos ls -lhy %s" % fname
           cmd  = "%s %s" % (env,eos)
           proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
           stdout, stderr = proc.communicate()

           elem  = stdout.decode("utf-8").split("\n")[0].split(" ") 
           binfo = ""

           if elem[0] == "d1::t1" :
              idx = elem.index(month)
              binfo = "%s %s" % (elem[idx-2],elem[idx-1])           
           else : 
              continue   

           if binfo.find("k") != -1 :
              bdata.append( float(binfo.split(" ")[0]) * 1e-3 )
           elif binfo.find("M") != -1 :
              bdata.append( float(binfo.split(" ")[0]) )

           if f%5000 == 0 :
              print( "\t\t\tat line [%d], byte [%s]" % (f,binfo) )

       print( "\t\tnumber of data points [%d]" % len(bdata) )


       # make plots
       amin  = 0.
       amax  = 100

       if subdir == "novafiles" : 
        
          bin_edges = [ 0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0 ]
          for b in range(12, 102, 2) :
              bin_edges.append( b )    

          figure, axis = plt.subplots()
          plt.figure()
          plt.xlabel( 'File Size (MB)' )
          plt.ylabel( 'Number of Files' )

          plt.gca().ticklabel_format(axis='y', style='sci', scilimits=(0, 0))
          plt.title( "NOvA Analysis Fake Dataset" )
          plt.hist(x=bdata,bins=bin_edges,color='m',range=(amin,amax))
          plt.savefig("NovaFakeDatasetOnEos%s.png" % config)

       elif subdir == "randomfiles" : 
        
          bin_edges = [ 0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0 ]

          print(len(bin_edges))
          print(len(bdata))

          figure, axis = plt.subplots()
          plt.figure()
          plt.xlabel( 'File Size (MB)' )
          plt.ylabel( 'Number of Files' )

          pcolor = 'c'
          if config == "june2024" : pcolor = 'g'

          plt.gca().ticklabel_format(axis='y', style='sci', scilimits=(0, 0))
          plt.title( "Single Data Bins Dataset" )
          plt.hist(x=bdata,bins=bin_edges,color=pcolor,range=(amin,amax))
          plt.savefig("SingleBinDatasetOnEos%s.png" % config)



   print( "\nExit making plots for files on EOS dev\n" )

