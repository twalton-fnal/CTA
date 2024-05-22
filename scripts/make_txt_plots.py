#!/usr/bin/env python

import os, sys, signal, string, re, shutil, math, subprocess, json
import copy
import datetime as dt
import matplotlib.pyplot as plt


if __name__ == '__main__' :

   print( "\nEnter making plots using text files on EOS dev\n" )

   # eos environment
   env = "EOS_MGM_URL=root://storagedev201.fnal.gov XrdSecPROTOCOL=sss XrdSecSSSKT=/home/eos/cta_twalton.keytab"

   # container to store data
   bdata = []

   # text file directory
   txtdir   = "/home/eos/prometheus/analysis/txt"
   txtfiles = os.listdir(txtdir) 
   ntxt     = 0

   # loop over text files
   for t, txtfile in enumerate(txtfiles) :
       if txtfile.find("rand-nova-files") == -1 : continue

       txtpath  = "%s/%s" % (txtdir,txtfile)
       txtread  = open(txtpath,"r")
       txtlines = txtread.readlines()
       ntxt    += len(txtlines)

       if t%10 == 0 :
          print( "\tat line [%d], reading number of files [%d]" % (t,len(txtlines)) ) 

       for l, txtline in enumerate(txtlines) :
           eos = "eos ls -lhy %s" % txtline
           cmd  = "%s %s" % (env,eos)
           proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
           stdout, stderr = proc.communicate()

           elem  = stdout.decode("utf-8").split("\n")[0].split(" ") 
           binfo = ""

           if elem[0] == "d1::t1" :
              idx = elem.index("May")
              binfo = "%s %s" % (elem[idx-2],elem[idx-1])           
           else : 
              continue   

           if binfo.find("k") != -1 :
              bdata.append( float(binfo.split(" ")[0]) * 1e-3 )
           elif binfo.find("M") != -1 :
              bdata.append( float(binfo.split(" ")[0]) )

       txtread.close()
       
   print( "\tfinished getting the data for nfiles [%d : %d]" % (ntxt,len(bdata)) )

   btotal = 0
   for b in bdata :
       btotal += b
   btotal_tb = btotal * 1e-6
   print( "\t\tData size is %.4f TB" % btotal_tb )


   # make plots
   amin  = 0.
   amax  = 100

   bin_edges = [ 0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0 ]
   for b in range(12, 102, 2) :
       bin_edges.append( b )    

   figure, axis = plt.subplots()
   plt.figure()
   plt.xlabel( 'File Size (MB)' )
   plt.ylabel( 'Number of Files' )

   plt.gca().ticklabel_format(axis='y', style='sci', scilimits=(0, 0))
   plt.title( "NOvA Analysis Fake Dataset" )
   plt.hist(x=bdata,bins=bin_edges,color='g',range=(amin,amax))
   plt.savefig("NovaRandomFakeDatasetOnEos.png")


   print( "\nExit making plots using text files on EOS dev\n" )

