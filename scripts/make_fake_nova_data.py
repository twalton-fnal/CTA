#!/usr/local/bin python


import os, sys, signal, string, re, shutil, math, subprocess, json

import datetime as dt
import multiprocessing as mp
import itertools 

import numpy as np
from numpy import random
import matplotlib.pyplot as plt
import scipy as scipy
from scipy.optimize import curve_fit

from argparse import ArgumentParser as ap

PWD = str(os.environ.get('PWD'))
USER = str(os.environ.get('USER'))



"""
initializer for multiprocessing
"""
def _PoolInit(l):
    global lock
    lock = l

"""
get the plot directory
"""
def _GetPlotDirectory() :
    plotdir = PWD.replace("scripts","plots")
    if not os.path.isdir(plotdir) :
       print( "\t\tCreating the plot directory [%s]" % plotdir )
       os.makedirs(plotdir)
    return plotdir



"""
get the number of files in the top directory
"""
def _GetTopDirectoryFileCount() :
    count_files = 0
    if not os.path.isdir(TOPDIR) : return 0

    subdirs = os.listdir(TOPDIR)
    for subdir in subdirs : 
        fnames = os.listdir("%s/%s/" % (TOPDIR,subdir))
        count_files += len(fnames)

    return count_files


"""
get the size of files in directory
"""
def _GetTopDirectorySize() :
    count_files_tb = 0.
    if not os.path.isdir(TOPDIR) : return count_files_tb

    subdirs = os.listdir(TOPDIR)
    for subdir in subdirs : 
        fnames = os.listdir("%s/%s/" % (TOPDIR,subdir))
        for fname in fnames :
            fstats           = os.stat( "%s/%s/%s" % (TOPDIR,subdir,fname) )
            created_filesize = fstats.st_size
            cfsize_tb        = float(created_filesize) * 1e-12
            count_files_tb  += cfsize_tb

    return count_files_tb
   

"""
make plots
"""
def _MakeNovaPlot( ndata, pname, color ) :

    print( "\tEnter make data plots")

    mb_threshold  = 100
   
    nbins = int(mb_threshold / 2)
    amin  = 0.
    amax  = 100

    bin_edges = [ 0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0 ]
    for b in range(12, 102, 2) :
       bin_edges.append( b )
   
    title = "NOvA Analysis Dataset : File Size < 100 MB"

    figure, axis = plt.subplots()

    plt.figure()
    plt.xlabel( 'File Size (MB)' )
    plt.ylabel( 'Number of Files' ) #, fontsize=12 )
    plt.gca().ticklabel_format(axis='y', style='sci', scilimits=(0, 0))
    plt.title( title )

    ( n )  = plt.hist(x=ndata,bins=bin_edges,color=color,range=(amin,amax))

    plotdir   = _GetPlotDirectory()
    full_name = "%s/%s" % (plotdir,pname)
    plt.savefig(full_name)

    print( "\t\tCreated the file [%s]" % full_name )

    ydata = n[0]
    xdata = n[1]
    xdata = xdata[:-1]

    print( "\tExit make data plots\n\n")
    return [ ydata, xdata ]


""" 
parse the Nova analysis dataset
"""
def _GetNovaData() :

    print( "\tEnter creating fake data based on NOvA analysis dataset\n\n")

    fake_file_sizes = []
    total_file_sizes = 0

    # open file and get file sizes
    tdir  = PWD.replace("scripts","data") 
    fnova = open("%s/nova.files.txt" % tdir, "r")
    lnova = fnova.readlines()

    min_file_size = 100000000000000
    max_file_size = 0

    min_filename  = ""
    max_filename  = ""  

    im = 0
    fm = 0

    for l, lline in enumerate(lnova) :
        if l%50000 == 0 :
           print( "\t\tAt line %d : [%s]" % (l,lline.strip()) )

        lwords = lline.strip().split("\t")

        fake_file_sizes.append([lwords[0],int(lwords[2]),int(lwords[3])] )
        total_file_sizes += int(lwords[2])

        if int(lwords[2]) < min_file_size :
           min_file_size = int(lwords[2])
           min_filename  = lwords[0]
           im = l

        if int(lwords[2]) > max_file_size :
           max_file_size = int(lwords[2])
           max_filename  = lwords[0]
           fm = l

    fnova.close()

    print( "\n\tCompleted reading lines [%d], with total files giving [%d] size (byte)" % ( len(lnova),total_file_sizes ) )

    file_size_tb = total_file_sizes * 1e-12
    min_file_size_mb = min_file_size 
    max_file_size_mb = max_file_size
    
    print( "\t\tdataset in TB [%.3f]" % (file_size_tb) )
    print( "\t\tmin and max file sizes [(%d) %s::%.3f bytes, (%s) %s::%.3f bytes]\n" % (im,min_filename,min_file_size_mb,fm,max_filename,max_file_size_mb) )

    return fake_file_sizes


"""
return file sizes only
"""
def _GetNovaDataFileSizes( nova_data ) :

    file_sizes = []
    for nova in nova_data :
        fsize_mb = float(nova[1]) * 0.000001
        if fsize_mb <= 100 :
           file_sizes.append( fsize_mb )
  
    return file_sizes


"""
create a function for the data
"""
def _FitFunc( x, mu1, sigma1, A, mu2, sigma2, B ) :
    func = A*np.exp(-(x-mu1)**2/2/sigma1**2) + B*np.exp(-(x-mu2)**2/2/sigma2**2)
    return func

def _GaussFit( x, mu, sigma, c ) :
    func = c * np.exp(-(x-mu)**2/2/sigma**2)
    return func

def _ManyPeaksGaussFit( x, *params ) :
    y = np.zeros_like(x)
    for i in range(0, len(params), 3 ) :
        mean   = params[i]
        width  = params[i+1]
        height = params[i+2]
        y = y + _GaussFit( x, mean, width, height ) 
    return y


"""
fit the nova data
"""
def _FitNovaData( ydata, xdata ) :

    print( "\tEnter fit the data")

    xdata = np.asarray(xdata)
    ydata = np.asarray(ydata)

    # initial values
    initial = [ 10., 5., 70000, 25, 8, 25000, 65, 1, 8000, 85, 3, 175000, 95, 1, 100000 ]

    parameters, covariance = curve_fit(_ManyPeaksGaussFit, xdata, ydata, p0=initial) #,bounds=(0,15) )
    fresults = _ManyPeaksGaussFit( xdata, *parameters )

    plt.clf()
    plt.plot(xdata, ydata, 'o', label='data')
    plt.plot(xdata, fresults, '-', label='fit')
    plt.legend()

    pname     = "NovaDatasetSmallFileSizeFit.png" 
    plotdir   = _GetPlotDirectory()
    full_name = "%s/%s" % (plotdir,pname)

    plt.savefig(full_name)
    print( "\t\tCreated the file [%s]" % full_name )
    print( "\tExit fit the data\n\n")

    return fresults



"""
get probabilty for each data bin
"""
def _GetBinProbability( xdata, ydata ) :

    print( "\tEnter get the probability per bin" )

    total = 0.
    for y in ydata :
        total += y

    values = []
    for i, y in enumerate(ydata) :
        p = y / total

        """
        xf = 105. if i+1 >= len(ydata) else xdata[i+1] 
        xi =  xdata[i]
        x  = ((xf-xi)/2.0) + xi
        """
        x = xdata[i]
        values.append( [x,p] ) 

    print( "\t\tRetrieve [%d] values." % len(values) )
    print( "\tExit get the probability per bin\n\n" )
    return values 


"""
generate distribution based on nova data
"""
def _GenerateNovaFakeData( probs, prob_size ) :

    print( "\tEnter generate the fake distribution" )

    fdata = []
    fprob = []

    for prob in probs :
        fdata.append( prob[0] )
        fprob.append( prob[1] )

    datavolume = MAX_FILES_TB * 1.0e6
    total      = 0.
    results    = []
   
    while total < datavolume : 
       values = random.choice(fdata,p=fprob,size=(prob_size))
       for value in values :
           total += value
       results.extend(values)

    pname = "NovaDatasetSmallFileRandomChoice.png"
    color = "r"
    _MakeNovaPlot( results, pname, color )

    print( "\t\tGenerate [%d] numerical values" % len(results) )
    print( "\tExit generate the fake distribution\n\n" )
    return results



"""
create dataset with fake nova-like data
"""
def _GenerateNovaFakeDataFiles( data, min_filesize, max_filesize ) :

    print( "\tEnter CreateNovaFakeData with ndata [%d]\n" % len(data) )
    print( "\t\tcpu count=[%d]" % mp.cpu_count() )

    l = mp.Lock()

    max_files_dir = 2000 
    if len(data) < max_files_dir : max_files_dir = len(data)

    adata = []
    for a in range(0,len(data),max_files_dir) :
        tmp = []
        amin = a
        amax = amin + max_files_dir
        for b in range(amin,amax) :
            if b < len(data) : 
               if data[b] >= min_filesize and data[b] <= max_filesize :
                  tmp.append( data[b] )
        adata.append( tmp )

    print( "\t\tnumber of subdirectories [%d] with the maximum number files: [%d]\n" % (len(adata),max_files_dir) )

    try :
      pool   = mp.Pool(processes=mp.cpu_count(),initializer=_PoolInit,initargs=(l,))
      result = pool.starmap(_CreateNovaFakeDataFilesTask,enumerate(adata))
    finally :
      pool.close()
      pool.join()

    print( "\tExit CreateNovaFakeData\n" )


"""
create dataset with fake nova-like data
"""
def _CreateNovaFakeDataFilesTask( i, idata ) :

    # verbosity
    print( "\t\tat line [%d] in enter CreateNovaFakeDataTask" % i )

    # create the subdirectory
    subpath = "%s/%s-%d" % (TOPDIR,dt.datetime.now().strftime("%Y-%m-%d-%H-%M-%S"),i)
    if not os.path.isdir(subpath) :
       print( "\t\t creating the output directory [%s]" % subpath )
       os.makedirs(subpath)
    else :
       print( "\t\t storing files in the output directory [%s]" % subpath )

   
    # loop over data and create fake files
    for f, fsize in enumerate(idata) :

        if f%500 == 0 :
           print( "\t\t\tat line [%d] for creating fake nova data" % f )
 
        count     = 1024 if fsize < 1.0 else 1000
        index     = i * len(idata) + f 
        data_byte = int(fsize * count)
        date      = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename  = "rand_nova_fake_file_%s_%d" % (date,index)
        fpath     = "%s/%s" % (subpath,filename)

        command   = "dd if=/dev/urandom of=%s bs=%d count=%d" % (fpath,data_byte,count)
        process   = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        pipe      = process.communicate()[0].split()

    # end
    return True


"""
plot the fake data
"""
def _MakeNovaFakeFilesPlot() :

    print( "\tEnter make plot for generated data" )
    fdata = []

    subdirs = os.listdir(TOPDIR)
    for subdir in subdirs :
        fpath      = "%s/%s" % (TOPDIR,subdir)
        fake_files = os.listdir(fpath)

        print( "\t\tat subdir [%s : %d]" % (fpath,len(fake_files)) )

        for fake_file in fake_files :
            fstats  = os.stat( "%s/%s" % (fpath,fake_file) )
            created_filesize = fstats.st_size
            cfsize_mb        = float(created_filesize) * 1e-6
            fdata.append( cfsize_mb )

    pname = "NovaDatasetFakeSmallFiles.png"
    color = "orange"
    _MakeNovaPlot(fdata,pname,color)

    print( "\tExit make plot for generated data\n\n" )




#-----------------------------------------------------------------------------
#
# main function
#
#-----------------------------------------------------------------------------
if __name__ == '__main__' :

   print( "Create fake data based on NOvA analysis dataset\n\n")


   # input arguments
   parser = ap()
   parser.add_argument('--outdir', type=str, default="/pnfs/dune/scratch/users/%s/CTA_LTO9/NovaFakeData/" % USER, help="The location of the output data [default: %default]")
   parser.add_argument('--size', type=float, default=4.0, help="The size of the fake dataset in TB (default: %default TB)")
   parser.add_argument('--min', type=float, default=10., help="The minimum file size to generate, where the default is %default MB" )
   parser.add_argument('--max', type=float, default=100., help="The maximum file size to generate, where the default is %default MB" )
   parser.add_argument('--prob_size', type=int, default=1000, help="The size parameter in the random choice function, where the default is %default" )
   args = parser.parse_args()


   # store files in this directory
   outdir = args.outdir
   if not os.path.isdir(outdir) :
      print( "\tCreating the top output directory [%s]" % outdir )
      os.makedirs(outdir)
   global TOPDIR
   TOPDIR = outdir

   # max number of files to create
   global MAX_FILES_TB
   MAX_FILES_TB = args.size

   # get the nova data
   nova_data = _GetNovaData()

   # get the sizes only
   file_sizes = _GetNovaDataFileSizes( nova_data )

   # plot the data
   pdata = _MakeNovaPlot(file_sizes,"NovaDatasetSmallFileSize.png","b")

   # fit the data
   _FitNovaData(pdata[0],pdata[1])

   # get the probability for each bin
   probs = _GetBinProbability(pdata[1],pdata[0])

   # generate fake data
   fake_values = _GenerateNovaFakeData( probs, args.prob_size )   

   # generate rand files
   _GenerateNovaFakeDataFiles( fake_values, args.min, args.max )

   # plot distribution of fake files
   _MakeNovaFakeFilesPlot() 

   # count the dataset size
   nfiles = _GetTopDirectoryFileCount()

   # fake dataset size
   count_files_tb = _GetTopDirectorySize()

   print( "\tSuccessful created a fake dataset size [%d, %.3f TB]" % (nfiles,count_files_tb) )
   print( "\nCompleted creating fake data based on NOvA analysis dataset\n\n")
