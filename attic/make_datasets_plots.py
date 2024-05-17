import os, sys, string, re, shutil, math, subprocess, json

from array import array

import datetime as dt
import matplotlib.pyplot as plt
import matplotlib.ticker as tick

"""
make the plots
"""
def _MakeByteDataPlots( nova_data_list, fake_data_list ) :

    print( "\tEnter make data plots\n\n")

    bytes2mb      = 1e-6
    mb_threshold  = 100

    sfake_data_mb = []
    snova_data_mb = []

    lfake_data_mb = []
    lnova_data_mb = []


    for f in fake_data_list[0] : 
        value = f * bytes2mb
        if value < mb_threshold :
           sfake_data_mb.append( value )
        else :
           lfake_data_mb.append( value )


    for n in nova_data_list[0] :
        value = n * bytes2mb
        if value < mb_threshold :
           snova_data_mb.append( value )
        else :
           lnova_data_mb.append( value )

    xmin  = nova_data_list[1] * bytes2mb
    xmax  = nova_data_list[2] * bytes2mb

    amin  = [ 0,            mb_threshold,  0,            mb_threshold  ]
    amax  = [ mb_threshold, 5000,          mb_threshold, 5000 ]

    nbins = int(mb_threshold / 2)


    # draw


    data   = [ snova_data_mb, lnova_data_mb, sfake_data_mb, lfake_data_mb ]
    colors = [ 'b', 'r', 'tab:orange', 'tab:purple' ]

    titles = [   "NOvA Analysis Dataset : File Size < %d MBytes" % (mb_threshold)
               , "NOvA Analysis Dataset : File Size > %d MBytes" % (mb_threshold)
               , "Fake Nova Dataset : File Size < %d MBytes" % (mb_threshold)
               , "Fake Nova Dataset : File Size > %d MBytes" % (mb_threshold) 
             ]

    pnames = [   "NovaDatasetSmallFileSize.png"
               , "NovaDatasetLargeFileSize.png"
               , "FakeNovaDatasetSmallFileSize.png"
               , "FakeNovaDatasetLargeFileSize.png"
             ]


    for i, d in enumerate(data) :
        figure, axis = plt.subplots()
    
        plt.figure()

        plt.xlabel( 'File Size (MB)' )
        plt.ylabel( 'Number of Files' ) #, fontsize=12 )
    
        #axis.yaxis.set_major_formatter(tick.FormatStrFormatter('%.3g'))
        plt.gca().ticklabel_format(axis='y', style='sci', scilimits=(0, 0))

        plt.title( titles[i] )

        plt.hist(x=d,bins=nbins,color=colors[i],range=(amin[i],amax[i]))

        plt.savefig(pnames[i])



"""
get information from the fake data
"""
def _GetNovaFakeData() :

    print( "\tEnter get nova fake data")

    fake_file_sizes = []
    count_files_tb  = 0.
    nfiles          = 0
    i               = 0

    # get top directory of the store files
    topdir = "/pnfs/dune/scratch/users/twalton/CTA/NovaTest/"
    if not os.path.isdir(topdir) :
       sys.exit( "ERROR::the top directory [%s] does not exist" % topdir )


    # get subdirectories
    subdirs = os.listdir(topdir)
    for subdir in subdirs :
        fnames  = os.listdir("%s/%s/" % (topdir,subdir))
        nfiles += len(fnames)

        for fname in fnames :
            fstats           = os.stat( "%s/%s/%s" % (topdir,subdir,fname) )
            created_filesize = fstats.st_size
            cfsize_tb        = float(created_filesize) * 1e-12
            count_files_tb  += cfsize_tb

            fake_file_sizes.append( float(created_filesize) )


            if i%5000 == 0 :
               print( "\t\tAt line [%d], filename [%s], size [ %d bytes ]" % (i,fname,created_filesize) )
            i += 1


    print( "\tCompleted getting stats for nfiles [%d], with total bytes size [%.3f TB]" % (nfiles,count_files_tb) )
    return [ fake_file_sizes ]



"""
get information for nova analysis dataset
"""
def _GetNovaData() :

    print( "\tEnter get nova data")

    nova_file_sizes = []
    total_file_sizes = 0
    count_files_below_threshold = 0
    mb_threshold  = 1e8

    # open file and get file sizes
    tdir  = "/home/eos/prometheus/analysis/txt"
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

        nova_file_sizes.append(int(lwords[2]))

        total_file_sizes += int(lwords[2])

        if int(lwords[2]) <= mb_threshold :
           count_files_below_threshold += 1

        if int(lwords[2]) < min_file_size :
           min_file_size = int(lwords[2])
           min_filename  = lwords[0]
           im = l

        if int(lwords[2]) > max_file_size :
           max_file_size = int(lwords[2])
           max_filename  = lwords[0]
           fm = l

    fnova.close()

    print( "\tCompleted reading lines [%d], with total files giving [%d] size (byte)" % ( len(lnova),total_file_sizes ) )

    file_size_tb = total_file_sizes * 1e-12
    min_file_size_mb = float(min_file_size) * 1e-6
    max_file_size_mb = float(max_file_size) * 1e-6

    print( "\t\tdataset in TB [%.3f]" % (file_size_tb) )
    print( "\t\tmin and max file sizes [(%d) %s :: %.3f bytes, (%s) %s :: %.3f bytes]\n" % (im,min_filename,min_file_size,fm,max_filename,max_file_size) )
    print( "\t\tnumber of files less than 100 MB is %d" % count_files_below_threshold )

    return [ nova_file_sizes, min_file_size_mb, max_file_size_mb ]



#-----------------------------------------------------------------------------
#
# the main function
#
#-----------------------------------------------------------------------------
if __name__ == '__main__' :

   print( "\nCreate fake data based on NOvA analysis dataset\n\n")

   # get the nova data
   print( "Nova Data" )
   nova_data = _GetNovaData()
   print( "\tentries [%d], minimum file size [%.3f], maximum file size [%.3f]" % (len(nova_data[0]),nova_data[1],nova_data[2]) )
   print( "\n\n" )
   

   # get the fake data
   print( "Fake Data" )
   fake_data = _GetNovaFakeData()
   print( "\tentries [%d]" % len(fake_data[0]) )
   print( "\n\n" )


   # make plots
   print( "Make Plots" )
   _MakeByteDataPlots(nova_data,fake_data)


   print( "\n\nCompleted making plots\n\n" )
