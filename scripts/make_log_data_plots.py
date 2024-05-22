#!/usr/bin/env python


import sys, re, os, time
import matplotlib.pyplot as plt
from datetime import datetime


"""
Get the data by dates
"""
def _GetDataFilesExtRangeForWrite() :
    flist = [   ["May 16 13:25", "May 17 03:00"]
              , ["May 17 09:00", "May 18 01:00"]
            ]
    return flist



def _GetDataFilesExtRangeForRead() :
    flist = [   ["May 18 17:00", "May 19 10:00"]
            ]
    return flist



def _GetDataFilesExtRangeForReadQuarter() :
    flist = [   ["May 19 22:30", "May 20 07:00"]
            ]
    return flist


"""
Get the first and last line for reading in files
"""
def _GetStartAndEndLines( config ) :
    print( "\tEnter get the first and last lines of the log files")


    linerange = []
    search    = []

    if config == "write" :
       search   = _GetDataFilesExtRangeForWrite() 
       filename = "/exp/dune/data/users/twalton/CTAMessages/messagesMay2024/messages-20240518" 
    elif config == "read" :
       search   = _GetDataFilesExtRangeForRead()
       filename = "/exp/dune/data/users/twalton/CTAMessages/messagesMay2024/messages-20240519"  
    elif config == "read25" :
       search   = _GetDataFilesExtRangeForReadQuarter()
       filename = "/exp/dune/data/users/twalton/CTAMessages/messagesMay2024/messages-20240520"  

    logfile  = open(filename,"r")
    loglines = logfile.readlines()

    print( "\t\tconfiguration [%s] and number of lines in [%s : %d]" % (config,filename,len(loglines)) )

    for range in search :
        index = []    
        for r in range :
            for l, logline in enumerate(loglines) :
                if logline.find(r) != -1 :
                   index.append( l )
                   break
        linerange.append(index)

    logfile.close()

    print( "\trange is", linerange )
    print( "\tExit get the first and last lines of the log files\n")
    return [linerange,filename]


"""
Extract the data from the log files
"""
def _ExtractByteDataFromFiles( config, configInfo ) :
    print( "\tEnter extract the data from log files")

    lineranges = configInfo[0]
    filename   = configInfo[1]

    logfile  = open(filename,"r")
    loglines = logfile.readlines()

    fileSize     = []
    transferTime = []
    driveTransferSpeedMBps = []
    positionTime = []

    fmessage = "fileSize"
    gmessage = "File successfully transmitted to drive"

    if config.find("read") != -1 : 
       gmessage = "File successfully read from tape"
       fmessage = "dataVolume"

    for linerange in lineranges :
        print("\t range is (%d, %d)" % (linerange[0],linerange[1]) )

        count = 0
        for l in range(linerange[0],linerange[1]) :
            if loglines[l].find(gmessage) == -1 : continue
            if loglines[l].find("rand_nova_fake_file") == -1 : continue
            loglist = loglines[l].split(" ")

            for elem in loglist :
                if   elem.find(fmessage)       != -1 : fileSize.append( float(elem.split("=")[1].replace("\"","")) )
                elif elem.find("transferTime") != -1 : transferTime.append( float(elem.split("=")[1].replace("\"","")) )            
                elif elem.find("driveTransferSpeedMBps") != -1 : driveTransferSpeedMBps.append( float(elem.split("=")[1].replace("\"","")) )
                elif elem.find("positionTime") != -1 : positionTime.append( float(elem.split("=")[1].replace("\"","")) )

            if count%10000 == 0 :
               if config.find("read") != -1 :
                  print( "\t\tat line [%d:%d], with file size = [ %.3f ], transfer time = [ %.3f ], drive transfer speed = [ %.3f ], position time = [ %.3f ]" % (count,l,fileSize[-1],transferTime[-1],driveTransferSpeedMBps[-1],positionTime[-1]) )
               else :
                  print( "\t\tat line [%d:%d], with file size = [ %.3f ], transfer time = [ %.3f ], drive transfer speed = [ %.3f ]" % (count,l,fileSize[-1],transferTime[-1],driveTransferSpeedMBps[-1]) )
            count += 1

    logfile.close()


    print( "\t file size [%d], transfer time [%d], drive transfer speed [%d]" % (len(fileSize),len(transferTime),len(driveTransferSpeedMBps)) )
    print( "\tExit extract the data from log files\n")
    return [ fileSize, transferTime, driveTransferSpeedMBps, positionTime ]



"""
organize the data by file size
"""
def _OrganizeDataByFileSize( bdata, config ) :
    print( "\tEnter organize the data by file size")

    filesizes = []
    tmp = []

    for b in bdata[0] :
        if b in tmp : continue
        tmp.append( b )

    filesizes = sorted(tmp)
    print( "\ttsorted the file sizes [ %d ]" % len(filesizes) )

    tdata = bdata[1]
    sdata = bdata[2]    
    pdata = bdata[3]
    data  = []

    for f, filesize in enumerate(filesizes) :
        time     = []
        speed    = []
        position = []

        for i, bsize in enumerate(bdata[0]) :
            if bsize != filesize : continue
            time.append(tdata[i])
            speed.append(sdata[i])

            if config.find("read") != -1 :
               position.append(pdata[i])
        

        avgTime     = sum(time)  / float(len(time))
        avgSpeed    = sum(speed) / float(len(speed))
        fsize       = float(filesize) * 1.0e-6
        avgPosition = 0. if config.find("read") == -1 else sum(position) / float(len(position))


        print( "\t\tfile size, average time, average speed, average position = ( %.4f, %.4f, %.4f, %.4f )" % (fsize,avgTime,avgSpeed,avgPosition) )
        data.append( [fsize,avgTime,avgSpeed,avgPosition] )

    print( "\tExit organize the data by file size\n")
    return data


"""
plot the data by file size
"""
def _PlotDataByFileSize( dataPerFileSize, config ) :
    print( "\tEnter plot the data by file size" )

    xdata = []
    ydata = []

    for bdata in dataPerFileSize :
        xdata.append( bdata[0] )
        ydata.append( bdata[2] )


    if config == "write" :
       plt.xlabel( 'File Size (MB)' )
       plt.ylabel( 'Average Drive Transfer Speed (MB/s)', fontsize=12 )

    style = 'bo-'
    if config == "read"     : style = 'ro-'
    elif config == "read25" : style = 'mo-'

    plt.plot(xdata, ydata, style)


    if config == "read25" :
       plt.legend(["Write", "Read", "Read 25% of Data"],loc='best')   
       plt.savefig('NovaFilesDriveTransferSpeed.png')
       plt.clf()

    print( "\tExit plot the data by file size\n" )


def _PlotReadDataByFilesize( dataPerFileSize, config ) :
    print( "\tEnter make only read data plot by file size" )

    xdata = []
    ydata = []

    for bdata in dataPerFileSize :
        xdata.append( bdata[0] )
        ydata.append( bdata[3] )


    if config == "read" :
       plt.xlabel( 'File Size (MB)' )
       plt.ylabel( 'Average Position Time (s)', fontsize=12 )

    style = ''
    if config == "read"     : style = 'ro-'
    elif config == "read25" : style = 'mo-'

    plt.plot(xdata, ydata, style)

    if config == "read25" :
       plt.legend(["Read", "Read 25% of Data"],loc='best')   
       plt.savefig('NovaFilesReadPositionTime.png')

    print( "\tExit make only read data plot by file size" )





##########################################################################
#  main block 
##########################################################################

if __name__ == '__main__' :

   print( "Enter Analysis of the Log Files\n" )

   dataPerFileSizeContainer      = []
   readOnlyDataFileSizeContainer = []

   for i in range(0,3) :

       if i == 0   : print("  Getting the data for writing files")
       elif i == 1 : print("  Getting the data for reading files")
       elif i == 2 : print("  Getting the data for reading 25% of the total files")

       config = ""
       if i == 0   : config = "write"
       elif i == 1 : config = "read"
       elif i == 2 : config = "read25"

       configInfo = _GetStartAndEndLines( config )
       bdata      = _ExtractByteDataFromFiles( config, configInfo )

       dataPerFileSize  = _OrganizeDataByFileSize(bdata,config)
       dataPerFileSizeContainer.append(dataPerFileSize)


   for i in range(0,3) :
       config = ""
       if i == 0   : config = "write"
       elif i == 1 : config = "read"
       elif i == 2 : config = "read25"
       _PlotDataByFileSize( dataPerFileSizeContainer[i], config )


   for i in range(0,3) :
       config = ""
       if i == 1   : config = "read"
       elif i == 2 : config = "read25"
       else : continue
       _PlotReadDataByFilesize( dataPerFileSizeContainer[i], config )


   print( "Exit Analysis of the Log Files\n" )
   
