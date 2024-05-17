#!/usr/bin/env python

#---------------------------
# author: Ren Baueer
#---------------------------

import sys, re, os, time, datetime
import matplotlib.pyplot as plt

"""
Get the data by dates
"""

def _GetDataFilesExtRangeForWrite() :

    flist = [   ["2024-02-16-00-00-00", "2024-02-22-19-00-00", "100"] 
              , ["2024-02-22-21-30-00", "2024-02-23-13-30-00",  "90"]
              , ["2024-02-23-14-00-00", "2024-02-24-04-40-00",  "80"]
              , ["2024-02-24-05-00-00", "2024-02-25-23-00-00",  "70"]
              , ["2024-02-26-14-40-00", "2024-02-26-22-00-00",  "60"]
              , ["2024-02-26-08-00-00", "2024-02-26-14-00-00",  "50"]
              , ["2024-02-26-03-00-00", "2024-02-26-07-45-00",  "40"]
              , ["2024-02-25-11-05-00", "2024-02-26-02-30-00",  "30"]
              , ["2024-02-26-22-40-00", "2024-02-27-01-00-00",  "20"]
              , ["2024-02-27-01-30-00", "2024-03-14-18-00-00",  "10"]
              , ["2024-04-12-15-30-00", "2024-04-12-23-00-00",   "9"]
              , ["2024-04-15-07-00-00", "2024-04-19-00-00-00",   "8"]
              , ["2024-04-22-16-00-00", "2024-04-22-17-30-00",   "7"]
              , ["2024-04-22-18-00-00", "2024-04-22-19-15-00",   "6"]
              , ["2024-04-22-20-00-00", "2024-04-22-21-30-00",   "5"]
              , ["2024-04-22-22-15-00", "2024-04-22-23-30-00",   "4"]
              , ["2024-04-23-00-30-00", "2024-04-23-01-30-00",   "3"]
              , ["2024-04-23-02-00-00", "2024-04-24-15-00-00",   "2"]
              , ["2024-04-24-15-15-00", "2024-04-24-17-45-00",   "1"]
              , ["2024-04-24-18-15-00", "2024-04-25-12-00-00", "0.9"]
              , ["2024-04-25-13-00-00", "2024-04-25-16-00-00", "0.8"]
              , ["2024-04-25-16-15-00", "2024-05-07-14-00-00", "0.7"]
            ]

    return flist



def _GetDataFilesExtRangeForRead() :

    flist = [   ["2024-02-16-00-00-00", "2024-02-22-19-00-00", "100"] 
              , ["2024-02-22-21-30-00", "2024-02-23-13-30-00",  "90"]
              , ["2024-02-23-14-00-00", "2024-02-24-04-40-00",  "80"]
              , ["2024-02-24-05-00-00", "2024-02-25-23-00-00",  "70"]
              , ["2024-02-26-14-40-00", "2024-02-26-22-00-00",  "60"]
              , ["2024-02-26-08-00-00", "2024-02-26-14-00-00",  "50"]
              , ["2024-02-26-03-00-00", "2024-02-26-07-45-00",  "40"]
              , ["2024-02-25-11-05-00", "2024-02-26-02-30-00",  "30"]
              , ["2024-02-26-22-40-00", "2024-02-27-01-00-00",  "20"]
              , ["2024-02-27-01-30-00", "2024-03-14-18-00-00",  "10"]
              , ["2024-04-12-15-30-00", "2024-04-12-23-00-00",   "9"]
              , ["2024-04-15-07-00-00", "2024-04-19-00-00-00",   "8"]
              , ["2024-04-22-16-00-00", "2024-04-22-17-30-00",   "7"]
              , ["2024-04-22-18-00-00", "2024-04-22-19-15-00",   "6"]
              , ["2024-04-22-20-00-00", "2024-04-22-21-30-00",   "5"]
              , ["2024-04-22-22-15-00", "2024-04-22-23-30-00",   "4"]
              , ["2024-04-23-00-30-00", "2024-04-23-01-30-00",   "3"]
              , ["2024-04-23-02-00-00", "2024-04-24-15-00-00",   "2"]
              , ["2024-04-24-15-15-00", "2024-04-24-17-45-00",   "1"]
              , ["2024-04-24-18-15-00", "2024-04-25-12-00-00", "0.9"]
              , ["2024-04-25-13-00-00", "2024-04-25-16-00-00", "0.8"]
              , ["2024-04-25-16-15-00", "2024-05-07-14-00-00", "0.7"]
            ]

    return flist




"""
get the prometheus data
"""
def _GetPrometheusData( vname ) :
    print( "\tEnter GetPrometheusData[ %s ]" % vname )

    data       = {}
    session_id = 0     
    count      = 0
    topdir     = "/home/eos/prometheus/analysis/data"

    if not os.path.isdir(topdir) :
       sys.exit( "The directory [%s] does not exist. Cannot continue." % topdir )

    print( "\t\tparsing nfiles [%d]" % len(os.listdir(topdir)) )


    dataByDates = _GetDataFilesExtRangeForWrite()

    for dataInRange in dataByDates :
        ibegin = time.strptime(dataInRange[0], "%Y-%m-%d-%H-%M-%S")
        iend   = time.strptime(dataInRange[1], "%Y-%m-%d-%H-%M-%S")
     
        print( "\t\t\tbegin and end times [%s/%s]" % (dataInRange[0],dataInRange[1]) )
 
        values = {}
        size   = dataInRange[2]
        

        for v, vfile in enumerate(os.listdir(topdir)) :
            if vfile.find(vname) == -1 : continue

            """
            if v%100 == 0 :
               print( "\tat file [%d/%d : %s]" % (v,len(os.listdir(topdir)),vfile) )
            """

            fdate = vfile.split(".")[-1]
            ftimestamp = time.strptime(fdate, "%Y-%m-%d-%H-%M-%S")

            if ftimestamp < ibegin : continue
            if ftimestamp > iend   : continue

            fpath  = "%s/%s" % (topdir,vfile)
            vlines = open(fpath,'r').readlines()

            if len(vlines) > 1 : 
               print( "\t\t\t  at file [%d/%d : %s with lines(%d)]" % (v,len(os.listdir(topdir)),vfile,len(vlines)) )
            else : continue

            count += 1

            for vline in vlines: 
                if "session_id" in vline:
                   session_id_re = re.compile(r".*session_id=\"(\d+)\"")
                   session_id    = int(re.match(session_id_re, vline).groups(0)[0])
                   if session_id not in data :
                      values[session_id] = {}
                elif " @[" in vline :
                   var, timestamp = vline[:-2].split(" @[")
                   values[session_id][int(timestamp)] = int(var)

        print( "\t\t\t number of passing files [%d]" % count )
        print( "\t\t\t number of session is [%d]" % len(values) )
        data[size] = values

    print( "\n\t\tnumber of passing files [%d]" % count )
    print( "\tExit GetPrometheusData[ %s ]\n" % vname )

    return data




"""
match the transfer bytes and elasped time per session id
"""
def _MatchTransferredByteAndElaspedTimeData( transferedBytesData, elapsedTimeData ) :
    print( "\tEnter MatchTransferredByteAndElaspedTimeData" )

    if len(transferedBytesData) != len(elapsedTimeData) :
        sys.exit( "ERROR - cannot match the bytes transferred to elapsed time, sizes are not equal [%d,%d]" % (len(transferedBytesData),len(elapsedTimeData)) )

    data = {}

    for key, values in transferedBytesData.items() :
        i = -1
        print("\t\tkey is %s MB" % key)
  
        dataValues = {}
        elapsedTimeDataValues = elapsedTimeData[key]

        if len(values) != len(elapsedTimeDataValues) :
           sys.exit( "ERROR - cannot match the bytes transferred [%s] to elapsed time, sizes are not equal [%d,%d]" % (key,len(values),len(elapsedTimeDataValues)) ) 
        else :
           print( "\t\t  sizes of the data [%d,%d]" % (len(values),len(elapsedTimeDataValues)) ) 

        for sessionid, bytesData in values.items() :
            i += 1
            j  = 0
            
            #print("\t\t\tentry [%d] -- session id [%s] : byte data number of entries [%d]" % (i,sessionid,len(bytesData)))
            tmp = []

            for timestamp, bytes in bytesData.items() :
                j += 1
                stime = time.strftime('%m/%d/%Y::%H:%M:%S', time.gmtime(int(timestamp)))


                if int(timestamp) in elapsedTimeDataValues.get(sessionid,{}) :
                   timeData = elapsedTimeDataValues[sessionid][int(timestamp)] #the elapsed time
                   tmp.append( [timeData,bytes] )
                else :
                    print( "\t\tMatching transferred bytes and elasped time data failed for session id and timestamp [%s, %s]!" % (sessionid,timestamp) )


                """
                try :
                   timeData = elapsedTimeDataValues[sessionid][int(timestamp)] #the elapsed time
                   tmp.append( [timeData,bytes] )
                   #data[sessionid][j] = (timeData,bytes) #[timeData] = bytes
                except :
                   print( "\t\tMatching transferred bytes and elasped time data failed for session id and timestamp [%s, %s]!" % (sessionid,timestamp) )
                   #print( "Elasped Data")
                   #print( elapsedTimeDataValues[sessionid] )
                   #quit()
                """

            if len(tmp) > 0 :
               dataValues[sessionid] = tmp

        print( "\t\t\tnumber of session ids [%d]" % len(dataValues) )
        data[key] = dataValues


    print( "\t\tnumber of bins [%d]" % len(data) )
    print( "\tExit MatchTransferredByteAndElaspedTimeData\n" )
    return data



"""
calculate the transfer bytes per elasped time for each session id
"""
def _CalculateThroughput( ntuple ) :
    print( "\tEnter CalculateThroughput") 

    data = {}
    for key, values in ntuple.items() :
    
        print( "\t\tat key [%s MB]" % key )
        s = 0
        dataValues = {}

        for sessionid, metrics in values.items() :
            """
            print("\t\t\tentry [%d] -- session id [%s], metrics [%d]" % (s,sessionid,len(metrics)))
            """
            info = []

            for metric in metrics :
                time  = metric[0]
                bytes = metric[1]

                if time == 0 :
                   b_t = 9999999999999
                else :
                   b_t = float(bytes) / float(time)

                info.append( [bytes, b_t] )

            dataValues[sessionid] = info
            s += 1

        print( "\t\t number of session ids [%d]" % len(dataValues) )
        data[key] = dataValues 

    print( "\tExit CalculateThroughput\n")
    return data 



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




"""
plot the data by file size
"""
def _PlotDataByFileSize( dataPerFileSize ) :
    print( "\tEnter PlotDataByFileSize" )


    xdata = []
    ydata = []

    for key, value in dataPerFileSize.items() :
        xdata.append( float(key) )
        ydata.append( value )

    plt.xlabel( 'File Size (MB)' )
    plt.ylabel( 'Average Write Throughput (MB/s)', fontsize=12 )

    #plt.gca().ticklabel_format(axis='y', style='sci', scilimits=(0, 0))

    plt.plot(xdata, ydata, 'ro-')


    plt.savefig('WriteThroughput.png')

    print( "\tExit PlotDataByFileSize\n" )






##########################################################################
#  main block 
##########################################################################

if __name__ == '__main__' :

   print( "Enter Analysis of the Prometheus Data\n" )


   vars = [ "transfered_bytes", "elapsed_time" ]
   data = []

   for var in vars :
       data.append( _GetPrometheusData( var ) )

   matchedData      = _MatchTransferredByteAndElaspedTimeData( data[0], data[1])
   dataPerSessionID = _CalculateThroughput( matchedData )
   dataPerFileSize  = _OrganizeDataByFileSize( dataPerSessionID )

   _PlotDataByFileSize( dataPerFileSize )

   print( "Exit Analysis of the Prometheus Data\n" )
   
