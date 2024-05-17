#!/usr/bin/env python

#---------------------------
# author: Ren Baueer
#---------------------------

import sys, re, os, time, datetime


"""
get data files
"""
def _GetDataFilesExtRange() :

    flist = [ [2024-02-16-00-00-00, 2024-02-22-19-00-00] ] #100 MB

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


    for v, vfile in enumerate(os.listdir(topdir)) :
        if vfile.find(vname) == -1 : continue

        """
        if v%100 == 0 :
           print( "\tat file [%d/%d : %s]" % (v,len(os.listdir(topdir)),vfile) )
        """

        print(vfile)
        fdate = vfile.split(".")[-1]
        ftimestamp = time.strptime(fdate, "%Y-%m-%d-%H-%M-%S")
        print(fdate)
        print(ftimestamp)
        sys.exit()


        fpath  = "%s/%s" % (topdir,vfile)
        vlines = open(fpath,'r').readlines()

        if len(vlines) > 1 : 
           print( "\tat file [%d/%d : %s with lines(%d)]" % (v,len(os.listdir(topdir)),vfile,len(vlines)) )
        else : continue

        count += 1

        for vline in vlines: 
            if "session_id" in vline:
                session_id_re = re.compile(r".*session_id=\"(\d+)\"")
                session_id    = int(re.match(session_id_re, vline).groups(0)[0])
                if session_id not in data :
                    data[session_id] = {}
            elif " @[" in vline :
                var, timestamp = vline[:-2].split(" @[")
                data[session_id][int(timestamp)] = int(var)

    print( "\t\tnumber of passing files [%d]" % count )
    print( "\t\tnumber of session ids [%d]" % len(data) )
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
    i    = -1

    for sessionid, bytesData in transferedBytesData.items() :
        i +=  1
        j  =  0

        print("\tentry [%d] -- session id [%s] : byte data number of entries [%d]" % (i,sessionid,len(bytesData)))

        tmp = []

        for timestamp, bytes in bytesData.items() :
            j += 1

            stime = time.strftime('%m/%d/%Y::%H:%M:%S', time.gmtime(int(timestamp)))

            """
            if j%100 == 0 :
               print("\t\t\tat line [%d] -- timestamp [%s : %s]" % (j,timestamp,stime))
            """            

            try :
               timeData = elapsedTimeData[sessionid][int(timestamp)] #the elapsed time
               tmp.append( [timeData,bytes] )
               #data[sessionid][j] = (timeData,bytes) #[timeData] = bytes
            except :
               print( "Matching transferred bytes and elasped time data failed for session id and timestamp [%s, %s]!" % (sessionid,timestamp) )
               print( "Elasped Data")
               print elapsedTimeData[sessionid]
               quit()

        data[sessionid] = tmp

    print( "\t\tnumber of session ids [%d]" % len(data) )
    print( "\tExit MatchTransferredByteAndElaspedTimeData\n" )
    return data



"""
calculate the transfer bytes per elasped time for each session id
"""
def _CalculateThroughput( ntuple ) :
    print( "\tEnter CalculateThroughput") 

    data = {}
    s    = 0

    for sessionid, metrics in ntuple.items() :
        print("\tentry [%d] -- session id [%s], metrics [%d]" % (s,sessionid,len(metrics)))

        info = []

        for metric in metrics :
            time  = metric[0]
            bytes = metric[1]

            if time == 0 :
               b_t = 9999999999999
            else :
               b_t = float(bytes) / float(time)

            info.append( [bytes, b_t] )

        data[sessionid] = info
        s += 1
 
    print( "\t\tnumber of session ids [%d]" % len(data) )
    print( "\tExit CalculateThroughput\n")
    return data 



"""
organize the data by file size
"""
def _OrganizeDataByFileSize( dataPerSessionID ) :
    print( "\tEnter OrganizeDataByFileSize")

    files = []
    for sdata in dataPerSessionID.values() :
        for metrics in sdata :
            fsize = metrics[0]
            if not fsize in files : 
               files.append(fsize)
 
    print("\t\tnumber of different files' sizes found [%d]" % len(files))

    data = {}

    for f, fsize in enumerate(files) :
        tmp = []

        for sdata in dataPerSessionID.values() :
            for metric in sdata :
                if metric[0] != fsize : continue
                tmp.append( metric[1] )

        if f%10 == 0 :
           print( "\t\tat line [%d], file size [%d] bytes, number of measurements [%d]" % (f,fsize,len(tmp)) )

        data[fsize] = tmp

    print( "\t\tnumber of saved file sizes [%d]" % len(data) )
    print( "\tExit OrganizeDataByFileSize\n")
    return data




"""
compute the average transfered bytes per file size
"""
def _ComputeAvgBytesByFileSize( dataPerFileSize ) :
    print( "\tEnter ComputeAvgBytesByFileSize" )

    data = []


    print( "\tExit ComputeAvgBytesByFileSize\n" )
    return data






##########################################################################
#  main block 
##########################################################################

if __name__ == '__main__' :

   print( "Enter Analysis of the Prometheus Data\n" )


   vars = [ "transfered_bytes", "elapsed_time" ]
   data = []

   for var in vars :
       data.append( _GetPrometheusData( var ) )

   
   """
   matchedData      = _MatchTransferredByteAndElaspedTimeData( data[0], data[1])
   dataPerSessionID = _CalculateThroughput( matchedData )
   dataPerFileSize  = _OrganizeDataByFileSize( dataPerSessionID )
   avgBytePerFsize  = _ComputeAvgBytesByFileSize( dataPerFileSize )
   """

   print( "Exit Analysis of the Prometheus Data\n" )
   
