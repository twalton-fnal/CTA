#!/bin/bash


if ! command -v promtool &> /dev/null
then
	echo 'promtool command not found! Ensure promtool is in $PATH'
	exit 1
fi


startDate=("2024-05-07-18-20-00" "2024-05-07-20-00-00" "2024-05-07-21-30-00" "2024-05-08-09-30-00" "2024-05-08-14-00-00" "2024-05-08-16-00-00" "2024-05-08-19-15-00" "2024-05-08-22-30-00" "2024-05-09-09-00-00" "2024-05-09-11-20-00" "2024-05-09-13-30-00" "2024-05-09-18-25-00" "2024-05-09-21-00-00" "2024-05-10-17-30-00" "2024-05-10-20-00-00" "2024-05-10-22-30-00" "2024-05-10-01-30-00" "2024-05-11-11-45-00" "2024-05-11-15-10-00" "2024-05-11-19-30-00" "2024-05-11-23-15-00" "2024-05-12-01-30-00" "2024-05-12-06-00-00" "2024-05-12-10-00-00" "2024-05-12-13-50-00" "2024-05-12-16-30-00" "2024-05-12-19-00-00")


endDate=("2024-05-07-19-20-00" "2024-05-07-21-15-00" "2024-05-08-09-00-00" "2024-05-08-13-45-00" "2024-05-08-15-30-00" "2024-05-08-18-00-00" "2024-05-08-22-15-00" "2024-05-08-23-30-00" "2024-05-09-10-50-00" "2024-05-09-12-55-00" "2024-05-09-18-00-00" "2024-05-09-20-30-00" "2024-05-10-17-15-00" "2024-05-10-19-30-00" "2024-05-10-22-00-00" "2024-05-11-00-25-00" "2024-05-11-04-15-00" "2024-05-11-14-30-00" "2024-05-11-17-15-00" "2024-05-11-22-00-00" "2024-05-12-00-30-00" "2024-05-12-03-00-00" "2024-05-12-07-30-00" "2024-05-12-12-15-00" "2024-05-12-15-30-00" "2024-05-12-18-00-00" "2024-05-14-14-00-00")


ndates=${#startDate[@]}

for (( i=0; i<${ndates}; i++ ));
do 

    pstart=${startDate[$i]}
    astart=(${pstart//-/ })
    start="${astart[0]}-${astart[1]}-${astart[2]}T${astart[3]}:${astart[4]}:${astart[5]} UTC"
    tstart=$(date -d "$start + 5 hours" +"%s")

    pend=${endDate[$i]}   
    aend=(${pend//-/ })
    end="${aend[0]}-${aend[1]}-${aend[2]}T${aend[3]}:${aend[4]}:${aend[5]} UTC"
    tend=$(date -d "$end + 5 hours" +"%s")

    echo -e "at element $i, date range is ( ${startDate[$i]} and ${endDate[$i]} ) and ( ${tstart} and ${tend})\n"

    begin=$tstart
    stop=$tend
    while [ $begin -lt $stop ];
    do 

          next=$((begin+11000))
          #ext=${startDate[$i]}

          a=$(date -d @$begin +'%Y-%m-%dT%H:%M:%S')
          ext=$(date -d "$a + 6  hours" +%Y-%m-%d-%H-%M-%S)  
         
          promtool query range --start $begin --end $next --step 1s http://storagedev201.fnal.gov:9090 "cta_bytes_transfered_in_session{current_tape_pool='twaltontest',drive_status='TRANSFERING'}" > /home/eos/prometheus/analysis/data/transfered_bytes.$ext
          promtool query range --start $begin --end $next --step 1s http://storagedev201.fnal.gov:9090 "cta_session_elapsed_time{current_tape_pool='twaltontest',drive_status='TRANSFERING'}" > /home/eos/prometheus/analysis/data/elapsed_time.$ext

          begin=$next

    done
done

