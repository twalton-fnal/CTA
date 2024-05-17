#!/bin/bash

#---------------------------
# author: Ren Baueer
#---------------------------

if ! command -v promtool &> /dev/null
then
	echo 'promtool command not found! Ensure promtool is in $PATH'
	exit 1
fi

start=1692172800; 
echo "the start time is [$start]"
echo "the current date is [$(date +%s)]"


while [ $start -lt $(date +%s) ]; 
do
   #echo -e "$start  $(date -u -r 1650113014 "+%Y/%m/%d %H:%M:%S")"
   end=$(($start + 11000))

   ext=$(date +'%Y-%m-%d-%H-%M-%S' -d "@$start")
   promtool query range --start $start --end $end --step 1s http://storagedev201.fnal.gov:9090 "cta_bytes_transfered_in_session{current_tape_pool='twaltontest',drive_status='TRANSFERING'}" > /home/eos/prometheus/analysis/data/transfered_bytes.$ext
   promtool query range --start $start --end $end --step 1s http://storagedev201.fnal.gov:9090 "cta_session_elapsed_time{current_tape_pool='twaltontest',drive_status='TRANSFERING'}" > /home/eos/prometheus/analysis/data/elapsed_time.$ext
   start=$end
done
