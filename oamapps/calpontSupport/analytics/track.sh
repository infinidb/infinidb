#!/bin/bash
#
# Usage:
# track.sh 

# top output
#top - 12:45:49 up 21:58,  2 users,  load average: 0.04, 0.10, 0.08
#Tasks: 164 total,   1 running, 163 sleeping,   0 stopped,   0 zombie
#Cpu(s):  2.9%us,  2.4%sy, 10.3%ni, 76.2%id,  8.0%wa,  0.0%hi,  0.2%si,  0.0%st
#Mem:  16440336k total,  6129588k used, 10310748k free,    74336k buffers
#Swap:  2031608k total,    14848k used,  2016760k free,   511012k cached
#
#  PID USER      PR  NI  VIRT  RES  SHR S %CPU %MEM    TIME+  COMMAND

# track.sh output
# SEQ DATE PID USER PR NI SWAP CACHE VIRT RES SHR S %CPU %MEM TIME+ COMMAND
# SWAP and CACHE are systemwise swap space: Swap: ... used ... cached

i=0
while [ true ]
do
  let i++
  dt=`date '+%Y-%m-%d %H:%M:%S'`
  top -b -n 1 |
  egrep "Swap:|PrimProc|ExeMgr|DMLProc|DDLProc|cpimport|WriteEngineServ|controllernode|workernode|mysqld|DecomSvr" |
  awk '{if(NR==1){s=$4; c=$8; next}}; {print s" "c" "$0}' |
  awk -v i=$i -v dt="$dt" '{
    sub(/k/,"",$1); sub(/k/,"",$2); sub(/m/,"000",$7); sub(/m/,"000",$8); sub(/m/,"000",$9);
    sub(/\.([0-9]+)g/, "&00000",$7); sub(/^[0-9]+g/, "&000000",$7); gsub(/[.|g]/, "",$7);
    sub(/\.([0-9]+)g/, "&00000",$8); sub(/^[0-9]+g/, "&000000",$8); gsub(/[.|g]/, "",$8)}
    {print i"|"dt"|"$3"|"$4"|"$5"|"$6"|"$1"|"$2"|"$7"|"$8"|"$9"|"$10"|"$11"|"$12"|"$13"|"$14"|"}'
  if [ -f stop.txt ]; then
    rm -f stop.txt
    exit
  fi
  sleep 1
done
