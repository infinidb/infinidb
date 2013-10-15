#!/bin/bash
#
   numRoots=4
#
   for ((idx=1; $idx<=$numRoots; idx++)); do
      rm -rf /usr/local/Calpont/data$idx/dwbackup/2 & 
      pids[$idx]=$!
   done
#
   keepChecking=1
   while [ $keepChecking -eq 1 ]; do
      keepChecking=0
      for ((idx=1; $idx<=$numRoots; idx++)); do
         if [ ${pids[idx]} -ne 0 ]
         then
            lines=`ps -p ${pids[idx]} |wc -l`
            if [ $lines -eq 1 ]
            then
               pids[$idx]=0
            else
               keepChecking=1
            fi
         fi
      done
      sleep 5
   done
#
   for ((idx=1; $idx<=$numRoots; idx++)); do
      mv /usr/local/Calpont/data$idx/dwbackup/1 /usr/local/Calpont/data$idx/dwbackup/2
      mkdir -p /usr/local/Calpont/data$idx/dwbackup/1
      cp -r /usr/local/Calpont/data$idx/000.dir /usr/local/Calpont/data$idx/dwbackup/1 &
      pids[$idx]=$!
   done
#
   keepChecking=1
   while [ $keepChecking -eq 1 ]; do
      keepChecking=0
      for ((idx=1; $idx<=$numRoots; idx++)); do
         if [ ${pids[idx]} -ne 0 ]
         then
            lines=`ps -p ${pids[idx]} |wc -l`
            if [ $lines -eq 1 ]
            then
               pids[$idx]=0
            else
               keepChecking=1
            fi
         fi
      done
      sleep 5
   done
   cp -r /mnt/OAM/dbrm /usr/local/Calpont/data1/dwbackup/1/.
