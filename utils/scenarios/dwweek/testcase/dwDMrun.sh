#!/bin/bash
#
# test #1
#  run user queries from 7:00am to 6:00pm
#
   /root/genii/utils/scenarios/dwweek/test/dwControlGroup.sh dwweek 7 18 200 3 0 &
   sleep 5
   /root/genii/utils/scenarios/dwweek/test/dwControlGroup.sh dwweek 7 18 201 3 15 &
   sleep 5
   /root/genii/utils/scenarios/dwweek/test/dwControlGroup.sh dwweek 7 18 202 4 30 &
#
# run user query group #3 from 6:00pm to midnight
# Each run should take over one hour so we will stop initiating jobs after 10:00pm
# Effectively, jobs will finished sometime after 11:00pm
#
   /root/genii/utils/scenarios/dwweek/test/dwControlGroup.sh dwweek 18 22 3 2 0 &
#
#  Nightly delete, update, and backup -midnight to 7:00am
#
   /root/genii/utils/scenarios/dwweek/test/dwControlNightly.sh dwweek 0 &
