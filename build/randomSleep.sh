#!/bin/bash

# This a tool for buildbot to keep the nightlies from swamping svn

sleep=$((($RANDOM % 16) * 45))

echo sleep $sleep
sleep $sleep

