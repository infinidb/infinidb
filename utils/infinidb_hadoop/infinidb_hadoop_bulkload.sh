#!/bin/sh
export LD_LIBRARY_PATH=/usr/local/Calpont/lib:$LD_LIBRARY_PATH
export CALPONT_CONFIG_FILE=/usr/local/Calpont/etc/Calpont.xml
export PATH=$PATH:/usr/local/hadoop-0.20.2/bin:/usr/local/Calpont/bin
export CALPONT_HOME=/usr/local/Calpont/etc
hadoop dfs -cat $1 | cpimport $2 $3

