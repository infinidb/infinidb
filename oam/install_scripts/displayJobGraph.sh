#!/bin/bash
#
# $Id$

PATH=/usr/local/Calpont/bin:$PATH

graphdir=$(getConfig SystemConfig WorkingDir)
if [ -z "$graphdir" ]; then
	echo "Note: looking for graph files in default directory /tmp"
	graphdir=/tmp
fi
if [ ! -d "$graphdir" ]; then
	echo "Note: looking for graph files in default directory /tmp"
	graphdir=/tmp
fi
cd "$graphdir"
if [ -z "$1" ]; then
	filename=$(ls -1tr jobstep*.dot 2>&- | tail -1)
else
	filename="$1"
fi
if [ -z "$filename" ]; then
	echo "No graph file specified and none found in $graphdir" 1>&2
	exit 1
fi

dot -V >/dev/null 2>&1
if [ $? -ne -0 ]; then
	echo "'dot' command not found" 1>&2
	echo "This script requires the 'dot' command to convert jobfiles into PNG files" 1>&2
	echo "The 'graphviz' package is one possible source for the 'dot' command" 1>&2
	exit 1
fi
outname=$(echo $filename | sed 's/\.dot$/.png/')
cnt=$(expr $outname : '.*\.png$')
if [ $cnt -eq 0 ]; then
	outname=${outname}.png
fi
dot -Tpng -o$outname $filename

eog --version >/dev/null 2>&1
if [ $? -ne 0 ]; then
	echo "Output PNG left in $graphdir/$outname"
	exit 0
fi
eog $outname

# vim:ts=4 sw=4:
