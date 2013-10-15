#!/bin/bash
#
# $Id: genVersion.sh 1019 2009-12-18 14:42:35Z rdempsey $
#

prefix=export
for arg in "$@"; do
	if [ `expr -- "$arg" : '--prefix='` -eq 9 ]; then
		prefix="`echo $arg | awk -F= '{print $2}'`"
	else
		echo "ignoring unknown argument: $arg" 1>&2
	fi
done

#try to find project root
while [ ! -d dbcon ]; do
	cd ..
	curdir=$(pwd)
	if [ $curdir = / -o $curdir = $HOME ]; then
		echo "I could not find the project root directory: I can't continue!"
		exit 1
	fi
done

if [ ! -f ./build/releasenum ]; then
	echo "I could not find the file 'releasesum' in the build directory: I can't continue!"
	exit 1
fi

. ./build/releasenum

mkdir -p ${prefix}/include

echo "
#ifndef VERSIONNUMBER_H_
#define VERSIONNUMBER_H_
#include <string>
const std::string idb_version(\"$version\");
const std::string idb_release(\"$release\");
#endif
" > ${prefix}/include/versionnumber.h.tmp

diff -bBq ${prefix}/include/versionnumber.h.tmp ${prefix}/include/versionnumber.h >/dev/null 2>&1
if [ $? -ne 0 ]; then
	cp ${prefix}/include/versionnumber.h.tmp ${prefix}/include/versionnumber.h
fi

rm -f ${prefix}/include/versionnumber.h.tmp

