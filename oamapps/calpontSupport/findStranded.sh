#!/bin/sh

#
# This script lists InfiniDBlpont data files that do not have associated extent map entries.
#
# NOTES:  
# 1) Only looks in $INFINIDB_INSTALL_DIR/data* for the data files.
# 2) Only checks for an existing extent with a matching OID, doesn't validate that there is an
#    existing extent for the exact segment.
#
# Close enough for hand grenades.

if [ -z "$INFINIDB_INSTALL_DIR" ]; then
	INFINIDB_INSTALL_DIR=/usr/local/Calpont
fi

export INFINIDB_INSTALL_DIR=$INFINIDB_INSTALL_DIR

if [ $INFINIDB_INSTALL_DIR != "/usr/local/Calpont" ]; then
	export PATH=$INFINIDB_INSTALL_DIR/bin:$INFINIDB_INSTALL_DIR/mysql/bin:/bin:/usr/bin
	export LD_LIBRARY_PATH=$INFINIDB_INSTALL_DIR/lib:$INFINIDB_INSTALL_DIR/mysql/lib/mysql
fi

cd $INFINIDB_INSTALL_DIR

last=-1
existsInExtentMap=0
count=0

for i in $INFINIDB_INSTALL_DIR/data*/*/*/*/*/*/FILE*cdf; do
        let count++
        oid=`$INFINIDB_INSTALL_DIR/bin/file2oid.pl $i`
	if [ $last -ne $oid ]; then
		last=$oid
        	existsInExtentMap=`$INFINIDB_INSTALL_DIR/bin/editem -o $oid | wc -l`
	fi
        if [ $existsInExtentMap -le 0 ]; then
                echo "Missing oid $oid path $i"
        fi
done
