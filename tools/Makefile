include ../rules.mak
#******************************************************************************************
# $Id: Makefile 2375 2013-08-07 03:34:29Z bwilkinson $
#
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
#*****************************************************************************************/
TARGETS = bootstrap clean test docs coverage leakcheck install

.PHONY: all $(TARGETS)

PROGS = dbbuilder editem dbloadxml \
	getConfig cplogger \
	clearShm setConfig \
	configMgt viewtablelock cleartablelock ddlcleanup \
	idbmeminfo \
	hdfsCheck

all $(TARGETS):
	if [ $@ != all ]; \
		then target=$@; \
	fi; \
	for prog in $(PROGS); \
		do $(MAKE) -C $$prog $$target || exit 1; \
	done

