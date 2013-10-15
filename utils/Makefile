include ../rules.mak
#******************************************************************************************
# $Id: Makefile 4037 2013-08-07 03:37:31Z bwilkinson $
#
# Copyright (C) 2009-2013 Calpont Corporation
# All rights reserved
#*****************************************************************************************/

TARGETS = bootstrap clean test docs coverage leakcheck install

.PHONY: $(TARGETS)

PROGS = boost_idb startup common configcpp loggingcpp messageqcpp threadpool multicast rwlock dataconvert joiner rowgroup cacheutils funcexp udfsdk compress batchloader ddlcleanup mysqlcl_idb querystats jemalloc windowfunction idbdatafile idbhdfs

all $(TARGETS):
	if [ $@ != all ]; \
		then target=$@; \
	fi; \
	for prog in $(PROGS); \
		do $(MAKE) -C $$prog $$target || exit $$?; \
	done

