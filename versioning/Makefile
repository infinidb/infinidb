include ../rules.mak
#******************************************************************************************
# $Id: Makefile 1547 2012-04-04 18:19:01Z rdempsey $
#
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
#*****************************************************************************************/
TARGETS = bootstrap clean test docs coverage leakcheck install tools dbrm \
test-dbrm install_tools install_all install_dbrm

.PHONY: $(TARGETS)

all:
	$(MAKE) -C BRM 

$(TARGETS):
	$(MAKE) -C BRM $@

