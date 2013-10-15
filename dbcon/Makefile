include ../rules.mak
#******************************************************************************************
# $Id: Makefile 9255 2013-02-05 13:20:13Z rdempsey $
#
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
#*****************************************************************************************/
TARGETS = bootstrap clean test docs coverage leakcheck install

.PHONY: all $(TARGETS)

all:
	$(MAKE) -C execplan
	$(MAKE) -C ddlpackage
	$(MAKE) -C ddlpackageproc
	$(MAKE) -C dmlpackage
	$(MAKE) -C dmlpackageproc
	$(MAKE) -C joblist
	$(MAKE) -C mysql

$(TARGETS):
	$(MAKE) -C execplan $@
	$(MAKE) -C ddlpackage $@
	$(MAKE) -C ddlpackageproc $@
	$(MAKE) -C dmlpackage $@
	$(MAKE) -C dmlpackageproc $@
	$(MAKE) -C joblist $@
	$(MAKE) -C mysql $@

