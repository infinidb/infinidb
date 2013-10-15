include ../rules.mak
#******************************************************************************************
# $Id: Makefile 3121 2013-06-26 15:31:13Z rdempsey $
#
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
#*****************************************************************************************/
TARGETS = bootstrap clean test docs coverage leakcheck install

.PHONY: all $(TARGETS)

all:
	$(MAKE) -C calpontConsole
	$(MAKE) -C traphandler
	$(MAKE) -C serverMonitor
	$(MAKE) -C postConfigure
	$(MAKE) -C calpontDB
	$(MAKE) -C sendtrap
$(TARGETS):
	$(MAKE) -C calpontConsole $@
	$(MAKE) -C traphandler $@
	$(MAKE) -C serverMonitor $@
	$(MAKE) -C postConfigure $@
	$(MAKE) -C calpontDB $@
	$(MAKE) -C sendtrap $@
