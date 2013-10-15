include ../rules.mak
#******************************************************************************************
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
#*****************************************************************************************/
TARGETS = bootstrap clean test docs coverage leakcheck install

.PHONY: all $(TARGETS)

all:
	$(MAKE) -C etc
	$(MAKE) -C snmpmanager

$(TARGETS):
	$(MAKE) -C etc $@
	$(MAKE) -C snmpmanager $@

