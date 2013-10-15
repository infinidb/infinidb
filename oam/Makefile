include ../rules.mak
#******************************************************************************************
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
#*****************************************************************************************/
TARGETS = bootstrap clean install test docs coverage leakcheck 

.PHONY: all $(TARGETS)

all:
	$(MAKE) -C etc
	$(MAKE) -C post
	$(MAKE) -C oamcpp
	$(MAKE) -C install_scripts
	$(MAKE) -C cloud

$(TARGETS):
	$(MAKE) -C etc $@
	$(MAKE) -C post $@
	$(MAKE) -C oamcpp $@
	$(MAKE) -C install_scripts $@
	$(MAKE) -C cloud $@

