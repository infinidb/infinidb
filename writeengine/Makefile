include ../rules.mak
#*******************************************************************************
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
#******************************************************************************/
TARGETS = bootstrap test docs coverage install leakcheck

.PHONY: $(TARGETS) install_bulk install_server install_splitter install_all

all:
	$(MAKE) -C shared
	$(MAKE) -C dictionary
	$(MAKE) -C xml 
	$(MAKE) -C wrapper 
	$(MAKE) -C bulk 
	$(MAKE) -C client
	$(MAKE) -C splitter
	$(MAKE) -C server
	$(MAKE) -C redistribute

clean:
	$(MAKE) -C shared clean
	$(MAKE) -C dictionary clean
	$(MAKE) -C xml clean
	$(MAKE) -C wrapper clean
	$(MAKE) -C bulk clean
	$(MAKE) -C client clean
	$(MAKE) -C splitter clean
	$(MAKE) -C server clean
	$(MAKE) -C redistribute clean
	rm -rf obj

$(TARGETS):
	$(MAKE) -C shared $@
	$(MAKE) -C dictionary $@
	$(MAKE) -C xml $@
	$(MAKE) -C wrapper $@
	$(MAKE) -C bulk $@
	$(MAKE) -C client $@
	$(MAKE) -C splitter $@
	$(MAKE) -C server $@
	$(MAKE) -C redistribute $@

install_bulk:
	$(MAKE) -C bulk $@

install_server:
	$(MAKE) -C server $@

install_splitter:
	$(MAKE) -C splitter $@

install_all: bootstrap all install_bulk install_server install_splitter
