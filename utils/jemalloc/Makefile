#******************************************************************************************
# $Id: Makefile 3048 2012-04-04 15:33:45Z rdempsey $
#
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
#*****************************************************************************************/
include ../../rules.mak
.PHONY: install clean test docs coverage leakcheck

LIBRARY=libjemalloc.so
VERSION=3.3.0
FULL_NAME=$(LIBRARY).$(VERSION)

all:

install:
	$(INSTALL) $(FULL_NAME) $(INSTALL_ROOT_LIB)
	cd $(INSTALL_ROOT_LIB) &&  \
	ln -sf $(FULL_NAME) $(LIBRARY) 

bootstrap:

docs:

clean:


