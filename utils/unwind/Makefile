#******************************************************************************************
# $Id: Makefile 3048 2012-04-04 15:33:45Z rdempsey $
#
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
#*****************************************************************************************/
include ../../rules.mak
.PHONY: install clean test docs coverage leakcheck

LIBRARY=libunwind.so
VERSION=8.0.1
FULL_NAME=$(LIBRARY).$(VERSION)

LIBRARY1=libunwind-x86_64.so
VERSION1=8.0.1
FULL_NAME1=$(LIBRARY1).$(VERSION1)

LIBRARY2=libiberty.a

LINCLUDES=\
	demangle.h \
	libunwind.h \
	libunwind-ptrace.h \
	libunwind-common.h \
	libunwind-dynamic.h \
	libunwind-x86_64.h \
	unwind.h \
	libiberty.h \
	ansidecl.h

all:
	$(INSTALL) $(LIBRARY2) $(INSTALL_ROOT_LIB)
	$(INSTALL) $(FULL_NAME) $(INSTALL_ROOT_LIB)
	$(INSTALL) $(FULL_NAME1) $(INSTALL_ROOT_LIB)
	cd $(INSTALL_ROOT_LIB) &&  \
	ln -sf $(FULL_NAME) $(LIBRARY).8 && \
	ln -sf $(FULL_NAME1) $(LIBRARY1).8 && \
	ln -sf $(FULL_NAME) $(LIBRARY) && \
	ln -sf $(FULL_NAME1) $(LIBRARY1)


install:
	$(INSTALL) $(LIBRARY2) $(INSTALL_ROOT_LIB)
	$(INSTALL) $(FULL_NAME) $(INSTALL_ROOT_LIB)
	$(INSTALL) $(FULL_NAME1) $(INSTALL_ROOT_LIB)
	cd $(INSTALL_ROOT_LIB) &&  \
	ln -sf $(FULL_NAME) $(LIBRARY).8 && \
	ln -sf $(FULL_NAME1) $(LIBRARY1).8 && \
	ln -sf $(FULL_NAME) $(LIBRARY) && \
        ln -sf $(FULL_NAME1) $(LIBRARY1)


bootstrap:
	$(INSTALL) $(LINCLUDES) $(INSTALL_ROOT_INCLUDE)

docs:

clean:

