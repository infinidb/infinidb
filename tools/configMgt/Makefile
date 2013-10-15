#******************************************************************************************
# $Id$
#
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
#*****************************************************************************************/
include ../../rules.mak
.PHONY: install clean test docs coverage leakcheck

IPROGRAM=autoInstaller
APROGRAM=autoConfigure
QPROGRAM=svnQuery
SPROGRAM=stackReleaseChecker

VERSION=1.0.0

CPPFLAGS=-I$(EXPORT_ROOT)/include -I/usr/include/libxml2
CXXFLAGS+=$(DEBUG_FLAGS) -Wall

CLIBS=-L$(EXPORT_ROOT)/lib -L/usr/local/Calpont/lib $(IDB_COMMON_LIBS) $(IDB_SNMP_LIBS)
LDFLAGS+=$(CLIBS) -Wl,--rpath -Wl,/usr/local/Calpont/lib

SRCS=autoInstaller.cpp autoConfigure.cpp svnQuery.cpp
OBJS=$(SRCS:.cpp=.o)

all: $(IPROGRAM) $(APROGRAM) $(QPROGRAM) $(SPROGRAM)

$(IPROGRAM): autoInstaller.o
	$(LINK.cpp) -o $@ $^ -Wl,-Bstatic -lreadline -Wl,-Bdynamic -lncurses

$(APROGRAM): autoConfigure.o
	$(LINK.cpp) -o $@ $^

$(QPROGRAM): svnQuery.o
	$(LINK.cpp) -o $@ $^

$(SPROGRAM): stackReleaseChecker.o
	$(LINK.cpp) -o $@ $^

install: bootstrap $(APROGRAM)
	mkdir -p $(INSTALL_ROOT_BIN)
	$(INSTALL) $(APROGRAM) $(INSTALL_ROOT_BIN)

bootstrap:

clean:
	rm -f $(OBJS) tdriver.o $(IPROGRAM) $(APROGRAM) $(QPROGRAM) $(SPROGRAM) $(LIBRARY) tdriver core *~ *.tag *-gcov.* *.gcov tdriver-gcov *.d *.d.*
	rm -rf html

docs:

tdriver:

test:

coverage:

leakcheck:

ifndef BOOTSTRAP
-include $(SRCS:.cpp=.d) tdriver.d
endif
