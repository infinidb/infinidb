#******************************************************************************************
# $Id$
#
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
#*****************************************************************************************/
include ../../rules.mak
.PHONY: install clean test docs coverage leakcheck

PPROGRAM=postConfigure
IPROGRAM=installer
APROGRAM=patchInstaller
ZPROGRAM=amazonInstaller

VERSION=1.0.0

CPPFLAGS=-I$(EXPORT_ROOT)/include -I/usr/include/libxml2
CXXFLAGS+=$(DEBUG_FLAGS) -Wall

CLIBS=-L$(EXPORT_ROOT)/lib -L/usr/local/Calpont/lib $(IDB_COMMON_LIBS) $(IDB_SNMP_LIBS)
LDFLAGS+=$(CLIBS) -Wl,--rpath -Wl,/usr/local/Calpont/lib
TLIBS=-lcppunit -ldl
GLIBS=$(CLIBS) -lcppunit -ldl

SRCS=helpers.cpp postConfigure.cpp installer.cpp patchInstaller.cpp queryStats.cpp amazonInstaller.cpp

OBJS=$(SRCS:.cpp=.o)

all: $(PPROGRAM) $(IPROGRAM) $(APROGRAM) $(ZPROGRAM) 

$(PPROGRAM): postConfigure.o helpers.o
	$(LINK.cpp) -o $@ $^ -Wl,-Bstatic -lreadline -Wl,-Bdynamic -lncurses

$(IPROGRAM): installer.o helpers.o
	$(LINK.cpp) -o $@ $^ -Wl,-Bstatic -lreadline -Wl,-Bdynamic -lncurses

$(APROGRAM): patchInstaller.o
	$(LINK.cpp) -o $@ $^

$(ZPROGRAM): amazonInstaller.o helpers.o
	$(LINK.cpp) -o $@ $^ -Wl,-Bstatic -lreadline -Wl,-Bdynamic -lncurses


install: bootstrap $(PPROGRAM) $(IPROGRAM) $(APROGRAM) $(ZPROGRAM) 
	mkdir -p $(INSTALL_ROOT_BIN)
	$(INSTALL) $(PPROGRAM) $(IPROGRAM) $(APROGRAM) $(ZPROGRAM)  $(INSTALL_ROOT_BIN)

bootstrap:

clean:
	rm -f $(OBJS) tdriver.o $(PPROGRAM) $(IPROGRAM) $(APROGRAM) $(ZPROGRAM)  $(LIBRARY) tdriver core *~ *.tag *-gcov.* *.gcov tdriver-gcov *.d
	rm -rf html

docs:
	doxygen $(EXPORT_ROOT)/etc/Doxyfile

tdriver: tdriver.o
	$(LINK.cpp) -o $@ $^ $(TLIBS)

test:

%-gcov.o: %.cpp
	$(COMPILE.cpp) -o $@ $^

tdriver-gcov: CXXFLAGS+=-fprofile-arcs -ftest-coverage
tdriver-gcov: postConfigure-gcov.o $(subst .o,-gcov.o,postConfigure.o)
	$(LINK.cpp) -o $@ $^ $(GLIBS)

coverage:

leakcheck:

%.d: %.cpp
	@set -e; rm -f $@; \
	$(CC) -MM $(CPPFLAGS) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

ifndef BOOTSTRAP
-include $(SRCS:.cpp=.d) tdriver.d
endif
