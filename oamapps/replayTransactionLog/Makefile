#******************************************************************************************
# $Id: Makefile 203 2007-04-04 20:30:53Z jrodriguez $
#
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
# 
#*****************************************************************************************/
include ../../rules.mak
.PHONY: install clean docs test coverage leakcheck

PROGRAM=ReplayTransactionLog
VERSION=1.0.0

CPPFLAGS=-I$(EXPORT_ROOT)/include -I/usr/include/libxml2 
CXXFLAGS+=$(DEBUG_FLAGS) -Wall -D_DEBUG

# we need to add some search paths here because on a bootstrap build we won't find liboamcpp
#  anywhere except in ../oamcpp
CLIBS=-L$(EXPORT_ROOT)/lib $(IDB_COMMON_LIBS) -lreplaytxnlog $(IDB_SNMP_LIBS)
LDFLAGS+=$(CLIBS) -Wl,--rpath -Wl,/usr/local/Calpont/lib
TLIBS=-lcppunit -ldl
GLIBS=$(CLIBS) -lcppunit -ldl

SRCS=replaytransactionlog.cpp
LINCLUDES=

OBJS=$(SRCS:.cpp=.o)

$(PROGRAM): $(OBJS)
	$(LINK.cpp) -o $@ $^

install: bootstrap $(PROGRAM)
	mkdir -p $(INSTALL_ROOT_BIN)
	$(INSTALL) $(PROGRAM) $(INSTALL_ROOT_BIN)

bootstrap:

clean:
	rm -f $(OBJS) tdriver.o $(PROGRAM) $(LIBRARY) tdriver core *~ *.tag *-gcov.* *.gcov tdriver-gcov *.d
	rm -rf html

docs:
	doxygen $(EXPORT_ROOT)/etc/Doxyfile

tdriver: tdriver.o
	$(LINK.cpp) -o $@ $^ $(TLIBS)

test: 

#test: $(LIBRARY) tdriver
#	-ipcs-pat -d >& /dev/null
#	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver
#	-ipcs-pat -d >& /dev/null

%-gcov.o: %.cpp
	$(COMPILE.cpp) -o $@ $^

tdriver-gcov: CXXFLAGS+=-fprofile-arcs -ftest-coverage
tdriver-gcov: sessionwalker-gcov.o $(subst .o,-gcov.o,$(OBJS))
	$(LINK.cpp) -o $@ $^ $(GLIBS)

leakcheck: tdriver

%.d: %.cpp
	@set -e; rm -f $@; \
	$(CC) -MM $(CPPFLAGS) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

ifndef BOOTSTRAP
-include $(SRCS:.cpp=.d) tdriver.d
endif
