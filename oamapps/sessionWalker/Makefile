#******************************************************************************************
# $Id: Makefile 2339 2012-05-02 18:20:56Z pleblanc $
#
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
# 
#*****************************************************************************************/
include ../../rules.mak
.PHONY: install clean docs test coverage leakcheck

PROGRAM=sessionWalker
VERSION=1.0.0

CPPFLAGS=-I$(EXPORT_ROOT)/include -I/usr/include/libxml2
CXXFLAGS+=$(DEBUG_FLAGS) -Wall

# we need to add some search paths here because on a boostrap build we won't find liboamcpp
#  anywhere except in ../oamcpp
CLIBS=-L$(EXPORT_ROOT)/lib -L/usr/local/Calpont/lib $(IDB_COMMON_LIBS) $(IDB_WRITE_LIBS)
TLIBS=-lcppunit -ldl
GLIBS=$(CLIBS) -lcppunit -ldl
OAMLIBS=-loamcpp -lsnmpmanager -lnetsnmp \
-lnetsnmpagent -lnetsnmphelpers -lnetsnmpmibs 
LDFLAGS+=$(CLIBS) $(OAMLIBS) -Wl,--rpath -Wl,/usr/local/Calpont/lib

SRCS=sessionwalker.cpp
LINCLUDES=

OBJS=$(SRCS:.cpp=.o)

$(PROGRAM): $(OBJS)
	$(LINK.cpp) $(LIBS) -o $@ $^

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

coverage: tdriver-gcov

leakcheck: tdriver

%.d: %.cpp
	@set -e; rm -f $@; \
	$(CC) -MM $(CPPFLAGS) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

ifndef BOOTSTRAP
-include $(SRCS:.cpp=.d) tdriver.d
endif
