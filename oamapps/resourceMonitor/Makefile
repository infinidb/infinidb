#******************************************************************************************
# $Id: Makefile 2339 2012-05-02 18:20:56Z pleblanc $
#
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
# Author: Zhixuan Zhu
#*****************************************************************************************/
include ../../rules.mak
.PHONY: install clean test docs coverage leakcheck

PROGRAM=resourceMonitor
VERSION=1.0.0

CPPFLAGS=-I$(EXPORT_ROOT)/include -I/usr/include/libxml2
CXXFLAGS+=$(DEBUG_FLAGS) -Wall

# we need to add some search paths here because on a boostrap build we won't find liboamcpp
#  anywhere except in ../oamcpp
CLIBS=-L../oamcpp -L$(EXPORT_ROOT)/lib -L/usr/local/Calpont/lib -loamcpp -lmessageqcpp -lconfigcpp \
-lxml2 -lsnmpmanager -lnetsnmpmibs -lnetsnmpagent -lnetsnmp -lnetsnmpmibs -lnetsnmphelpers \
-lboost_filesystem -lloggingcpp
LDFLAGS+=$(CLIBS) -Wl,--rpath -Wl,/usr/local/Calpont/lib
TLIBS=-lcppunit -ldl
GLIBS=$(CLIBS) -lcppunit -ldl
LD_LIBRARY_PATH=

SRCS=resourceMonitor.cpp
LINCLUDES=

OBJS=$(SRCS:.cpp=.o)

$(PROGRAM): $(OBJS)
	$(LINK.cpp) -o $@ $^

install:
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

test: $(LIBRARY) tdriver
#	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib ./resourceMonitor

%-gcov.o: %.cpp
	$(COMPILE.cpp) -o $@ $^

tdriver-gcov: CXXFLAGS+=-fprofile-arcs -ftest-coverage
tdriver-gcov: resourceMonitor-gcov.o $(subst .o,-gcov.o,$(OBJS))
	$(LINK.cpp) -o $@ $^ $(GLIBS)

coverage: tdriver-gcov
#	rm -f *.gcda
#	rm -f /var/log/Calpont/cpu*.log
#	LD_LIBRARY_PATH=$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver-gcov
#	for file in $(SRCS); do \
#		bfile=`basename $$file .cpp`; \
#		gcov -o $${bfile}-gcov $$file >/dev/null; \
#	done
#	/usr/local/bin/genCoverage.pl $(SRCS)

leakcheck: $(LIBRARY) tdriver
#	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib valgrind --tool=memcheck --leak-check=yes ./resourceMonitor

%.d: %.cpp
	@set -e; rm -f $@; \
	$(CC) -MM $(CPPFLAGS) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

ifndef BOOTSTRAP
-include $(SRCS:.cpp=.d) tdriver.d
endif
