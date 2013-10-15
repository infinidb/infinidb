#******************************************************************************************
# $Id: Makefile 678 2012-04-04 18:20:46Z rdempsey $
#
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
# Author: Zhixuan Zhu
#*****************************************************************************************/
include ../../rules.mak
.PHONY: install clean test docs coverage leakcheck

LIBS64=$(shell test `arch -k` == 'x86_64' && echo -L/usr/lib64)

PROGRAM=libsnmpmanager.so
VERSION=1.0.0
LIBRARY=$(PROGRAM).$(VERSION)

CPPFLAGS=-I$(EXPORT_ROOT)/include -I/usr/include/libxml2
CXXFLAGS+=$(DEBUG_FLAGS) -Wall -fpic

CLIBS=-L$(EXPORT_ROOT)/lib -lmessageqcpp -lconfigcpp $(LIBS64) -lxml2 -lnetsnmphelpers -lnetsnmpagent -lnetsnmp \
-lnetsnmpmibs -lrpm -loamcpp -lboost_filesystem -lloggingcpp 
XLIBS=$(CLIBS)
TLIBS=-L. -lsnmpmanager $(CLIBS) -lcppunit -ldl
GLIBS=$(CLIBS) -lcppunit -ldl

LLIBS=-L$(CALPONT_LIBRARY_PATH) -lloggingcpp -loamcpp -lnetsnmpagent -lnetsnmp -lnetsnmpmibs -lnetsnmphelpers

SRCS=snmpmanager.cpp alarm.cpp
LINCLUDES=snmpglobal.h snmpmanager.h alarm.h

OBJS=$(SRCS:.cpp=.o)

$(LIBRARY): $(OBJS)
	$(LINK.cpp) -shared -o $(LIBRARY) $(OBJS) -L$(EXPORT_ROOT)/lib $(IDB_SNMP_LIBS)
	rm -f $(PROGRAM)
	ln -s $(LIBRARY) $(PROGRAM)

install: bootstrap $(LIBRARY)
	umask 002; \
	mkdir -p $(INSTALL_ROOT_LIB) $(INSTALL_ROOT_INCLUDE); \
	pushd $(INSTALL_ROOT_LIB) >/dev/null; \
		rm -f $(LIBRARY) $(PROGRAM); \
		ln -s $(LIBRARY) $(PROGRAM); \
	popd >/dev/null; \
	$(INSTALL) $(LIBRARY) $(INSTALL_ROOT_LIB)

bootstrap:
	$(INSTALL) $(LINCLUDES) $(INSTALL_ROOT_INCLUDE)

clean:
	rm -f $(OBJS) tdriver.o $(PROGRAM) $(LIBRARY) tdriver core *~ *.tag *-gcov.* *.gcov tdriver-gcov *.d
	rm -rf html

docs:
	doxygen $(EXPORT_ROOT)/etc/Doxyfile

tdriver: tdriver.o
	$(LINK.cpp) -o $@ $^ $(TLIBS)

test: $(LIBRARY) tdriver
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver

%-gcov.o: %.cpp
	$(COMPILE.cpp) -o $@ $^

tdriver-gcov: CXXFLAGS+=-fprofile-arcs -ftest-coverage
tdriver-gcov: tdriver-gcov.o $(subst .o,-gcov.o,$(OBJS))
	$(LINK.cpp) -o $@ $^ $(GLIBS)

coverage: tdriver-gcov
	rm -f *.gcda
	LD_LIBRARY_PATH=$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver-gcov
	for file in $(SRCS); do \
		bfile=`basename $$file .cpp`; \
		gcov -o $${bfile}-gcov $$file >/dev/null; \
	done
	/usr/local/bin/genCoverage.pl $(SRCS)

leakcheck: $(LIBRARY) tdriver
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib valgrind --tool=memcheck --leak-check=yes ./tdriver

%.d: %.cpp
	@set -e; rm -f $@; \
	$(CC) -MM $(CPPFLAGS) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

ifndef BOOTSTRAP
-include $(SRCS:.cpp=.d) tdriver.d
endif
