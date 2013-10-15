#******************************************************************************************
# $Id: Makefile 33 2006-09-29 13:45:51Z dhill $
#
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
# Author: Zhixuan Zhu
#*****************************************************************************************/
include ../../rules.mak
.PHONY: install clean test docs coverage leakcheck

PROGRAM=HardwareMonitor
VERSION=1.0.0

CPPFLAGS=-I$(EXPORT_ROOT)/include -I/usr/include/libxml2
CXXFLAGS+=$(DEBUG_FLAGS) -Wall

# we need to add some search paths here because on a boostrap build we won't find liboamcpp
#  anywhere except in ../oamcpp
CLIBS=-L$(EXPORT_ROOT)/lib -lloggingcpp -lsnmpmanager -lnetsnmpmibs -lnetsnmpagent -lnetsnmp \
-lnetsnmpmibs -lnetsnmphelpers -loamcpp -lmessageqcpp -lconfigcpp -lboost_filesystem -lssl -lxml2 -ldl
LDFLAGS+=$(CLIBS) -Wl,--rpath -Wl,/usr/local/Calpont/lib
TLIBS=-lcppunit -ldl
GLIBS=$(CLIBS) -lcppunit -ldl
LD_LIBRARY_PATH=

SRCS=hardwareMonitor.cpp
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


%.d: %.cpp
	@set -e; rm -f $@; \
	$(CC) -MM $(CPPFLAGS) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

ifndef BOOTSTRAP
-include $(SRCS:.cpp=.d) tdriver.d
endif
