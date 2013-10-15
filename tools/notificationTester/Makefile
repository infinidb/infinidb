#******************************************************************************************
# $Id: Makefile 33 2006-09-29 13:45:51Z dhill $
#
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
# Author: David Hill
#*****************************************************************************************/
include ../../rules.mak
.PHONY: install clean test docs coverage leakcheck

PROGRAM=notificationTester
VERSION=1.0.0

CPPFLAGS=-I$(EXPORT_ROOT)/include -I/usr/include/libxml2
CXXFLAGS+=$(DEBUG_FLAGS) -Wall

# we need to add some search paths here because on a boostrap build we won't find liboamcpp
#  anywhere except in ../oamcpp
CLIBS=-L$(EXPORT_ROOT)/lib $(IDB_COMMON_LIBS) $(IDB_SNMP_LIBS)
LDFLAGS+=$(CLIBS) -Wl,--rpath -Wl,/usr/local/Calpont/lib
TLIBS=-lcppunit -ldl
GLIBS=$(CLIBS) -lcppunit -ldl

SRCS=main.cpp
LINCLUDES=

OBJS=$(SRCS:.cpp=.o)

$(PROGRAM): $(OBJS)
	$(LINK.cpp) -o $@ $^

client: client.o
	$(LINK.cpp) -o $@ $^

install: bootstrap $(PROGRAM)
	mkdir -p $(INSTALL_ROOT_BIN)
	$(INSTALL) $(PROGRAM) $(INSTALL_ROOT_BIN)

bootstrap:

clean:
	rm -f $(OBJS) tdriver.o $(PROGRAM) $(LIBRARY) tdriver core *~ *.tag *-gcov.* *.gcov tdriver-gcov *.d *.d.*
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
