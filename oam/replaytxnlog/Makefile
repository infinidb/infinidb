#******************************************************************************************
# $Id: Makefile 389 2007-03-22 22:56:03Z wweeks $
#
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
#*****************************************************************************************/
include ../../rules.mak
.PHONY: install clean test docs coverage leakcheck

PROGRAM=libreplaytxnlog.so
VERSION=1.0.0
LIBRARY=$(PROGRAM).$(VERSION)

CPPFLAGS=-I$(EXPORT_ROOT)/include -I/usr/include/libxml2
CXXFLAGS+=$(DEBUG_FLAGS) -Wall -fpic

CLIBS=-L../oamcpp -L$(EXPORT_ROOT)/lib -loamcpp -lmessageqcpp -lloggingcpp -loamcpp -lconfigcpp -lxml2 -lsnmpmanager -lnetsnmpmibs -lnetsnmpagent -L/usr/local/Calpont/lib -lnetsnmp -lnetsnmphelpers -lcppunit -ldl -lrwlock -lbrm
TLIBS=-L. -loamcpp $(CLIBS)
GLIBS=$(CLIBS)

LLIBS=-L$(CALPONT_LIBRARY_PATH) -loamcpp -lexecplan

SRCS=replaytxnlog.cpp 
LINCLUDES=replaytxnlog.h 

OBJS=$(SRCS:.cpp=.o)

$(LIBRARY): $(OBJS)
	$(LINK.cpp) -shared -o $(LIBRARY) $(OBJS)
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
	rm -f $(OBJS) $(PROGRAM) $(LIBRARY) core *~ *.tag *-gcov.* *.gcov *.d *.d.*  
	rm -rf html

docs:
	doxygen $(EXPORT_ROOT)/etc/Doxyfile

tdriver: 

test:

%-gcov.o: %.cpp
	$(COMPILE.cpp) -o $@ $^

tdriver-gcov: 

leakcheck:

%.d: %.cpp
	@set -e; rm -f $@; \
	$(CC) -MM $(CPPFLAGS) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

ifndef BOOTSTRAP
-include $(SRCS:.cpp=.d) 
endif
