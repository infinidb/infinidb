#******************************************************************************************
# $Id: Makefile 3821 2013-05-17 23:58:16Z xlou $
#
# Copyright (C) 2009-2013 Calpont Corporation
# All rights reserved
#*****************************************************************************************/
include ../../rules.mak
.PHONY: install clean test docs coverage leakcheck

PROGRAM=libwindowfunction.so
VERSION=1.0.0
LIBRARY=$(PROGRAM).$(VERSION)

CPPFLAGS= -I. -I$(EXPORT_ROOT)/include -I/usr/include/libxml2
CXXFLAGS+=$(DEBUG_FLAGS) -Wall -fpic

TLIBS=-L. -L$(EXPORT_ROOT)/lib 
GLIBS=-L$(EXPORT_ROOT)/lib 

LLIBS=-L$(CALPONT_LIBRARY_PATH) -lwindowfunction

SRCS=\
	framebound.cpp \
	frameboundrange.cpp \
	frameboundrow.cpp \
	idborderby.cpp \
	windowframe.cpp \
	windowfunction.cpp \
	windowfunctiontype.cpp \
	wf_count.cpp \
	wf_lead_lag.cpp \
	wf_min_max.cpp \
	wf_nth_value.cpp \
	wf_ntile.cpp \
	wf_percentile.cpp \
	wf_ranking.cpp \
	wf_row_number.cpp \
	wf_stats.cpp \
	wf_sum_avg.cpp

LINCLUDES=\
	framebound.h \
	frameboundrange.h \
	frameboundrow.h \
	idborderby.h \
	windowframe.h \
	windowfunction.h \
	windowfunctiontype.h

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
	rm -rf $(PROGRAM) $(LIBRARY) $(OBJS)
	rm -rf *~ *.o *.d* *-gcov* *.gcov
	rm -rf html config.tag

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
