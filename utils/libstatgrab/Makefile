#******************************************************************************************
# $Id$
#
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
#*****************************************************************************************/
#
include ../../rules.mak
.PHONY: install clean test docs coverage leakcheck

CC=gcc
PROGRAM=libstatgrab.so
VERSION=6.2.0
LIBRARY=$(PROGRAM).$(VERSION)

CPPFLAGS=-I$(EXPORT_ROOT)/include
CFLAGS+=$(CXXFLAGS) $(DEBUG_FLAGS) -Wall -fpic -DHAVE_CONFIG_H -DUSE_THIS_CONFIG_H

CLIBS=-L$(EXPORT_ROOT)/lib -lcppunit -ldl
TLIBS=-L. $(CLIBS)
GLIBS=$(CLIBS)

LLIBS=-L$(CALPONT_LIBRARY_PATH)

SRCS=\
cpu_stats.c \
disk_stats.c \
load_stats.c \
memory_stats.c \
network_stats.c \
os_info.c \
page_stats.c \
process_stats.c \
swap_stats.c \
user_stats.c \
tools.c \
vector.c \
error.c \
statgrab_deprecated.c

LINCLUDES=statgrab.h

OBJS=$(SRCS:.c=.o)

$(LIBRARY): $(OBJS)
	$(LINK.c) -shared -Wl,-soname -Wl,libstatgrab.so.6 -o $(LIBRARY) $(OBJS)
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

test:

xtest: $(LIBRARY) tdriver
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver

%-gcov.o: %.cpp
	$(COMPILE.cpp) -o $@ $^

tdriver-gcov: CXXFLAGS+=-fprofile-arcs -ftest-coverage
tdriver-gcov: tdriver-gcov.o $(subst .o,-gcov.o,$(OBJS))
	$(LINK.cpp) -o $@ $^ $(GLIBS)

coverage:

xcoverage: tdriver-gcov
	rm -f *.gcda
	LD_LIBRARY_PATH=$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver-gcov
	for file in $(SRCS); do \
		bfile=`basename $$file .cpp`; \
		gcov -o $${bfile}-gcov $$file >/dev/null; \
	done
	/usr/local/bin/genCoverage.pl $(SRCS)

leakcheck:

xleakcheck: $(LIBRARY) tdriver
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib valgrind --tool=memcheck --leak-check=yes ./tdriver

%.d: %.c
	@set -e; rm -f $@; \
	$(CC) -MM $(CPPFLAGS) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

ifndef BOOTSTRAP
-include $(SRCS:.c=.d) tdriver.d
endif
