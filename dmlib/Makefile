#******************************************************************************************
# $Id: Makefile 466 2011-11-01 16:45:40Z rdempsey $
#
# Copyright (C) 2006 Calpont Corp
# All rights reserved
#*****************************************************************************************/
include ../rules.mak
.PHONY: install clean test docs coverage leakcheck

CC=gcc
PROGRAM=libdm.so
VERSION=1.0.0
LIBRARY=$(PROGRAM).$(VERSION)

CPPFLAGS=-I$(EXPORT_ROOT)/include -I/usr/include/libxml2
CFLAGS=$(CXXFLAGS) $(DEBUG_FLAGS) -Wall -fpic

CLIBS=-L$(EXPORT_ROOT)/lib
TLIBS=-L. -ldm $(CLIBS)
GLIBS=$(CLIBS)

CSRCS=dm.c
CPPSRCS=
LINCLUDES=

OBJS=$(CSRCS:.c=.o)
OBJS+=$(CPPSRCS:.cpp=.o)

$(LIBRARY): $(OBJS)
	$(LINK.c) -shared -o $(LIBRARY) $(OBJS)
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


clean:
	rm -f $(OBJS) tdriver.o $(PROGRAM) $(LIBRARY) tdriver core *~ *.tag *-gcov.* *.gcov tdriver-gcov *.d
	rm -rf html

docs:
	doxygen $(EXPORT_ROOT)/etc/Doxyfile

tdriver: tdriver.o
	$(LINK.c) -o $@ $^ $(TLIBS)

test:

xtest: $(LIBRARY) tdriver
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver

%-gcov.o: %.c
	$(COMPILE.c) -o $@ $^

tdriver-gcov: CFLAGS+=-fprofile-arcs -ftest-coverage
tdriver-gcov: tdriver-gcov.o $(subst .o,-gcov.o,$(OBJS))
	$(LINK.c) -o $@ $^ $(GLIBS)

coverage:

xcoverage: tdriver-gcov
	rm -f *.gcda
	LD_LIBRARY_PATH=$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver-gcov
	for file in $(CSRCS); do \
		bfile=`basename $$file .c`; \
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
-include $(CSRCS:.c=.d) tdriver.d
endif
