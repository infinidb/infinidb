include ../../rules.mak
#******************************************************************************************
#
# $Id: Makefile 2142 2013-08-07 03:34:03Z bwilkinson $
#
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
#*****************************************************************************************/

# The name of the executable
PROGRAM=PrimProc
VERSION=1.0.0

# List all the source files here
SRCS=primproc.cpp \
	batchprimitiveprocessor.cpp \
	bppseeder.cpp \
	bppsendthread.cpp \
	columncommand.cpp \
	command.cpp \
	dictstep.cpp \
	filtercommand.cpp \
	logger.cpp \
	passthrucommand.cpp \
	primitiveserver.cpp \
	rtscommand.cpp \
	umsocketselector.cpp \
	udf.cpp \

# Run-time directories for project shared libs
CALPONT_LIBRARY_PATH=$(EXPORT_ROOT)/lib

# Preprocessor flags
CPPFLAGS=-I. -I../blockcache -I../linux-port -I$(EXPORT_ROOT)/include -I/usr/include/libxml2

# Compiler flags
CXXFLAGS+=$(DEBUG_FLAGS) -Wall $(EXTRA_FLAGS)

OAMLIBS=-loamcpp -lsnmpmanager -lnetsnmp -lnetsnmpagent -lnetsnmphelpers -lnetsnmpmibs

# Linker flags
LDFLAGS+=-L$(CALPONT_LIBRARY_PATH) $(IDB_COMMON_LIBS) -lthreadpool -lwriteengine \
	-lidbdatafile -lcacheutils $(IDB_SNMP_LIBS) -Wl,--rpath -Wl,/usr/local/Calpont/lib

# install dir (set CALPONT_INSTALL_ROOT in env to override)
CALPONT_INSTALL_ROOT?=$(EXPORT_ROOT)

# end (sub-)project-specifc settings

.PHONY: install clean test docs FORCE coverage leakcheck lport-install bootstrap

GLIBS=-lcppunit -ldl

OBJS=$(SRCS:.cpp=.o)

.PHONY: bc-install lport-install

all: $(PROGRAM)

../blockcache/libdbbc.a: bc-install

../linux-port/libprocessor.a: lport-install

$(PROGRAM): ../blockcache/libdbbc.a ../linux-port/libprocessor.a $(OBJS)
	$(LINK.cpp) -o $@ $(OBJS) ../blockcache/libdbbc.a ../linux-port/libprocessor.a

bc-install:
	$(MAKE) -C ../blockcache install

lport-install:
	$(MAKE) -C ../linux-port install

bootstrap:

install: bootstrap $(PROGRAM)
	mkdir -p $(INSTALL_ROOT_BIN)
	$(INSTALL) $(PROGRAM) $(INSTALL_ROOT_BIN)

clean:
	rm -f $(OBJS) $(PROGRAM) core *~ *-gcov.* *.gcov $(PROGRAM)-gcov *.d config.tag *.d.*
	rm -rf html

docs:
	doxygen $(EXPORT_ROOT)/etc/Doxyfile

FORCE:

%-gcov.o: %.cpp
	$(COMPILE.cpp) -o $@ $^

$(PROGRAM)-gcov: CXXFLAGS+=-fprofile-arcs -ftest-coverage
$(PROGRAM)-gcov: $(subst .o,-gcov.o,$(OBJS))
	$(LINK.cpp) -o $@ $^ $(GLIBS)

coverage:

xcoverage: $(PROGRAM)-gcov FORCE
	LD_LIBRARY_PATH=$(LD_LIBRARY_PATH) ./$(PROGRAM)-gcov
	for file in $(SRCS); do \
		bfile=`basename $$file .cpp`; \
		gcov -o $${bfile}-gcov $$file; \
	done

test:

xtest run:
	LD_LIBRARY_PATH=$(LD_LIBRARY_PATH) ./$(PROGRAM)

leakcheck:

xleakcheck: $(PROGRAM)
	LD_LIBRARY_PATH=$(LD_LIBRARY_PATH) valgrind --tool=memcheck --leak-check=yes ./$(PROGRAM)

tdriver: tdriver.o
	$(LINK.cpp) -o $@ $^
tdriver-umsocksel: tdriver-umsocksel.o umsocketselector.o
	$(LINK.cpp) -o $@ $^

%.d: %.cpp
	@set -e; rm -f $@; \
	$(CC) -MM $(CPPFLAGS) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

ifndef BOOTSTRAP
-include $(SRCS:.cpp=.d)
endif

