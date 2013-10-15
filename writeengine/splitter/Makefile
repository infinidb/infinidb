include ../../rules.mak
include ../build/we_rules.mak
#******************************************************************************************
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
#
# $Id: Makefile 3153 2011-10-12 20:41:55Z bpaul $
#
#*****************************************************************************************/
# The name of the executable
PROGRAM=cpimport
VERSION=1.0.0

# List all the source files here
SRCS=we_splitterapp.cpp \
     we_cmdargs.cpp \
     we_sdhandler.cpp \
     we_respreadthread.cpp \
     we_filereadthread.cpp \
     we_splclient.cpp \
     we_brmupdater.cpp \
     we_tablelockgrabber.cpp \
     we_xmlgetter.cpp

# Preprocessor flags
CPPFLAGS=-I$(EXPORT_ROOT)/include -I/usr/include/libxml2

# Compiler flags
CXXFLAGS+=$(DEBUG_FLAGS) -Wall

# Linker flags
LDFLAGS+=-L$(CALPONT_LIBRARY_PATH) $(IDB_COMMON_LIBS) $(IDB_WRITE_LIBS) -lthreadpool $(IDB_SNMP_LIBS) -Wl,--rpath -Wl,/usr/local/Calpont/lib -lbatchloader

.PHONY: install clean docs test coverage leakcheck 
TLIBS+=-L$(CALPONT_LIBRARY_PATH) $(IDB_COMMON_LIBS) $(IDB_WRITE_LIBS) 
GLIBS=-L$(CALPONT_LIBRARY_PATH) $(IDB_COMMON_LIBS) $(IDB_WRITE_LIBS) $(IDB_SNMP_LIBS) -lthreadpool -ldl -lbatchloader

LINCLUDES=

OBJS=$(SRCS:.cpp=.o)

objects: $(OBJS)

GOBJS=$(LOBJS:.o=-gcov.o)

$(PROGRAM): $(OBJS)
	$(LINK.cpp) -o $@ $^


install: bootstrap

install_splitter: $(PROGRAM) 
	mkdir -p $(INSTALL_ROOT_BIN)
	$(INSTALL) $(PROGRAM) $(INSTALL_ROOT_BIN)

bootstrap:
#	$(INSTALL) $(LINCLUDES) $(INSTALL_ROOT_INCLUDE)

clean:
	rm -f $(PROGRAM) $(OBJS) core *~ *.tag *-gcov.* *.gcov *.d *.swp *.dat
	rm -rf html

docs:
	doxygen $(EXPORT_ROOT)/etc/Doxyfile

test:

xtest: $(PROGRAM) 
	LD_LIBRARY_PATH=$(LD_LIBRARY_PATH) ./$(PROGRAM)

%-gcov.o: %.cpp
	$(COMPILE.cpp) -o $@ $^

$(PROGRAM)-gcov: CXXFLAGS+=-fprofile-arcs -ftest-coverage
$(PROGRAM)-gcov: $(PROGRAM)-gcov.o $(subst .o,-gcov.o,$(OBJS))
	$(LINK.cpp) -o $@ $^ $(GOBJS) $(GLIBS)
	cp *-gcov.o $(LIBDIR)

coverage:

xcoverage: $(PROGRAM)-gcov
	$(IPCS_CLEANUP)
	rm -f *.gcda
	LD_LIBRARY_PATH=$(LD_LIBRARY_PATH) ./$(PROGRAM)-gcov
	for file in $(SRCS); do \
		bfile=`basename $$file .cpp`; \
		gcov -o $${bfile}-gcov $$file >/dev/null; \
	done
#	/usr/local/bin/genCoverage.pl $(SRCS)

leakcheck:

xleakcheck: $(PROGRAM) 
	$(IPCS_CLEANUP)
	LD_LIBRARY_PATH=$(LD_LIBRARY_PATH) valgrind --error-limit=no --tool=memcheck --leak-check=yes ./$(PROGRAM)

%.d: %.cpp
	@set -e; rm -f $@; \
	$(CC) -MM $(CPPFLAGS) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

ifndef BOOTSTRAP
-include $(SRCS:.cpp=.d)
endif
