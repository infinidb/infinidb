include ../rules.mak
#******************************************************************************************
#
# $Id: Makefile 778 2012-06-15 15:24:24Z bpaul $
#
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
#*****************************************************************************************/

# The name of the executable
PROGRAM=DMLProc
VERSION=1.0.0

# List all the source files here
SRCS=dmlproc.cpp dmlprocessor.cpp dmlresultbuffer.cpp batchinsertprocessor.cpp

# Run-time directories for project shared libs
CALPONT_LIBRARY_PATH=$(EXPORT_ROOT)/lib

# Preprocessor flags
CPPFLAGS=-I$(EXPORT_ROOT)/include -I/usr/include/libxml2

# Compiler flags
CXXFLAGS+=$(DEBUG_FLAGS) -Wall

# Linker flags
LDFLAGS+=-L$(CALPONT_LIBRARY_PATH) $(IDB_WRITE_LIBS) $(IDB_COMMON_LIBS) -lthreadpool -lddlcleanuputil -lbatchloader $(IDB_SNMP_LIBS) -Wl,--rpath -Wl,/usr/local/Calpont/lib

# install dir (set CALPONT_INSTALL_ROOT in env to override)
CALPONT_INSTALL_ROOT?=$(EXPORT_ROOT)

# end (sub-)project-specifc settings

.PHONY: install clean test docs FORCE coverage leakcheck

GLIBS=-lcppunit -ldl -lbatchloader

OBJS=$(SRCS:.cpp=.o)

$(PROGRAM): $(OBJS)
	$(LINK.cpp) -o $@ $^

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

%.d: %.cpp
	@set -e; rm -f $@; \
	$(CC) -MM $(CPPFLAGS) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

-include $(SRCS:.cpp=.d)

