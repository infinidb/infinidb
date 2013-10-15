include ../../rules.mak
#******************************************************************************************
# $Id$
#
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
#*****************************************************************************************/

# The name of the executable
PROGRAM=QFE
VERSION=1.0.0

# List all the source files here
SRCS=server.cpp parsequery.cpp sendcsep.cpp returnedrows.cpp socketio.cpp cseputils.cpp ddlstmts.cpp

# Run-time directories for project shared libs
CALPONT_LIBRARY_PATH=$(EXPORT_ROOT)/lib

# Preprocessor flags
CPPFLAGS=-I$(EXPORT_ROOT)/include -I/usr/include/libxml2

# Compiler flags
### DEBUG_FLAGS=-g
CXXFLAGS+=$(DEBUG_FLAGS) -Wall -Wno-unused-label

# Linker flags
LDFLAGS+=-L$(CALPONT_LIBRARY_PATH) -Wl,--rpath -Wl,/usr/local/Calpont/lib 
QFE_LDLIBS=$(IDB_WRITE_LIBS) $(IDB_COMMON_LIBS) $(IDB_SNMP_LIBS)
CLI_LDLIBS=$(IDB_COMMON_LIBS) $(IDB_SNMP_LIBS)

.PHONY: install clean test docs coverage leakcheck

GLIBS=-lcppunit -ldl

OBJS=$(SRCS:.cpp=.o)

all: $(PROGRAM) cli

$(PROGRAM): qfeparser.o qfelexer.o $(OBJS)
	$(LINK.cpp) $(QFE_LDLIBS) -o $@ $^

qfelexer.cpp: qfelexer.lpp
	flex -i -Pqfe -oqfelexer.cpp qfelexer.lpp

qfeparser.cpp: qfeparser.ypp
	bison -d -p qfe -o qfeparser.cpp qfeparser.ypp
	cmp -s qfeparser.hpp qfeparser.h || cp qfeparser.hpp qfeparser.h

install: $(PROGRAM)
	mkdir -p $(INSTALL_ROOT_BIN)
	$(INSTALL) $(PROGRAM) $(INSTALL_ROOT_BIN)

clean:
	rm -f $(OBJS) $(PROGRAM) core *~ *-gcov.* *.gcov $(PROGRAM)-gcov *.d *.d.* config.tag cli.o cli
	rm -rf html
	rm -rf qfeparser.h qfeparser.cpp qfeparser.hpp qfelexer.cpp qfeparser.output qfeparser.o qfelexer.o

docs:
	doxygen $(EXPORT_ROOT)/etc/Doxyfile

cli: cli.o socketio.o
	$(LINK.cpp) $(CLI_LDLIBS) -o $@ $^

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

xtest run: $(PROGRAM)
	LD_LIBRARY_PATH=$(LD_LIBRARY_PATH) ./$(PROGRAM)

leakcheck:

xleakcheck: $(PROGRAM)
	LD_LIBRARY_PATH=$(LD_LIBRARY_PATH) valgrind --tool=memcheck --leak-check=yes ./$(PROGRAM)

%.d: %.cpp
	@set -e; rm -f $@; \
	$(CC) -MM $(CPPFLAGS) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

ifndef BOOTSTRAP
-include $(SRCS:.cpp=.d)
endif
