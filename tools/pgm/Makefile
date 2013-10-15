include ../../rules.mak
#******************************************************************************************
# $Id: Makefile 444 2008-08-29 16:01:07Z rdempsey $
#
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
#*****************************************************************************************/

# The name of the executable
SPROGRAM=pgmsend
CPROGRAM=pgmrecv

# List all the source files here
SRCS=pgmsend.cpp prgrecv.cpp

# Run-time directories for project shared libs
CALPONT_LIBRARY_PATH=$(EXPORT_ROOT)/lib

GLIB_CFLAGS=$(shell pkg-config --cflags-only-I gthread-2.0)

# Preprocessor flags
CPPFLAGS=-I$(EXPORT_ROOT)/include -I/usr/include/libxml2 $(GLIB_CFLAGS) -I.

# Compiler flags
#DEBUG_FLAGS=-O0 -ggdb3
#DEBUG_FLAGS=-O3 -g0
CXXFLAGS+=$(DEBUG_FLAGS) -Wall

# Linker flags
LDFLAGS+=-L$(CALPONT_LIBRARY_PATH) -lmessageqcpp -lconfigcpp -lthreadpool -lloggingcpp -lboost_thread -lxml2 -lpgm -lglib-2.0  -lgthread-2.0 

.PHONY: install clean test docs FORCE coverage leakcheck

GLIB_LIBS=$(shell pkg-config --libs gthread-2.0)
CLIBS=-L$(EXPORT_ROOT)/lib $(GLIB_LIBS)
TLIBS=-L. -lpgm $(CLIBS)

OBJS=$(SRCS:.cpp=.o)

all: $(SPROGRAM) $(CPROGRAM) 

$(SPROGRAM): pgmsend.o
	$(LINK.cpp) $(LIBS) $(CLIBS) -o $@ $^

$(CPROGRAM): pgmrecv.o
	$(LINK.cpp) $(LIBS) -o $@ $^

install:
	mkdir -p $(INSTALL_ROOT_BIN)
	$(INSTALL) $(PROGRAM) $(INSTALL_ROOT_BIN)

clean:
	rm -f $(OBJS) $(SPROGRAM) $(CPROGRAM) core *~ *-gcov.* *.gcov *-gcov *.d config.tag *-prof.o *-prof server client
	rm -rf html

docs:
	doxygen $(EXPORT_ROOT)/etc/Doxyfile

FORCE:

%-prof.o: %.cpp
	$(COMPILE.cpp) -o $@ $^

$(SPROGRAM)-prof: CXXFLAGS+=-pg -O2 -g0 -DPROFILE
$(SPROGRAM)-prof: mainS-prof.o messagequeue-prof.o inetstreamsocket-prof.o
	$(LINK.cpp) -o $@ $^ $(LIBS)

%-gcov.o: %.cpp
	$(COMPILE.cpp) -o $@ $^

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
