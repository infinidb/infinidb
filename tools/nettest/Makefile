include ../../rules.mak
#******************************************************************************************
# $Id: Makefile 1763 2012-04-04 18:20:36Z rdempsey $
#
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
#*****************************************************************************************/

# The name of the executable
SPROGRAM=nettestS
CPROGRAM=nettestC

# List all the source files here
SRCS=mainS.cpp mainC.cpp

# Run-time directories for project shared libs
CALPONT_LIBRARY_PATH=$(EXPORT_ROOT)/lib

# Preprocessor flags
CPPFLAGS=-I$(EXPORT_ROOT)/include -I/usr/include/libxml2

# Compiler flags
CXXFLAGS+=$(DEBUG_FLAGS) -Wall

# Linker flags
LDFLAGS+=-L$(CALPONT_LIBRARY_PATH) -lmessageqcpp -lconfigcpp -lthreadpool -lloggingcpp -lboost_thread -lxml2

.PHONY: install clean test docs FORCE coverage leakcheck

GLIBS=-lcppunit -ldl

OBJS=$(SRCS:.cpp=.o)

all: $(SPROGRAM) $(CPROGRAM) $(SPROGRAM)-prof $(CPROGRAM)-prof

lh_bench: 
	$(CC) -o client lh_benchC.c
	$(CC) -o server lh_benchS.c

$(SPROGRAM): mainS.o
	$(LINK.cpp) $(LIBS) -o $@ $^

$(CPROGRAM): mainC.o
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

$(CPROGRAM)-prof: CXXFLAGS+=-pg -O2 -g0
$(CPROGRAM)-prof: mainC-prof.o messagequeue-prof.o inetstreamsocket-prof.o
	$(LINK.cpp) -o $@ $^ $(LIBS)

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
