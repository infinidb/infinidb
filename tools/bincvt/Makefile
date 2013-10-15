include ../../rules.mak
#******************************************************************************************
# $Id$
#
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
#*****************************************************************************************/

# The name of the executable
PROGRAM1=li2bin

# List all the source files here
SRCS1=li2bin.cpp

# Run-time directories for project shared libs
CALPONT_LIBRARY_PATH=$(EXPORT_ROOT)/lib

# Preprocessor flags
CPPFLAGS=-I$(EXPORT_ROOT)/include -I/usr/include/libxml2

# Compiler flags
CXXFLAGS+=$(DEBUG_FLAGS) -Wall

# Linker flags
LDFLAGS+=-L$(CALPONT_LIBRARY_PATH) -Wl,--rpath -Wl,/usr/local/Calpont/lib

# install dir (set CALPONT_INSTALL_ROOT in env to override)
CALPONT_INSTALL_ROOT?=$(EXPORT_ROOT)

# end (sub-)project-specifc settings

.PHONY: install clean test docs FORCE coverage leakcheck bootstrap

GLIBS=

OBJS1=$(SRCS1:.cpp=.o)

$(PROGRAM1): $(OBJS1)
	$(LINK.cpp) -o $@ $^

bootstrap:

install: bootstrap $(PROGRAM1)
	mkdir -p $(INSTALL_ROOT_BIN)
	$(INSTALL) $(PROGRAM1) $(INSTALL_ROOT_BIN)

clean:
	rm -f $(OBJS1) $(PROGRAM1) core *~ *-gcov.* *.gcov $(PROGRAM1)-gcov *.d config.tag *.d.*
	rm -rf html

docs:
	doxygen $(EXPORT_ROOT)/etc/Doxyfile

FORCE:

%-gcov.o: %.cpp
	$(COMPILE.cpp) -o $@ $^

$(PROGRAM1)-gcov: CXXFLAGS+=-fprofile-arcs -ftest-coverage
$(PROGRAM1)-gcov: $(subst .o,-gcov.o,$(OBJS1))
	$(LINK.cpp) -o $@ $^ $(GLIBS)

coverage:

xcoverage: $(PROGRAM1)-gcov FORCE
	LD_LIBRARY_PATH=$(LD_LIBRARY_PATH) ./$(PROGRAM1)-gcov
	for file in $(SRCS1); do \
		bfile=`basename $$file .cpp`; \
		gcov -o $${bfile}-gcov $$file; \
	done

tdriver: tdriver.o
	$(LINK.cpp) -o $@ $^ -lboost_program_options

test:

xtest run:
	LD_LIBRARY_PATH=$(LD_LIBRARY_PATH) ./$(PROGRAM1)

leakcheck:

xleakcheck: $(PROGRAM1)
	LD_LIBRARY_PATH=$(LD_LIBRARY_PATH) valgrind --tool=memcheck --leak-check=yes ./$(PROGRAM1)

%.d: %.cpp
	@set -e; rm -f $@; \
	$(CC) -MM $(CPPFLAGS) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

ifndef BOOTSTRAP
-include $(SRCS1:.cpp=.d)
endif

