include ./../../rules.mak
#******************************************************************************************
#
# $Id$
#
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
#*****************************************************************************************/

.PHONY: install clean test docs FORCE coverage leakcheck lport-install bootstrap

# The name of the executable
LIBRARY=libidbboot.a

# List all the source files here
SRCS=installdir.cpp

LINCLUDES=installdir.h

# Run-time directories for project shared libs
CALPONT_LIBRARY_PATH=$(EXPORT_ROOT)/lib

# Preprocessor flags
CPPFLAGS=-I$(EXPORT_ROOT)/include -I/usr/include/libxml2

# Compiler flags
CXXFLAGS+=$(DEBUG_FLAGS) -Wall -fPIC -DPIC

# Linker flags
LDFLAGS+=-L$(CALPONT_LIBRARY_PATH)

# install dir (set CALPONT_INSTALL_ROOT in env to override)
CALPONT_INSTALL_ROOT?=$(EXPORT_ROOT)

# end (sub-)project-specifc settings

TLIBS=-lpthread

GLIBS=-lpthread

OBJS=$(SRCS:.cpp=.o)

$(LIBRARY): $(OBJS)
	$(AR) scru $(LIBRARY) $(OBJS)

bootstrap:
	$(INSTALL) $(LINCLUDES) $(INSTALL_ROOT_INCLUDE)

install: bootstrap $(LIBRARY)
	umask 002; \
	mkdir -p $(INSTALL_ROOT_LIB) $(INSTALL_ROOT_INCLUDE); \
	$(INSTALL) $(LIBRARY) $(INSTALL_ROOT_LIB)

clean:
	rm -f $(OBJS) core *~ *-gcov.* *.gcov *.d config.tag *.d.* $(LIBRARY)
	rm -rf html

docs:
	doxygen $(EXPORT_ROOT)/etc/Doxyfile

%-gcov.o: %.cpp
	$(COMPILE.cpp) -o $@ $^

coverage:

test:

leakcheck:

%.d: %.cpp
	@set -e; rm -f $@; \
	$(CC) -MM $(CPPFLAGS) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$
	
ifndef BOOTSTRAP
-include $(SRCS:.cpp=.d)
endif
