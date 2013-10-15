include ./../../rules.mak
#******************************************************************************************
#
# $Id: Makefile 609 2008-06-11 12:40:07Z rdempsey $
#
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
#*****************************************************************************************/

.PHONY: install clean test docs FORCE coverage leakcheck lport-install bootstrap

# The name of the executable
PROGRAM=cfread mtread

# List all the source files here
SRCS=cfread.cpp mtread.cpp
	
# Run-time directories for project shared libs
CALPONT_LIBRARY_PATH=$(EXPORT_ROOT)/lib

# Preprocessor flags
CPPFLAGS=-I$(EXPORT_ROOT)/include -I/usr/include/libxml2

# Compiler flags
CXXFLAGS+=$(DEBUG_FLAGS) -Wall

# Linker flags
LDFLAGS+=-L. -L$(CALPONT_LIBRARY_PATH)

# install dir (set CALPONT_INSTALL_ROOT in env to override)
CALPONT_INSTALL_ROOT?=$(EXPORT_ROOT)

# end (sub-)project-specifc settings

TLIBS=$(IDB_WRITE_LIBS) $(IDB_COMMON_LIBS) $(IDB_SNMP_LIBS)

OBJS=$(SRCS:.cpp=.o)

all: $(PROGRAM)

bootstrap:

mtread: mtread.cpp
	$(LINK.cpp) -o $@ $^ $(TLIBS)

cfread: cfread.cpp
	$(LINK.cpp) -o $@ $^ $(TLIBS)

install: bootstrap $(PROGRAM)
	mkdir -p $(INSTALL_ROOT_BIN)
	$(INSTALL) $(PROGRAM) $(INSTALL_ROOT_BIN)

clean:
	rm -f $(OBJS) core *~ *-gcov.* *.gcov *.d config.tag *.d.* $(PROGRAM)
	rm -rf html

docs:
	doxygen $(EXPORT_ROOT)/etc/Doxyfile

coverage:

test:

leakcheck:

tdriver:

%.d: %.cpp
	@set -e; rm -f $@; \
	$(CC) -MM $(CPPFLAGS) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$
	
ifndef BOOTSTRAP
-include $(SRCS:.cpp=.d)
endif
