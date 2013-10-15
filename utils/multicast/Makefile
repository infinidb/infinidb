include ../../rules.mak
#******************************************************************************************
# $Id: $
#
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
#*****************************************************************************************/
.PHONY: install clean test docs coverage leakcheck
# The name of the executable

PROGRAM=libmulticast.so
VERSION=1.0.0
LIBRARY=$(PROGRAM).$(VERSION)

SRCS=multicast.cpp

LINCLUDES=multicast.h 

# Run-time directories for project shared libs
CALPONT_LIBRARY_PATH=$(EXPORT_ROOT)/lib

# Preprocessor flags
CPPFLAGS=-I. -I$(EXPORT_ROOT)/include -I/usr/include/libxml2

# Compiler flags
CXXFLAGS+=$(DEBUG_FLAGS) -Wall -fPIC

# Linker flags
LDFLAGS+=-L$(CALPONT_LIBRARY_PATH) -lmessageqcpp -lconfigcpp -lloggingcpp -lxml2

.PHONY: install clean test docs coverage leakcheck

CLIBS=-L$(EXPORT_ROOT)/lib
TLIBS=-L. $(CLIBS)

LLIBS=-L$(CALPONT_LIBRARY_PATH)

OBJS=$(SRCS:.cpp=.o)

$(LIBRARY): $(OBJS)
	$(MAKE) -C ../messageqcpp install
	$(MAKE) -C ../configcpp install
	$(MAKE) -C ../threadpool install
	$(MAKE) -C ../loggingcpp install
	$(LINK.cpp) -shared -o $(LIBRARY) $(OBJS)
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
	$(INSTALL) $(LINCLUDES) $(INSTALL_ROOT_INCLUDE)

clean:
	rm -f $(OBJS) tdriver.o $(PROGRAM) $(LIBRARY) tdriver core *~ *.tag *-gcov.* *.gcov tdriver-gcov *.d *.d.*

docs:
	doxygen $(EXPORT_ROOT)/etc/Doxyfile

%.d: %.cpp
	@set -e; rm -f $@; \
	$(CC) -MM $(CPPFLAGS) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

ifndef BOOTSTRAP
-include $(SRCS:.cpp=.d)
endif
