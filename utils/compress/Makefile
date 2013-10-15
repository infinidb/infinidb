#******************************************************************************************
# $Id: Makefile 3437 2012-12-04 20:43:29Z rdempsey $
#
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
#*****************************************************************************************/
include ../../rules.mak
.PHONY: install clean test docs coverage leakcheck

PROGRAM=libcompress.so
VERSION=1.0.0
LIBRARY=$(PROGRAM).$(VERSION)

CPPFLAGS=-I$(EXPORT_ROOT)/include -DNDEBUG
CXXFLAGS+=$(DEBUG_FLAGS) -Wall -fpic
CFLAGS+=$(CXXFLAGS)

TLIBS=-L. -L$(EXPORT_ROOT)/lib 
GLIBS=-L$(EXPORT_ROOT)/lib 

CXXSRCS=idbcompress.cpp snappy.cpp snappy-sinksource.cpp version1.cpp
CSRCS=

LINCLUDES=idbcompress.h

CXXOBJS=$(CXXSRCS:.cpp=.o)
COBJS=$(CSRCS:.c=.o)

$(LIBRARY): $(CXXOBJS) $(COBJS)
	$(LINK.cpp) -shared -o $(LIBRARY) $(CXXOBJS) $(COBJS)
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
	rm -rf $(PROGRAM) $(LIBRARY) $(COBJS) $(CXXOBJS) *~ *.o *.d* *-gcov* *.gcov

docs:
	doxygen $(EXPORT_ROOT)/etc/Doxyfile

%.d: %.cpp
	@set -e; rm -f $@; \
	$(CC) -MM $(CPPFLAGS) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

ifndef BOOTSTRAP
-include $(CXXSRCS:.cpp=.d) $(CSRCS:.c=.d)
endif
