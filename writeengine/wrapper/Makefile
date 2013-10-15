include ../../rules.mak
include ../build/we_rules.mak
#******************************************************************************************
# $Id: Makefile 4589 2013-04-02 14:41:08Z rdempsey $
#
# Copyright (C) 2009-2013 Calpont Corporation
# All rights reserved
#*****************************************************************************************/

.PHONY: install clean test docs coverage leakcheck

PROGRAM=libwriteengine.so
VERSION=1.0.0
LIBRARY=$(PROGRAM).$(VERSION)

TLIBS+=-lwriteengine
GLIBS=$(TLIBS)

LLIBS=-L$(CALPONT_LIBRARY_PATH) -lbrm -lcacheutils

SRCS=writeengine.cpp  we_colop.cpp we_colopcompress.cpp we_dctnrycompress.cpp we_tablemetadata.cpp
LINCLUDES=writeengine.h we_colop.h we_colopcompress.h we_dctnrycompress.h we_tablemetadata.h
OBJS=$(SRCS:.cpp=.o)

LOBJS=$(LOBJS_SHARED) $(LOBJS_DCTNRY) $(LOBJS_XML)
GOBJS=$(LOBJS:.o=-gcov.o)

$(LIBRARY): $(OBJS) $(LOBJS)
	$(LINK.cpp) -shared -o $(LIBRARY) $(OBJS) $(LOBJS)
	rm -f $(PROGRAM)
	ln -s $(LIBRARY) $(PROGRAM)
	make install

install: bootstrap
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
	rm -f $(LIBDIR)/tdriver.o  $(OBJS) tdriver.o $(PROGRAM) $(LIBRARY) tdriver core *~ *.tag *-gcov.* *.gcov tdriver-gcov *.d *.swp *.dat
	rm -rf html

docs:
	doxygen $(EXPORT_ROOT)/etc/Doxyfile

tdriver: tdriver.o
	$(LINK.cpp) -o $@ $^ $(TLIBS)

test:

xtest: $(LIBRARY) tdriver
	$(IPCS_CLEANUP)
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver

%-gcov.o: %.cpp
	$(COMPILE.cpp) -o $@ $^

tdriver-gcov: CXXFLAGS+=-fprofile-arcs -ftest-coverage
tdriver-gcov: tdriver-gcov.o $(subst .o,-gcov.o,$(OBJS))
	$(LINK.cpp) -o $@ $^ $(GLIBS) $(GOBJS)

coverage:

xcoverage: tdriver-gcov
	$(IPCS_CLEANUP)
	rm -f *.gcda
	LD_LIBRARY_PATH=$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver-gcov
	for file in $(SRCS); do \
		bfile=`basename $$file .cpp`; \
		gcov -o $${bfile}-gcov $$file >/dev/null; \
	done
	/usr/local/bin/genCoverage.pl $(SRCS)

leakcheck:

xleakcheck: $(LIBRARY) tdriver
	$(IPCS_CLEANUP)
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib valgrind --tool=memcheck --leak-check=yes ./tdriver

%.d: %.cpp
	@set -e; rm -f $@; \
	$(CC) -MM $(CPPFLAGS) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

ifndef BOOTSTRAP
-include $(SRCS:.cpp=.d) tdriver.d
endif
