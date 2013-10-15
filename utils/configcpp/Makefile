#******************************************************************************************
# $Id: Makefile 3465 2012-12-21 17:17:23Z dhill $
#
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
#*****************************************************************************************/
include ../../rules.mak
.PHONY: install clean test docs coverage leakcheck

PROGRAM=libconfigcpp.so
VERSION=1.0.0
LIBRARY=$(PROGRAM).$(VERSION)

CPPFLAGS=-I. -I$(EXPORT_ROOT)/include -I/usr/include/libxml2
CXXFLAGS+=$(DEBUG_FLAGS) -Wall -fpic

CLIBS=-L$(EXPORT_ROOT)/lib -lxml2 -lcppunit -ldl
TLIBS=-L. -lconfigcpp -lmessageqcpp -lloggingcpp -lidbboot -lboost_idb $(CLIBS)
GLIBS=$(TLIBS)

LLIBS=-L$(CALPONT_LIBRARY_PATH) -lmessageqcpp -lxml2

SRCS=configcpp.cpp writeonce.cpp xmlparser.cpp configstream.cpp
LINCLUDES=configcpp.h config.h xmlparser.h

OBJS=$(SRCS:.cpp=.o)

$(LIBRARY): $(OBJS)
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
	rm -f $(OBJS) tdriver*.o $(PROGRAM) $(LIBRARY) tdriver tdriver2 core *~ *.tag *-gcov.* *.gcov tdriver*-gcov *.d
	rm -rf html

docs:
	doxygen $(EXPORT_ROOT)/etc/Doxyfile

tdriver: tdriver.o
	$(LINK.cpp) -o $@ $^ $(TLIBS)

tdriver2: tdriver2.o
	$(LINK.cpp) -o $@ $^ $(TLIBS)

test: $(LIBRARY) tdriver tdriver2
	$(MAKE) -C ../messageqcpp install
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib:/usr/local/gcc45/lib64 ./tdriver
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib:/usr/local/gcc45/lib64 ./tdriver2

%-gcov.o: %.cpp
	$(COMPILE.cpp) -o $@ $^

tdriver-gcov: CXXFLAGS+=-fprofile-arcs -ftest-coverage
tdriver-gcov: tdriver-gcov.o $(subst .o,-gcov.o,$(OBJS))
	$(LINK.cpp) -o $@ $^ $(GLIBS)

tdriver2-gcov: CXXFLAGS+=-fprofile-arcs -ftest-coverage
tdriver2-gcov: tdriver2-gcov.o $(subst .o,-gcov.o,$(OBJS))
	$(LINK.cpp) -o $@ $^ $(GLIBS)

coverage: tdriver-gcov tdriver2-gcov
	rm -f *.gcda
	LD_LIBRARY_PATH=$(EXPORT_ROOT)/lib:/usr/local/lib:/usr/local/gcc45/lib64 ./tdriver-gcov
	LD_LIBRARY_PATH=$(EXPORT_ROOT)/lib:/usr/local/lib:/usr/local/gcc45/lib64 ./tdriver2-gcov
	for file in $(SRCS); do \
		bfile=`basename $$file .cpp`; \
		gcov -o $${bfile}-gcov $$file >/dev/null; \
	done
	/usr/local/bin/genCoverage.pl $(SRCS)

leakcheck: $(LIBRARY) tdriver tdriver2
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib:/usr/local/gcc45/lib64 valgrind --tool=memcheck --leak-check=yes ./tdriver
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib:/usr/local/gcc45/lib64 valgrind --tool=memcheck --leak-check=yes ./tdriver2

%.d: %.cpp
	@set -e; rm -f $@; \
	$(CC) -MM $(CPPFLAGS) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

ifndef BOOTSTRAP
-include $(SRCS:.cpp=.d) tdriver.d tdriver2.d
endif
