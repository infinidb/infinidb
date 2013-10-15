include ../../rules.mak
#******************************************************************************************
# $Id: Makefile 3193 2012-07-23 18:47:31Z pleblanc $
#
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
#*****************************************************************************************/
.PHONY: install clean test docs coverage leakcheck

PROGRAM=libthreadpool.so
VERSION=1.0.0
LIBRARY=$(PROGRAM).$(VERSION)

CPPFLAGS=-I$(EXPORT_ROOT)/include -I/usr/include/libxml2
CXXFLAGS+=$(DEBUG_FLAGS) -Wall -fpic

CLIBS=-L$(EXPORT_ROOT)/lib -lconfigcpp -lxml2 -lcppunit -lloggingcpp -ldl
XLIBS=-L. -lthreadpool -L$(EXPORT_ROOT)/lib -lconfigcpp -lxml2 -lloggingcpp -ldl
TLIBS=-L. -lthreadpool $(CLIBS) -lmessageqcpp
GLIBS=$(CLIBS) -lmessageqcpp

LLIBS=-L$(CALPONT_LIBRARY_PATH) -lloggingcpp

SRCS=weightedthreadpool.cpp threadpool.cpp prioritythreadpool.cpp
LINCLUDES=weightedthreadpool.h threadpool.h prioritythreadpool.h

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
	rm -f $(OBJS) tdriver.o $(PROGRAM) $(LIBRARY) tdriver core *~ *.tag *-gcov.* *.gcov tdriver-gcov *.d
	rm -rf html

docs:
	doxygen $(EXPORT_ROOT)/etc/Doxyfile

tp: tp.o
	$(LINK.cpp) -o $@ $^ $(XLIBS)

wtp: wtp.o
	$(LINK.cpp) -o $@ $^ $(XLIBS)

tdriver: tdriver.o
	$(LINK.cpp) -o $@ $^ $(TLIBS)

test: $(LIBRARY) tdriver
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver

%-gcov.o: %.cpp
	$(COMPILE.cpp) -o $@ $^

tdriver-gcov: CXXFLAGS+=-fprofile-arcs -ftest-coverage
tdriver-gcov: tdriver-gcov.o $(subst .o,-gcov.o,$(OBJS))
	$(LINK.cpp) -o $@ $^ $(GLIBS)

coverage: tdriver-gcov
	rm -f *.gcda
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver-gcov
	for file in $(SRCS); do \
		bfile=`basename $$file .cpp`; \
		gcov -o $${bfile}-gcov $$file >/dev/null; \
	done
	/usr/local/bin/genCoverage.pl $(SRCS)

PGLIBS=$(GLIBS)

%-prof.o: %.cpp
	$(COMPILE.cpp) -o $@ $^

tdriver-prof: CXXFLAGS+=-pg
tdriver-prof: tdriver-prof.o $(subst .o,-prof.o,$(OBJS))
	$(LINK.cpp) -o $@ $^ $(PGLIBS)

profile: tdriver-prof
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver-prof
	gprof ./tdriver-prof

leakcheck: $(LIBRARY) tdriver
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib valgrind --tool=memcheck --leak-check=yes ./tdriver --leakcheck

%.d: %.cpp
	@set -e; rm -f $@; \
	$(CC) -MM $(CPPFLAGS) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

ifndef BOOTSTRAP
-include $(SRCS:.cpp=.d) tdriver.d
endif

