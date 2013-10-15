include ../../rules.mak
#******************************************************************************************
# $Id: Makefile 16 2006-05-12 19:59:20Z rdempsey $
#
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
#*****************************************************************************************/
.PHONY: install clean test docs coverage leakcheck

# Take this out for now...
#LIBS64=$(shell test `arch -k` == 'x86_64' && echo -DBOOST_WORKAROUND)

PROGRAM=libddlpackageproc.so
VERSION=1.0.0
LIBRARY=$(PROGRAM).$(VERSION)

CPPFLAGS=$(LIBS64) -I$(EXPORT_ROOT)/include -I/usr/include/libxml2
CXXFLAGS+=$(DEBUG_FLAGS) -Wall -fpic #-Wno-deprecated-declarations -fno-strict-aliasing

CLIBS=-L$(EXPORT_ROOT)/lib -lmessageqcpp -lconfigcpp -lxml2 -lcppunit \
-lddlpackage -lwriteengine -lwriteengineclient -lexecplan  -lloggingcpp -lrwlock \
-lbrm -ldl
TLIBS=-L. -lddlpackageproc -L../joblist -ljoblist -lfuncexp -ljoiner -lcommon \
	-L../resultset $(CLIBS) -ldataconvert -lrowgroup -lcacheutils $(IDB_WRITE_LIBS)
GLIBS=$(CLIBS) $(IDB_WRITE_LIBS)

LLIBS=-L$(CALPONT_LIBRARY_PATH) -lddlpackage -lexecplan -lwriteengine -lwriteengineclient

SRCS=\
ddlpackageprocessor.cpp \
createtableprocessor.cpp \
altertableprocessor.cpp \
droptableprocessor.cpp \
markpartitionprocessor.cpp \
restorepartitionprocessor.cpp \
droppartitionprocessor.cpp 

LINCLUDES=\
ddlpackageprocessor.h \
createtableprocessor.h \
altertableprocessor.h \
droptableprocessor.h \
markpartitionprocessor.h \
restorepartitionprocessor.h \
droppartitionprocessor.h 

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
	rm -f $(OBJS) tdriver.o $(PROGRAM) $(LIBRARY) tdriver core *~ *.tag *-gcov.* *.gcov tdriver-gcov *.d *.d.* *.dat
	rm -rf html

docs:
	doxygen $(EXPORT_ROOT)/etc/Doxyfile

tdriver: tdriver.o
	$(LINK.cpp) -o $@ $^ $(TLIBS)

test:

xtest: $(LIBRARY) tdriver
	rm -f FILE_*.dat
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver

%-gcov.o: %.cpp
	$(COMPILE.cpp) -o $@ $^

tdriver-gcov: CXXFLAGS+=-fprofile-arcs -ftest-coverage
tdriver-gcov: tdriver-gcov.o $(subst .o,-gcov.o,$(OBJS))
	$(LINK.cpp) -o $@ $^ $(GLIBS)

coverage:

xcoverage: tdriver-gcov
	rm -f *.gcda
	LD_LIBRARY_PATH=$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver-gcov
	for file in $(SRCS); do \
		bfile=`basename $$file .cpp`; \
		gcov -o $${bfile}-gcov $$file >/dev/null; \
	done
	/usr/local/bin/genCoverage.pl $(SRCS)

leakcheck:

xleakcheck: $(LIBRARY) tdriver
	rm -f FILE_*.dat
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib valgrind --tool=memcheck --leak-check=yes ./tdriver

%.d: %.cpp
	@set -e; rm -f $@; \
	$(CC) -MM $(CPPFLAGS) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

ifndef BOOTSTRAP
-include $(SRCS:.cpp=.d) tdriver.d
endif

