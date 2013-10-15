include ./../../rules.mak
#******************************************************************************************
#
# $Id: Makefile 1855 2012-04-04 18:20:09Z rdempsey $
#
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
#*****************************************************************************************/

.PHONY: install clean test docs FORCE coverage leakcheck lport-install bootstrap

# The name of the executable
LIBRARY=libdbbc.a

# List all the source files here
SRCS=\
	blockcacheclient.cpp \
	blockrequestprocessor.cpp \
	fileblockrequestqueue.cpp \
	filebuffer.cpp \
	filebuffermgr.cpp \
	filerequest.cpp \
	iomanager.cpp \
	stats.cpp \
	fsutils.cpp

# Run-time directories for project shared libs
CALPONT_LIBRARY_PATH=$(EXPORT_ROOT)/lib

# Preprocessor flags
CPPFLAGS=-I$(EXPORT_ROOT)/include -I/usr/include/libxml2 -I../primproc

# Compiler flags
CXXFLAGS+=$(DEBUG_FLAGS) -Wall $(EXTRA_FLAGS)

# Linker flags
LDFLAGS+=-L. -L$(CALPONT_LIBRARY_PATH)

# install dir (set CALPONT_INSTALL_ROOT in env to override)
CALPONT_INSTALL_ROOT?=$(EXPORT_ROOT)

# end (sub-)project-specifc settings

TLIBS=-lpthread -lwriteengine -lbrm -lrwlock -lmessageqcpp \
-ldl -lconfigcpp -lxml2 -lloggingcpp -ldbbc

GLIBS=-lpthread -lwriteengine -lbrm -lrwlock -lmessageqcpp \
-ldl -lconfigcpp -lxml2 -lloggingcpp

OBJS=$(SRCS:.cpp=.o)

$(LIBRARY): $(OBJS)
	$(AR) scru $(LIBRARY) $(OBJS)

bootstrap:

install: bootstrap $(LIBRARY)

clean:
	rm -f $(OBJS) core *~ *-gcov.* *.gcov *.d config.tag *.d.* $(LIBRARY)
	rm -rf html

docs:
	doxygen $(EXPORT_ROOT)/etc/Doxyfile

%-gcov.o: %.cpp
	$(COMPILE.cpp) -o $@ $^

coverage:

zcoverage: tdriver-gcov
	rm -f *.gcda
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver-gcov
	for file in $(SRCS); do \
		bfile=`basename $$file .cpp`; \
		gcov -o $${bfile}-gcov $$file >/dev/null; \
	done
	/usr/local/bin/genCoverage.pl $(SRCS)

test:

# use -DXTEST flag for manual testing
xtest:
# 	CXXFLAGS+=$(CXXFLAGS) -DXTEST
	LD_LIBRARY_PATH=$(LD_LIBRARY_PATH) ./bcTest 10 1000
	
leakcheck:

xleakcheck:

xleakcheck: bcTest
	LD_LIBRARY_PATH=$(LD_LIBRARY_PATH) valgrind --tool=memcheck --leak-check=yes ./bcTest 2 10 32768

tdriver: tdriver.cpp $(LIBRARY)
	$(LINK.cpp) -o $@ $^ $(TLIBS)

%.d: %.cpp
	@set -e; rm -f $@; \
	$(CC) -MM $(CPPFLAGS) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$
	
bcTest: bcTest.cpp $(LIBRARY)
	$(LINK.cpp) -o $@ $^ $(TLIBS)

ifndef BOOTSTRAP
-include $(SRCS:.cpp=.d)
endif
