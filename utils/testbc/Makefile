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
PROGRAM=testbc testbc2 testbc3
LIBRARY=libdbbc.a

# List all the source files here
SRCS=\
	blockcacheclient.cpp \
	blockrequestprocessor.cpp \
	fileblockrequestqueue.cpp \
	filebuffer.cpp \
	filebuffermgr.cpp \
	filerequest.cpp \
	logger.cpp \
	iomanager.cpp \
	stats.cpp

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

TLIBS=-lpthread -lwriteengine -lbrm -lrwlock -lmessageqcpp \
-ldl -lconfigcpp -lxml2 -lboost_thread -lloggingcpp

LD_LIBRARY_PATH=.:$(CALPONT_LIBRARY_PATH):/usr/local/lib

OBJS=$(SRCS:.cpp=.o)

all: $(PROGRAM)


testbc: testbc.cpp $(LIBRARY)
	$(LINK.cpp) -o $@ $^ $(TLIBS)

testbc2: testbc2.cpp $(LIBRARY)
	$(LINK.cpp) -o $@ $^ $(TLIBS)

testbc3: testbc3.cpp $(LIBRARY)
	$(LINK.cpp) -o $@ $^ $(TLIBS)

$(LIBRARY): $(OBJS)
	$(AR) scru $(LIBRARY) $(OBJS)

bootstrap:

install: bootstrap $(LIBRARY)

clean:
	rm -f $(PROGRAM) $(OBJS) core *~ *-gcov.* *.gcov *.d config.tag *.d.* $(LIBRARY)
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

%.d: %.cpp
	@set -e; rm -f $@; \
	$(CC) -MM $(CPPFLAGS) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$
	
ifndef BOOTSTRAP
-include $(SRCS:.cpp=.d)
endif
