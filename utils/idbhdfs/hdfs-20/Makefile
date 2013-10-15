#******************************************************************************************
# $Id: Makefile 1058 2006-08-08 15:38:16Z rdempsey $
#
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
#*****************************************************************************************/
include ../../../rules.mak
.PHONY: install clean test docs coverage leakcheck

PLUGIN_SRC=../hdfs-shared
LIBRARY=hdfs-20.so
SCRIPTS=setenv-hdfs-20

CPPFLAGS=-I$(EXPORT_ROOT)/include -I/usr/include/libxml2 -I.
NEW_DEBUG_FLAGS=$(shell echo "$(DEBUG_FLAGS)" | sed s/-flto//g)
CXXFLAGS+=$(NEW_DEBUG_FLAGS) -Wall -fpic -DCDH4

TLIBS=-L. -L$(EXPORT_ROOT)/lib 
GLIBS=-L$(EXPORT_ROOT)/lib 

LDPATH= -L. -L../jvm/jdk1.6.0_31

LLIBS=$(LDPATH) -ljvm -lhdfs

SRCS=\
	HdfsFile.cpp \
	HdfsFileSystem.cpp \
	HdfsFsCache.cpp	\
	HdfsRdwrFileBuffer.cpp \
	HdfsRdwrMemBuffer.cpp \
	HdfsPlugin.cpp

%.o : $(PLUGIN_SRC)/%.cpp
	@mkdir -p $(dir $@)
	@echo "============="
	@echo "Compiling $<"
	@echo $(CXX) $(CXXFLAGS) $(CPPFLAGS) -c $< -o $@
	@$(CXX) $(CXXFLAGS) $(CPPFLAGS) -c $< -o $@
        
LINCLUDES=\

OBJS=$(SRCS:.cpp=.o)

$(LIBRARY): $(OBJS)
	$(LINK.cpp) -shared -o $(LIBRARY) $(OBJS) -L. -lhdfs

install: bootstrap $(PROGRAM) $(SCRIPTS)
	umask 002; \
	mkdir -p $(INSTALL_ROOT_BIN); \
	pushd $(INSTALL_ROOT_BIN) >/dev/null; \
		rm -f $(SCRIPTS); \
	popd >/dev/null; \
	$(INSTALL) $(SCRIPTS) $(INSTALL_ROOT_BIN)
	pushd $(INSTALL_ROOT_BIN) >/dev/null; \
	chmod -f 555 $(SCRIPTS); \

	umask 002; \
	mkdir -p $(INSTALL_ROOT_LIB); \
	$(INSTALL) $(LIBRARY) $(INSTALL_ROOT_LIB)

bootstrap:

clean:
	rm -rf $(PROGRAM) $(LIBRARY) $(OBJS) \
	*~ *.o *.d* *-gcov* *.gcov \
	html config.tag

docs:
	doxygen $(EXPORT_ROOT)/etc/Doxyfile

%.d: $(PLUGIN_SRC)/%.cpp
	@set -e; rm -f $@; \
	$(CC) -MM $(CPPFLAGS) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

ifndef BOOTSTRAP
-include $(SRCS:.cpp=.d) tdriver.d btdriver.d tdriver*.d
endif
