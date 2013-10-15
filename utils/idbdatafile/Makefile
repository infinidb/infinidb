#******************************************************************************************
# $Id: Makefile 3048 2012-04-04 15:33:45Z rdempsey $
#
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
#*****************************************************************************************/
include ../../rules.mak
.PHONY: install clean test docs coverage leakcheck

PROGRAM=libidbdatafile.so
VERSION=1.0.0
LIBRARY=$(PROGRAM).$(VERSION)

HAD_INCLUDES=
HAD_LDPATH=

CPPFLAGS=-I$(EXPORT_ROOT)/include $(HAD_INCLUDES) -I/usr/include/libxml2
CXXFLAGS+=$(DEBUG_FLAGS) -Wall -fPIC
CFLAGS+=$(CXXFLAGS)

# Run-time directories for project shared libs
CALPONT_LIBRARY_PATH=$(EXPORT_ROOT)/lib
LDFLAGS+=-L$(CALPONT_LIBRARY_PATH)

LLIBS=$(CXXFLAGS) -L$(CALPONT_LIBRARY_PATH) -lloggingcpp
#TLIBS+=-L$(CALPONT_LIBRARY_PATH)  $(IDB_WRITE_LIBS) $(IDB_SNMP_LIBS) -lloggingcpp -lboost_idb
TLIBS+=-L$(CALPONT_LIBRARY_PATH) $(IDB_COMMON_LIBS)  $(IDB_SNMP_LIBS) -lidbdatafile -lpthread -lbrm -lrwlock -lmessageqcpp -ldl
GLIBS=-L$(EXPORT_ROOT)/lib

CXXSRCS= \
	IDBDataFile.cpp		\
	UnbufferedFile.cpp	\
	BufferedFile.cpp	\
	IDBFileSystem.cpp	\
	PosixFileSystem.cpp	\
	IDBPolicy.cpp		\
	IDBLogger.cpp		\
	IDBFactory.cpp

CSRCS=

LINCLUDES= \
	IDBDataFile.h		\
	IDBFileSystem.h		\
	IDBPolicy.h		\
	IDBLogger.h		\
	IDBFactory.h		\
	utility.h    		\
	FileFactoryBase.h	\
	BufferedFile.h       \
	largefile64.h

CXXOBJS=$(CXXSRCS:.cpp=.o)
COBJS=$(CSRCS:.c=.o)

$(LIBRARY): $(CXXOBJS) $(COBJS)
	$(LINK.cpp) -shared -o $(LIBRARY) $(CXXOBJS) $(COBJS) $(HAD_LDPATH) $(LLIBS)
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

tdriver: tdriver.o
	$(LINK.cpp) -o $@ $^ $(TLIBS) -Wl,--rpath -Wl,/usr/local/Calpont/lib

tdriver1: tdriver1.o
	$(LINK.cpp) -o $@ $^ -Wl,-rpath -Wl,/home/rdempsey/genii/export/lib \
		-L/home/rdempsey/genii/utils/idbdatafile \
		-lidbdatafile -lloggingcpp -lconfigcpp \
		-lmessageqcpp -lboost_idb -lcompress -lidbboot -lbrm -lrwlock -lexecplan -lcacheutils -ljoblist \
		-loamcpp -lfuncexp -lrowgroup -ljoiner -lcommon -lwindowfunction -ldataconvert -lmysqlcl_idb \
		-lmulticast -lquerystats -lsnmpmanager -ludfsdk -lxml2

test: $(LIBRARY) tdriver
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib ./tdriver

%.d: %.cpp
	@set -e; rm -f $@; \
	$(CC) -MM $(CPPFLAGS) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

ifndef BOOTSTRAP
-include $(CXXSRCS:.cpp=.d) $(CSRCS:.c=.d)
endif
