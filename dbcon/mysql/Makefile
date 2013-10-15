include ../../rules.mak
#******************************************************************************************
# $Id: Makefile 9744 2013-08-07 03:32:19Z bwilkinson $
#
# Copyright (C) 2009-2013 Calpont Corporation
# All rights reserved
#*****************************************************************************************/
.PHONY: install clean test docs coverage leakcheck

MYSQL_DIR=../../../mysql

PROGRAM=libcalmysql.so
VERSION=1.0.0
LIBRARY=$(PROGRAM).$(VERSION)
MYSQL_INCLUDE_DIRS=-I$(MYSQL_DIR)/include -I$(MYSQL_DIR)/sql -I$(MYSQL_DIR)/regex

CPPFLAGS+= -I. -I$(EXPORT_ROOT)/include -I/usr/include/libxml2 $(MYSQL_INCLUDE_DIRS) -DMYSQL_DYNAMIC_PLUGIN
#only for debug builds
#CPPFLAGS+= -DSAFE_MUTEX -DINFINIDB_DEBUG -DDEBUG_WALK_COND -DDEBUG_SETENV
-include $(HOME)/IdbMySQLConnector.mak
NEW_DEBUG_FLAGS=$(shell echo "$(DEBUG_FLAGS)" | sed s/-flto//g)
CXXFLAGS+=$(NEW_DEBUG_FLAGS) -Wall -fpic

# setup libraries for driver

DLIBS=-L. -ljoblist -lfuncexp -L$(CALPONT_LIBRARY_PATH) -lrt -lexecplan -lbrm
DLIBS+=-lmessageqcpp -lrwlock -lconfigcpp -lxml2 -ldmlpackageproc -ldmlpackage
DLIBS+=-lwriteengine -lwriteengineclient -lidbdatafile -lloggingcpp -ldataconvert -lcompress -lcppunit -ldl

ELIBS=-L$(CALPONT_LIBRARY_PATH) -lddlpackageproc -lddlpackage
ELIBS+=-lmessageqcpp -lrwlock -lconfigcpp -lxml2 -ldmlpackageproc -ldmlpackage -lcompress
ELIBS+=-ljoblist -lfuncexp -ludfsdk -lwriteengine -lwriteengineclient -lidbdatafile -lbrm -lloggingcpp -ldataconvert -lexecplan
ELIBS+=-ljoiner -ldl -lrowgroup -lcacheutils
ELIBS+=-lmulticast -lquerystats
ELIBS+=-lfuncexp -lsnmpmanager -lidbboot -lboost_idb -lpthread -lrt

# setup libraries for tdriver

TLIBS=-L. -ljoblist -lfuncexp -L$(EXPORT_ROOT)/lib -lrt 
TLIBS+=-lmessageqcpp -lconfigcpp -lxml2 -lcppunit -ldl -lxml2 
TLIBS+=-lexecplan -lm -lthreadpool -lloggingcpp -lrwlock -lbrm -lcompress
TLIBS+=-ldmlpackageproc -ldmlpackage -lwriteengine -lwriteengineclient -lidbdatafile -lloggingcpp -ldataconvert

GLIBS=-L$(EXPORT_ROOT)/lib -lrt -lmessageqcpp -lconfigcpp -lxml2 -lcppunit
GLIBS+=-ldl -liberty -lxml2 -ldmlpackageproc -ldmlpackage -lwriteengine -lwriteengineclient -lidbdatafile -lloggingcpp -ldataconvert

SRCS=ha_calpont.cpp ha_calpont_impl.cpp ha_calpont_dml.cpp ha_calpont_ddl.cpp ha_calpont_udf.cpp \
ha_calpont_execplan.cpp ha_scalar_sub.cpp ha_in_sub.cpp ha_exists_sub.cpp ha_from_sub.cpp ha_select_sub.cpp \
ha_view.cpp sm.cpp ha_window_function.cpp ha_calpont_partition.cpp

DRIVERS=tdriver1.cpp

LINCLUDES=idb_mysql.h

MYSQLSCRIPTS=syscatalog_mysql.sql \
dumpcat_mysql.sql \
dumpcat.pl \
calsetuserpriority.sql \
calremoveuserpriority.sql \
calshowprocesslist.sql \

OBJS=$(SRCS:.cpp=.o)

all: $(LIBRARY)

ha_calpont.o: ha_calpont.cpp
	$(COMPILE.cpp) -fno-rtti -fno-implicit-templates -o ha_calpont.o ha_calpont.cpp

$(LIBRARY): $(OBJS)
	$(MAKE) -C ../ddlpackageproc install
	$(MAKE) -C ../ddlpackage install
	$(MAKE) -C ../dmlpackageproc install
	$(MAKE) -C ../dmlpackage install
	$(MAKE) -C ../joblist install
	$(MAKE) -C ../execplan install
	$(LINK.cpp) -shared -o $(LIBRARY) $(OBJS) $(ELIBS)
	rm -f $(PROGRAM)
	ln -s $(LIBRARY) $(PROGRAM)

install: bootstrap $(LIBRARY)
	umask 002; \
	mkdir -p $(INSTALL_ROOT_LIB) $(INSTALL_ROOT_INCLUDE) $(INSTALL_ROOT_MYSQL); \
	pushd $(INSTALL_ROOT_LIB) >/dev/null; \
		rm -f $(LIBRARY) $(PROGRAM); \
		ln -s $(LIBRARY) $(PROGRAM); \
	popd >/dev/null; \
	$(INSTALL) $(LIBRARY) $(INSTALL_ROOT_LIB); \
	$(INSTALL) $(MYSQLSCRIPTS) $(INSTALL_ROOT_MYSQL); 

bootstrap:
	$(INSTALL) $(LINCLUDES) $(INSTALL_ROOT_INCLUDE)

clean:
	rm -f $(OBJS) tdriver.o $(PROGRAM) $(LIBRARY)
	rm -f core *~ *.tag *-gcov.* *.gcov *.d *.d.*
	rm -rf html
	rm -f LDL-* core.*

docs:
	doxygen $(EXPORT_ROOT)/etc/Doxyfile

test:

xtest: $(LIBRARY) tdriver
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

xleakcheck: tdriver-datalist tdriver-dec
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib valgrind --tool=memcheck --leak-check=yes ./tdriver-datalist
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib valgrind --tool=memcheck --leak-check=yes ./tdriver-dec --leakcheck
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib valgrind --tool=memcheck --leak-check=yes ./tdriver-filter

tdriver1: tdriver1.o
	$(LINK.cpp) -o tdriver1 tdriver1.o

%.d: %.cpp
	@set -e; rm -f $@; \
	$(CC) -MM $(CPPFLAGS) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

ifndef BOOTSTRAP
-include $(SRCS:.cpp=.d) $(DRIVERS:.cpp=.d)
endif

