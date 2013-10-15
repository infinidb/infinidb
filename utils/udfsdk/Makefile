#******************************************************************************************
# $Id$
#
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
#*****************************************************************************************/
include ../../rules.mak
.PHONY: install clean test docs coverage leakcheck
MYSQL_DIR=../../../mysql

PROGRAM1=libudf_mysql.so
VERSION1=1.0.0
LIBRARY1=$(PROGRAM1).$(VERSION1)

PROGRAM2=libudfsdk.so
VERSION2=1.0.0
LIBRARY2=$(PROGRAM2).$(VERSION2)

MYSQL_INCLUDE_DIRS=-I$(MYSQL_DIR)/include -I$(MYSQL_DIR)/sql -I$(MYSQL_DIR)/regex

CPPFLAGS=-I$(EXPORT_ROOT)/include -I/usr/include/libxml2 $(MYSQL_INCLUDE_DIRS)
-include $(HOME)/IdbMySQLConnector.mak
NEW_DEBUG_FLAGS=$(shell echo "$(DEBUG_FLAGS)" | sed s/-flto//g)
CXXFLAGS+=$(NEW_DEBUG_FLAGS) -Wall -fPIC

TLIBS=-L. -L$(EXPORT_ROOT)/lib
GLIBS=-L$(EXPORT_ROOT)/lib -lloggingcpp

SRCS1=udfmysql.cpp
SRCS2=udfinfinidb.cpp

LINCLUDES1=udfsdk.h
LINCLUDES2=

OBJS1=$(SRCS1:.cpp=.o)
OBJS2=$(SRCS2:.cpp=.o)

all: $(LIBRARY1) $(LIBRARY2)

$(LIBRARY1): $(OBJS1)
	$(LINK.cpp) -shared -o $(LIBRARY1) $(OBJS1)
	rm -f $(PROGRAM1)
	ln -s $(LIBRARY1) $(PROGRAM1)

$(LIBRARY2): $(OBJS2)
	$(MAKE) -C ../loggingcpp install
	$(LINK.cpp) -shared -o $(LIBRARY2) $(OBJS2) $(GLIBS)
	rm -f $(PROGRAM2)
	ln -s $(LIBRARY2) $(PROGRAM2)

install: bootstrap $(LIBRARY1) $(LIBRARY2)
	umask 002; \
	mkdir -p $(INSTALL_ROOT_LIB) $(INSTALL_ROOT_INCLUDE) $(INSTALL_ROOT_TOOLS); \
	pushd $(INSTALL_ROOT_LIB) >/dev/null; \
		rm -f $(LIBRARY1) $(PROGRAM1); \
		ln -s $(LIBRARY1) $(PROGRAM1); \
		rm -f $(LIBRARY2) $(PROGRAM2); \
		ln -s $(LIBRARY2) $(PROGRAM2); \
	popd >/dev/null; \
	$(INSTALL) $(LIBRARY1) $(LIBRARY2) $(INSTALL_ROOT_LIB)

bootstrap:
	$(INSTALL) $(LINCLUDES1) $(LINCLUDES2) $(INSTALL_ROOT_INCLUDE)

clean:
	rm -rf $(PROGRAM1) $(LIBRARY1) $(OBJS1) $(PROGRAM2) $(LIBRARY2) $(OBJS2) *~ *.o *.d* *-gcov* *.gcov

docs:
	doxygen $(EXPORT_ROOT)/etc/Doxyfile

%.d: %.cpp
	@set -e; rm -f $@; \
	$(CC) -MM $(CPPFLAGS) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

ifndef BOOTSTRAP
-include $(SRCS1:.cpp=.d) $(SRCS2:.cpp=.d) 
endif
