include ../../rules.mak
#
# Copyright (C) 2009-2012 Calpont Corporation.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 2 of the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
#

#  $Id: Makefile 8436 2012-04-04 18:18:21Z rdempsey $

.PHONY: install clean test docs coverage leakcheck

PROGRAM=libddlpackage.so
VERSION=1.0.0
LIBRARY=$(PROGRAM).$(VERSION)

CPPFLAGS=-I$(EXPORT_ROOT)/include -I/usr/include/libxml2
CXXFLAGS+=$(DEBUG_FLAGS) -Wall -fpic

TLIBS=-L. -lddlpackage -L$(EXPORT_ROOT)/lib -lmessageqcpp -lloggingcpp -lconfigcpp -lxml2 -lcppunit -ldl
GLIBS=-L$(EXPORT_ROOT)/lib -lmessageqcpp -lloggingcpp -lconfigcpp -lxml2 -lcppunit -ldl

LLIBS=-L$(CALPONT_LIBRARY_PATH) -lmessageqcpp

SRCS=\
serialize.cpp \
ddl-scan.cpp \
ddl-gram.cpp \
ddlpkg.cpp \
columndef.cpp \
createtable.cpp \
tabledef.cpp \
sqlstatement.cpp \
sqlstatementlist.cpp \
altertable.cpp \
createindex.cpp \
dropindex.cpp \
droptable.cpp \
sqlparser.cpp \
markpartition.cpp \
restorepartition.cpp \
droppartition.cpp

LINCLUDES=\
ddlpkg.h \
sqlparser.h

#all: $(LIBRARY) tdriver gramtest

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

bootstrap: ddl-scan.cpp ddl-gram.cpp ddl-gram.h
	$(INSTALL) $(LINCLUDES) $(INSTALL_ROOT_INCLUDE)

ddl-scan.cpp: ddl.l
	flex -i -L -Pddl -oddl-scan-temp.cpp ddl.l
	set +e; \
	if [ -f ddl-scan.cpp ]; \
		then diff -abBq ddl-scan-temp.cpp ddl-scan.cpp >/dev/null 2>&1; \
		if [ $$? -ne 0 ]; \
			then mv -f ddl-scan-temp.cpp ddl-scan.cpp; \
			else touch ddl-scan.cpp; \
		fi; \
		else mv -f ddl-scan-temp.cpp ddl-scan.cpp; \
	fi
	rm -f ddl-scan-temp.cpp

ddl-gram.cpp ddl-gram.h: ddl.y
	bison -l -v -d -p ddl -o ddl-gram-temp.cpp ddl.y
	set +e; \
	if [ -f ddl-gram.cpp ]; \
		then diff -abBq ddl-gram-temp.cpp ddl-gram.cpp >/dev/null 2>&1; \
		if [ $$? -ne 0 ]; \
			then mv -f ddl-gram-temp.cpp ddl-gram.cpp; \
			else touch ddl-gram.cpp; \
		fi; \
		else mv -f ddl-gram-temp.cpp ddl-gram.cpp; \
	fi
	set +e; \
	if [ -f ddl-gram.h ]; \
		then diff -abBq ddl-gram-temp.hpp ddl-gram.h >/dev/null 2>&1; \
		if [ $$? -ne 0 ]; \
			then mv -f ddl-gram-temp.hpp ddl-gram.h; \
			else touch ddl-gram.h; \
		fi; \
		else mv -f ddl-gram-temp.hpp ddl-gram.h; \
	fi
	rm -f ddl-gram-temp.cpp ddl-gram-temp.hpp ddl-gram-temp.output

ddl.l: ddl-gram.h

sqlparser.cpp: ddl-gram.h

ddl-scan.o: ddl-scan.cpp ddl-gram.h
	$(COMPILE.cpp) -o $@ ddl-scan.cpp

ddl-gram.o: ddl-gram.cpp
	$(COMPILE.cpp) -o $@ ddl-gram.cpp

gramtest: gramtest.o $(LIBRARY)
	$(LINK.cpp) -L. -L$(EXPORT_ROOT)/lib -o $@ $^ -lddlpackage -lmessageqcpp -lconfigcpp -lxml2

sertest: sertest.o $(LIBRARY)
	$(LINK.cpp) -L. -L$(EXPORT_ROOT)/lib -o $@ $^ -lddlpackage -lmessageqcpp -lconfigcpp -lxml2


etags:
	etags ddlpkg.h sqlparser.h

clean:
	rm -f *.o $(PROGRAM) $(LIBRARY) TAGS tdriver core.* core *~ *.tag *-gcov.* *.gcov tdriver-gcov *.d *.d.*
	rm -rf html
	rm -f gramtest
	rm -f ddl-scan-temp.cpp
	rm -f ddl-gram-temp.cpp ddl-gram-temp.hpp ddl-gram-temp.output

docs:
	doxygen

tdriver: tdriver.o $(LIBRARY)
	$(LINK.cpp) -o $@ $^ $(TLIBS)

test:

xtest: $(LIBRARY) tdriver
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver
#	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver  > ./tdriver.output

%-gcov.o: %.cpp
	$(COMPILE.cpp) -o $@ $^

tdriver-gcov: CXXFLAGS+=-fprofile-arcs -ftest-coverage
tdriver-gcov: tdriver-gcov.o $(subst .o,-gcov.o,$(OBJS))
	$(LINK.cpp) -o $@ $^ $(GLIBS)

# In the flex/bison generated sources, there is code that we don't use
# and can't avoid generating.  So, these are omitted from coverage
# testing.

coverage:

xcoverage: tdriver-gcov
	rm -f *.gcda
	LD_LIBRARY_PATH=$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver-gcov
	for file in $(filter-out ddl-gram.cpp ddl-scan.cpp, $(SRCS)); do \
		bfile=`basename $$file .cpp`; \
		gcov -o $${bfile}-gcov $$file >/dev/null; \
	done
	/usr/local/bin/genCoverage.pl $(filter-out ddl-gram.cpp ddl-scan.cpp, $(SRCS))

leakcheck:

xleakcheck: $(LIBRARY) tdriver
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib valgrind --tool=memcheck --leak-check=yes ./tdriver

%.d: %.cpp
	@set -e; rm -f $@; \
	$(CC) -MM $(CPPFLAGS) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

ifndef BOOTSTRAP
-include $(SRCS:.cpp=.d) tdriver.d
endif
