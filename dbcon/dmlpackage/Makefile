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

PROGRAM=libdmlpackage.so
VERSION=1.0.0
LIBRARY=$(PROGRAM).$(VERSION)

CPPFLAGS=-I$(EXPORT_ROOT)/include -I/usr/include/libxml2
CXXFLAGS+=$(DEBUG_FLAGS) -Wall -fpic

TLIBS=-L. -ldmlpackage -L$(EXPORT_ROOT)/lib -lexecplan -ljoblist -lfuncexp -ldataconvert -lbrm -lrwlock -lmessageqcpp -lloggingcpp -lconfigcpp -lxml2 -lcppunit -ldl
GLIBS=-L$(EXPORT_ROOT)/lib -lmessageqcpp -lloggingcpp -lconfigcpp -lxml2 -lcppunit -ldl

LLIBS=-L$(CALPONT_LIBRARY_PATH) -lmessageqcpp -lexecplan

SRCS=\
dml-scan.cpp \
dml-gram.cpp \
calpontdmlfactory.cpp \
calpontdmlpackage.cpp \
dmlcolumn.cpp \
deletedmlpackage.cpp \
dmlobject.cpp \
insertdmlpackage.cpp \
mysqldmlstatement.cpp \
oracledmlstatement.cpp \
row.cpp \
dmltable.cpp \
updatedmlpackage.cpp \
vendordmlstatement.cpp \
commanddmlpackage.cpp \
dmlpkg.cpp \
dmlparser.cpp
LINCLUDES=\
calpontdmlfactory.h \
calpontdmlpackage.h \
dmlcolumn.h \
deletedmlpackage.h \
dmlobject.h \
dmlpackage.h \
insertdmlpackage.h \
mysqldmlstatement.h \
oracledmlstatement.h \
row.h \
dmltable.h \
updatedmlpackage.h \
vendordmlstatement.h \
commanddmlpackage.h \
dmlpkg.h \
dmlparser.h 

#all: $(LIBRARY) tdriver gramtest

OBJS=$(SRCS:.cpp=.o)

$(LIBRARY): $(OBJS)
	$(LINK.cpp) -shared -o $(LIBRARY) $(OBJS)
	rm -f $(PROGRAM)
	ln -s $(LIBRARY) $(PROGRAM)


dmlparser.cpp: dml-gram.h

dml-scan.o: dml-scan.cpp dml-gram.h
	$(COMPILE.cpp) -o $@ dml-scan.cpp

dml-gram.o: dml-gram.cpp
	$(COMPILE.cpp) -o $@ dml-gram.cpp

gramtest: gramtest.o $(LIBRARY)
	$(LINK.cpp) -L. -L$(EXPORT_ROOT)/lib -o $@ $^ -ldmlpackage -lmessageqcpp -lconfigcpp -lxml2

etags:
	etags dmlpkg.h dmlparser.h

install: bootstrap $(LIBRARY)
	umask 002; \
	mkdir -p $(INSTALL_ROOT_LIB) $(INSTALL_ROOT_INCLUDE); \
	pushd $(INSTALL_ROOT_LIB) >/dev/null; \
		rm -f $(LIBRARY) $(PROGRAM); \
		ln -s $(LIBRARY) $(PROGRAM); \
	popd >/dev/null; \
	$(INSTALL) $(LIBRARY) $(INSTALL_ROOT_LIB)

bootstrap: dml-scan.cpp dml-gram.cpp dml-gram.h
	$(INSTALL) $(LINCLUDES) $(INSTALL_ROOT_INCLUDE)

dml-scan.cpp: dml.l
	flex -i -L -Pdml -odml-scan-temp.cpp dml.l
	set +e; \
	if [ -f dml-scan.cpp ]; \
		then diff -abBq dml-scan-temp.cpp dml-scan.cpp >/dev/null 2>&1; \
		if [ $$? -ne 0 ]; \
			then mv -f dml-scan-temp.cpp dml-scan.cpp; \
			else touch dml-scan.cpp; \
		fi; \
		else mv -f dml-scan-temp.cpp dml-scan.cpp; \
	fi
	rm -f dml-scan-temp.cpp

dml-gram.cpp dml-gram.h: dml.y
	bison -l -v -d -p dml -o dml-gram-temp.cpp dml.y
	set +e; \
	if [ -f dml-gram.cpp ]; \
		then diff -abBq dml-gram-temp.cpp dml-gram.cpp >/dev/null 2>&1; \
		if [ $$? -ne 0 ]; \
			then mv -f dml-gram-temp.cpp dml-gram.cpp; \
			else touch dml-gram.cpp; \
		fi; \
		else mv -f dml-gram-temp.cpp dml-gram.cpp; \
	fi
	set +e; \
	if [ -f dml-gram.h ]; \
		then diff -abBq dml-gram-temp.hpp dml-gram.h >/dev/null 2>&1; \
		if [ $$? -ne 0 ]; \
			then mv -f dml-gram-temp.hpp dml-gram.h; \
			else touch dml-gram.h; \
		fi; \
		else mv -f dml-gram-temp.hpp dml-gram.h; \
	fi
	rm -f dml-gram-temp.cpp dml-gram-temp.hpp dml-gram-temp.output

dml.l: dml-gram.h

clean:
	rm -f $(OBJS) tdriver.o $(PROGRAM) $(LIBRARY) tdriver core *~ *.tag *-gcov.* *.gcov tdriver-gcov *.d *.d.*
	rm -rf html
	rm -f gramtest
	rm -f dml-scan-temp.cpp
	rm -f dml-gram-temp.cpp dml-gram-temp.hpp dml-gram-temp.output

docs:
	doxygen $(EXPORT_ROOT)/etc/Doxyfile

tdriver: tdriver.o
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

coverage:

xcoverage: tdriver-gcov
	rm -f *.gcda
	LD_LIBRARY_PATH=$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver-gcov
	for file in $(filter-out dml-gram.cpp dml-scan.cpp, $(SRCS)); do \
		bfile=`basename $$file .cpp`; \
		gcov -o $${bfile}-gcov $$file >/dev/null; \
	done
	/usr/local/bin/genCoverage.pl $(filter-out dml-gram.cpp dml-scan.cpp, $(SRCS))

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
