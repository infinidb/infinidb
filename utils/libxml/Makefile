#******************************************************************************************
# $Id$
#
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
#*****************************************************************************************/
include ../../rules.mak
.PHONY: install clean test docs coverage leakcheck

PROGRAM=libxml2.so
VERSION=2.7.3
LIBRARY=$(PROGRAM).$(VERSION)

CPPFLAGS=-I. -I./libxml -DHAVE_CONFIG_H -DPIC -D_REENTRANT
CFLAGS+=$(DEBUG_FLAGS) -Wall -fpic

CLIBS=-L$(EXPORT_ROOT)/lib -lcppunit -ldl
TLIBS=-L. -llibxml2 $(CLIBS)
GLIBS=$(TLIBS)

SRCS=parser.c \
	xmlIO.c \
	parserInternals.c \
	globals.c \
	SAX2.c \
	SAX.c \
	tree.c \
	valid.c \
	xmlregexp.c \
	dict.c \
	nanohttp.c \
	xmlunicode.c \
	encoding.c \
	HTMLparser.c \
	entities.c \
	hash.c \
	xmlstring.c \
	nanoftp.c \
	catalog.c \
	chvalid.c \
	list.c \
	threads.c \
	uri.c \
	xpath.c \
	pattern.c \
	debugXML.c \
	relaxng.c \
	xmlschemastypes.c \
	xmlschemas.c \
	xpointer.c \
	xmlmemory.c \
	error.c \
	xmlreader.c \
	HTMLtree.c \
	xmlsave.c \
	xinclude.c

LINCLUDES=libxml/xmlmemory.h \
	libxml/parser.h \
	libxml/xmlversion.h \
	libxml/threads.h \
	libxml/globals.h \
	libxml/tree.h \
	libxml/dict.h \
	libxml/hash.h \
	libxml/valid.h \
	libxml/entities.h \
	libxml/xmlerror.h \
	libxml/xmlstring.h \
	libxml/encoding.h \
	libxml/xmlIO.h \
	libxml/xmlexports.h \
	libxml/xmlregexp.h \
	libxml/list.h \
	libxml/xmlautomata.h \
	libxml/SAX.h \
	libxml/SAX2.h \
	libxml/xlink.h

OBJS=$(SRCS:.c=.o)

$(LIBRARY): $(OBJS)
	$(LINK.c) -shared -o $(LIBRARY) $(OBJS)
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
	umask 002; \
	mkdir -p $(INSTALL_ROOT_INCLUDE)/libxml; \
	$(INSTALL) $(LINCLUDES) $(INSTALL_ROOT_INCLUDE)/libxml

clean:
	rm -f $(OBJS) tdriver*.o $(PROGRAM) $(LIBRARY) tdriver tdriver2 core *~ *.tag *-gcov.* *.gcov tdriver*-gcov *.d
	rm -rf html

docs:
	doxygen $(EXPORT_ROOT)/etc/Doxyfile

tdriver: tdriver.o
	$(LINK.c) -o $@ $^ $(TLIBS)

tdriver2: tdriver2.o
	$(LINK.c) -o $@ $^ $(TLIBS)

test:

xtest: $(LIBRARY) tdriver
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver

%-gcov.o: %.c
	$(COMPILE.c) -o $@ $^

tdriver-gcov: CXXFLAGS+=-fprofile-arcs -ftest-coverage
tdriver-gcov: tdriver-gcov.o $(subst .o,-gcov.o,$(OBJS))
	$(LINK.c) -o $@ $^ $(GLIBS)

tdriver2-gcov: CXXFLAGS+=-fprofile-arcs -ftest-coverage
tdriver2-gcov: tdriver2-gcov.o $(subst .o,-gcov.o,$(OBJS))
	$(LINK.c) -o $@ $^ $(GLIBS)

coverage:

xcoverage: tdriver-gcov tdriver2-gcov
	rm -f *.gcda
	LD_LIBRARY_PATH=$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver-gcov
	LD_LIBRARY_PATH=$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver2-gcov
	for file in $(SRCS); do \
		bfile=`basename $$file .c`; \
		gcov -o $${bfile}-gcov $$file >/dev/null; \
	done
	/usr/local/bin/genCoverage.pl $(SRCS)

leakcheck:

xleakcheck: $(LIBRARY) tdriver tdriver2
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib valgrind --tool=memcheck --leak-check=yes ./tdriver
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib valgrind --tool=memcheck --leak-check=yes ./tdriver2

%.d: %.c
	@set -e; rm -f $@; \
	$(CC) -MM $(CPPFLAGS) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

ifndef BOOTSTRAP
-include $(SRCS:.c=.d)
endif
