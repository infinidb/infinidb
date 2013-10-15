#  Copyright (C) 2013 Calpont Corp.
#
#  This program is free software; you can redistribute it and/or
#  modify it under the terms of the GNU General Public License
#  as published by the Free Software Foundation; version 2 of
#  the License.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program; if not, write to the Free Software
#  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
#  MA 02110-1301, USA.
AM_CPPFLAGS = $(idb_common_includes) $(idb_cppflags)
AM_CFLAGS = $(idb_cflags)
AM_CXXFLAGS = $(idb_cxxflags)
AM_LDFLAGS = -version-info 1:0:0 $(idb_ldflags)
lib_LTLIBRARIES = libdmlpackage.la
libdmlpackage_la_SOURCES = dml-scan.cpp \
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
include_HEADERS = calpontdmlfactory.h \
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

dml-gram.h dml-gram.cpp: dml.y
	$(YACC) -l -v -d -p dml -o dml-gram-temp.cpp dml.y
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

dml-scan.cpp: dml.l
	$(LEX) -i -L -Pdml -odml-scan-temp.cpp dml.l
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

dml.l: dml-gram.h

test:

coverage:

leakcheck:

docs:

bootstrap: dml-gram.h dml-gram.cpp dml-scan.cpp install-data-am

