#******************************************************************************************
# $Id$
#
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
#*****************************************************************************************/
include ../../rules.mak
.PHONY: install clean test docs coverage leakcheck

PROGRAM=libpgm.so
VERSION=1.0.0
LIBRARY=$(PROGRAM).$(VERSION)

CC=gcc
GLIB_CFLAGS=$(shell pkg-config --cflags-only-I gthread-2.0)
CPPFLAGS=-I$(EXPORT_ROOT)/include -I/usr/include/libxml2 $(GLIB_CFLAGS) -I.
CFLAGS+=$(DEBUG_FLAGS) -Wall -fpic \
	-Wextra \
	-Wfloat-equal \
	-Wshadow \
	-Wunsafe-loop-optimizations \
	-Wpointer-arith \
	-Wbad-function-cast \
	-Wcast-qual \
	-Wcast-align \
	-Wwrite-strings \
	-Waggregate-return \
	-Wstrict-prototypes \
	-Wold-style-definition \
	-Wmissing-prototypes \
	-Wmissing-declarations \
	-Wmissing-noreturn \
	-Wmissing-format-attribute \
	-Wredundant-decls \
	-Wnested-externs \
	-Winline \
	-pedantic \
	-std=gnu99 \
	--param max-inline-insns-single=600 \
	-D_REENTRANT \
	-D_GNU_SOURCE \
	-D__need_IOV_MAX \
	-DCONFIG_HAVE_EPOLL \
	-DCONFIG_HAVE_RTC \
	-DCONFIG_HAVE_TSC \
	-DCONFIG_HAVE_IFR_NETMASK \
	-DCONFIG_HAVE_GETIFADDRS \
	-DCONFIG_HAVE_GETHOSTBYNAME2 \
	-DCONFIG_HAVE_GETPROTOBYNAME_R \
	-DCONFIG_HAVE_SIGHANDLER_T \
	-DCONFIG_LIBSOUP22 \
	-DCONFIG_BIND_INADDR_ANY \
	-DCONFIG_GALOIS_MUL_LUT

GLIB_LIBS=$(shell pkg-config --libs gthread-2.0)
CLIBS=-L$(EXPORT_ROOT)/lib $(GLIB_LIBS)
TLIBS=-L. -lpgm $(CLIBS)
GLIBS=$(CLIBS)

SRCS= \
	transport.c \
	gsi.c \
	if.c \
	log.c \
	backtrace.c \
	signal.c \
	reed_solomon.c \
	txwi.c \
	packet.c \
	rate_control.c \
	timer.c \
	rxwi.c \
	checksum.c \
	galois_tables.c

LINCLUDES= \
	pgm/async.h \
	pgm/backtrace.h \
	pgm/checksum.h \
	pgm/err.h \
	pgm/galois.h \
	pgm/gsi.h \
	pgm/if.h \
	pgm/log.h \
	pgm/msgv.h \
	pgm/notify.h \
	pgm/packet.h \
	pgm/pgm.h \
	pgm/rate_control.h \
	pgm/reed_solomon.h \
	pgm/rxwi.h \
	pgm/signal.h \
	pgm/sn.h \
	pgm/sockaddr.h \
	pgm/timer.h \
	pgm/transport.h \
	pgm/txwi.h \
	pgm/version.h \
	pgm/zero.h

OBJS=$(SRCS:.c=.o)

$(LIBRARY): $(OBJS)
	$(LINK.c) -shared -o $(LIBRARY) $(OBJS)
	rm -f $(PROGRAM)
	ln -s $(LIBRARY) $(PROGRAM)

install: bootstrap $(LIBRARY)
	umask 002; \
	mkdir -p $(INSTALL_ROOT_LIB) $(INSTALL_ROOT_INCLUDE)/pgm; \
	pushd $(INSTALL_ROOT_LIB) >/dev/null; \
		rm -f $(LIBRARY) $(PROGRAM); \
		ln -s $(LIBRARY) $(PROGRAM); \
	popd >/dev/null; \
	$(INSTALL) $(LIBRARY) $(INSTALL_ROOT_LIB)

bootstrap:
	umask 002; \
	mkdir -p $(INSTALL_ROOT_INCLUDE)/pgm; \
	$(INSTALL) $(LINCLUDES) $(INSTALL_ROOT_INCLUDE)/pgm

clean:
	rm -f $(OBJS) tdriver.o $(PROGRAM) $(LIBRARY) tdriver core *~ *.tag *-gcov.* *.gcov tdriver-gcov *.d pgmrecv pgmsend pgmrecv.o pgmsend.o
	rm -rf html

docs:
	doxygen $(EXPORT_ROOT)/etc/Doxyfile

pgmrecv: pgmrecv.o
	$(LINK.c) -o $@ $^ $(TLIBS)

pgmsend: pgmsend.o
	$(LINK.c) -o $@ $^ $(TLIBS)

tdriver: tdriver.o
	$(LINK.c) -o $@ $^ $(TLIBS)

test:

xtest: $(LIBRARY) tdriver
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver

%-gcov.o: %.c
	$(COMPILE.c) -o $@ $^

tdriver-gcov: CFLAGS+=-fprofile-arcs -ftest-coverage
tdriver-gcov: tdriver-gcov.o $(subst .o,-gcov.o,$(OBJS))
	$(LINK.c) -o $@ $^ $(GLIBS)

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

xleakcheck: $(LIBRARY) tdriver
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib valgrind --tool=memcheck --leak-check=yes ./tdriver

%.d: %.c
	@set -e; rm -f $@; \
	$(CC) -MM $(CPPFLAGS) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

ifndef BOOTSTRAP
-include $(SRCS:.c=.d) tdriver.d
endif
