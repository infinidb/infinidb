# $Id$
## Process this file with automake to produce Makefile.in

AM_CPPFLAGS = $(idb_cppflags)
AM_CFLAGS = $(idb_cflags)
AM_CXXFLAGS = $(idb_cxxflags)
AM_LDFLAGS = $(idb_ldflags)
lib_LTLIBRARIES = libpgm.la
libpgm_la_SOURCES = \
	backtrace.c \
	checksum.c \
	galois_tables.c \
	gsi.c \
	if.c \
	log.c \
	packet.c \
	rate_control.c \
	reed_solomon.c \
	rxwi.c \
	signal.c \
	timer.c \
	transport.c \
	txwi.c
libpgm_la_LDFLAGS = -version-info 1:0:0 $(AM_LDFLAGS)
libpgm_la_CPPFLAGS = -I@prefix@/Calpont/include -I/usr/include/libxml2 @GLIB_CFLAGS@ \
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
	-DCONFIG_GALOIS_MUL_LUT $(AM_CPPFLAGS)

test:

coverage:

leakcheck:

docs:

bootstrap: install-data-am

