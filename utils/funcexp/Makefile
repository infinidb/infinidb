#******************************************************************************************
# $Id: Makefile 3954 2013-07-08 16:30:15Z bpaul $
#
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
#*****************************************************************************************/
include ../../rules.mak
.PHONY: install clean test docs coverage leakcheck

PROGRAM=libfuncexp.so
VERSION=1.0.0
LIBRARY=$(PROGRAM).$(VERSION)

CPPFLAGS= -I. -I$(EXPORT_ROOT)/include -I/usr/include/libxml2
CXXFLAGS+=$(DEBUG_FLAGS) -Wall -fpic

CLIBS=-L$(CALPONT_LIBRARY_PATH) $(IDB_WRITE_LIBS) $(IDB_COMMON_LIBS) -lcppunit -lthreadpool -lwriteengine \
	-lcacheutils $(IDB_SNMP_LIBS) -Wl,--rpath -Wl,/usr/local/Calpont/lib
TLIBS = -L. -ldataconvert $(CLIBS) -lmulticast
GLIBS=$(CLIBS)

XLLIBS=-L$(CALPONT_LIBRARY_PATH) -lexecplan -ldataconvert

SRCS=\
	functor.cpp \
	funcexp.cpp \
	funcexpwrapper.cpp \
	func_abs.cpp \
	func_add_time.cpp \
	func_ascii.cpp \
	func_between.cpp \
	func_bitwise.cpp \
	func_case.cpp \
	func_cast.cpp \
	func_ceil.cpp \
	func_char.cpp \
	func_char_length.cpp \
	func_coalesce.cpp \
	func_concat.cpp \
	func_concat_ws.cpp \
	func_conv.cpp \
	func_crc32.cpp \
	func_date.cpp \
	func_date_add.cpp \
	func_date_format.cpp \
	func_day.cpp \
	func_dayname.cpp \
	func_dayofweek.cpp \
	func_dayofyear.cpp \
	func_div.cpp \
	func_elt.cpp \
	func_exp.cpp \
	func_extract.cpp \
	func_find_in_set.cpp \
	func_floor.cpp \
	func_from_days.cpp \
	func_from_unixtime.cpp \
	func_get_format.cpp \
	func_greatest.cpp \
	func_hex.cpp \
	func_hour.cpp \
	func_if.cpp \
	func_ifnull.cpp \
	func_in.cpp \
	func_inet_aton.cpp \
	func_inet_ntoa.cpp \
	func_insert.cpp \
	func_instr.cpp \
	func_isnull.cpp \
	func_last_day.cpp \
	func_lcase.cpp \
	func_least.cpp \
	func_left.cpp \
	func_length.cpp \
 	func_lpad.cpp \
	func_ltrim.cpp \
	func_maketime.cpp \
	func_makedate.cpp \
	func_math.cpp \
	func_md5.cpp \
	func_microsecond.cpp \
	func_minute.cpp \
	func_mod.cpp \
	func_month.cpp \
	func_monthname.cpp \
	func_nullif.cpp \
	func_pow.cpp \
	func_period_add.cpp \
	func_period_diff.cpp \
	func_quarter.cpp \
	func_rand.cpp \
	func_regexp.cpp \
	func_repeat.cpp \
	func_replace.cpp \
	func_reverse.cpp \
	func_right.cpp \
	func_round.cpp \
	func_rpad.cpp \
	func_rtrim.cpp \
	func_second.cpp \
	func_sec_to_time.cpp \
	func_sha.cpp \
	func_sign.cpp \
	func_str_to_date.cpp \
	func_strcmp.cpp \
	func_substr.cpp \
	func_substring_index.cpp \
	func_sysdate.cpp \
	func_time.cpp \
	func_time_format.cpp \
	func_time_to_sec.cpp \
	func_timediff.cpp \
	func_timestampdiff.cpp \
	func_to_days.cpp \
	func_trim.cpp \
	func_truncate.cpp \
	func_ucase.cpp \
	func_unhex.cpp \
	func_unix_timestamp.cpp \
	func_week.cpp \
	func_weekday.cpp \
	func_year.cpp \
	func_yearweek.cpp

LINCLUDES=\
	funcexp.h \
	funcexpwrapper.h \
	functor.h \
	functor_export.h \
	utils_utf8.h


OBJS=$(SRCS:.cpp=.o)

$(LIBRARY): $(OBJS)
	$(LINK.cpp) -shared -o $(LIBRARY) $(OBJS) $(LLIBS)
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
	rm -f $(OBJS) tdriver.o $(PROGRAM) $(LIBRARY) tdriver core *~ *.tag *-gcov.* *.gcov tdriver-gcov *.d
	rm -rf html

docs:
	doxygen $(EXPORT_ROOT)/etc/Doxyfile

tdriver: tdriver.o
	$(LINK.cpp) -o $@ $^ $(TLIBS)

test: $(LIBRARY) tdriver
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver

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
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver-gcov
	for file in $(SRCS); do \
		bfile=`basename $$file .cpp`; \
		gcov -o $${bfile}-gcov $$file >/dev/null; \
	done
	/usr/local/bin/genCoverage.pl $(SRCS)

PGLIBS=$(GLIBS)

%-prof.o: %.cpp
	$(COMPILE.cpp) -o $@ $^

tdriver-prof: CXXFLAGS+=-pg
tdriver-prof: tdriver-prof.o $(subst .o,-prof.o,$(OBJS))
	$(LINK.cpp) -o $@ $^ $(PGLIBS)

profile: tdriver-prof
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib ./tdriver-prof
	gprof ./tdriver-prof

leakcheck:

xleakcheck: $(LIBRARY) tdriver
	LD_LIBRARY_PATH=.:$(EXPORT_ROOT)/lib:/usr/local/lib valgrind --tool=memcheck --leak-check=yes ./tdriver --leakcheck

%.d: %.cpp
	@set -e; rm -f $@; \
	$(CC) -MM $(CPPFLAGS) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

ifndef BOOTSTRAP
-include $(SRCS:.cpp=.d) tdriver.d
endif

