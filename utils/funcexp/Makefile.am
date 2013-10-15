# Copyright (C) 2013 Calpont Corp.
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; version 2 of
# the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
# MA 02110-1301, USA.

# $Id$
## Process this file with automake to produce Makefile.in

AM_CPPFLAGS = $(idb_common_includes) $(idb_cppflags)
AM_CFLAGS = $(idb_cflags)
AM_CXXFLAGS = $(idb_cxxflags)
AM_LDFLAGS = -version-info 1:0:0 $(idb_ldflags)
lib_LTLIBRARIES = libfuncexp.la
libfuncexp_la_SOURCES = \
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
	func_makedate.cpp \
	func_maketime.cpp \
	func_math.cpp \
	func_microsecond.cpp \
	func_md5.cpp \
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


include_HEADERS = \
	funcexp.h \
	funcexpwrapper.h \
	functor.h \
	functor_export.h \
	utils_utf8.h

test:

coverage:

leakcheck:

docs:

bootstrap: install-data-am

