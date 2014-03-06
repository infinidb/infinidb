# $Id$
#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.59)
AC_INIT(Calpont, 1.0.0, support@calpont.com)
### we don't want the deault CXXFLAGS set by AC_INIT, but we do want to allow the user to set them
SAVE_CXXFLAGS=
if test -n "$CXXFLAGS"; then
	SAVE_CXXFLAGS="$CXXFLAGS"
fi
CXXFLAGS=
SAVE_CFLAGS=
if test -n "$CFLAGS"; then
	SAVE_CFLAGS="$CFLAGS"
fi
CFLAGS=
## AC_CONFIG_SRCDIR([configcpp/configcpp.cpp])
AM_INIT_AUTOMAKE
AC_CONFIG_HEADER([config.h])

AC_PREFIX_DEFAULT([/usr/local])

AC_LANG_CPLUSPLUS

# Checks for programs.
AC_PROG_CC
if test -n "$SAVE_CFLAGS"; then
	CFLAGS="$SAVE_CFLAGS"
else
	CFLAGS=
fi
AC_PROG_CXX
if test -n "$SAVE_CXXFLAGS"; then
	CXXFLAGS="$SAVE_CXXFLAGS"
else
	CXXFLAGS=
fi
AC_ENABLE_STATIC([no])
AC_PROG_LIBTOOL
AC_PROG_INSTALL
AC_PROG_LEX
if test "x$LEX" = "x:"; then
	AC_MSG_ERROR([Could not find a usable lex program!])
fi
AC_PROG_YACC
$YACC --version >/dev/null 2>&1
if test $? -ne 0; then
	AC_MSG_ERROR([Could not find a usable yacc program!])
fi
AC_PROG_AWK
AC_PROG_CPP
AC_PROG_LN_S
AC_PROG_MAKE_SET

# Checks for header files.
AC_FUNC_ALLOCA
AC_HEADER_STDC
AC_HEADER_SYS_WAIT
AC_CHECK_HEADERS([arpa/inet.h fcntl.h inttypes.h limits.h malloc.h netdb.h netinet/in.h stddef.h stdint.h stdlib.h string.h strings.h sys/file.h sys/mount.h sys/socket.h sys/statfs.h sys/time.h sys/timeb.h syslog.h unistd.h utime.h values.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_HEADER_STAT
AC_HEADER_STDBOOL
AC_C_CONST
AC_C_INLINE
AC_TYPE_MODE_T
AC_TYPE_OFF_T
AC_TYPE_PID_T
AC_C_RESTRICT
AC_TYPE_SIZE_T
AC_HEADER_TIME
AC_STRUCT_TM
AC_C_VOLATILE
AC_CHECK_TYPES([ptrdiff_t])

# Checks for library functions.
AC_FUNC_ERROR_AT_LINE
AC_REPLACE_FNMATCH
AC_FUNC_FORK
AC_FUNC_MALLOC
AC_FUNC_MEMCMP
AC_FUNC_MKTIME
AC_FUNC_SELECT_ARGTYPES
AC_FUNC_SETVBUF_REVERSED
AC_TYPE_SIGNAL
AC_FUNC_STAT
AC_FUNC_STRERROR_R
AC_FUNC_STRFTIME
AC_FUNC_STRTOD
AC_FUNC_UTIME_NULL
AC_CHECK_FUNCS([alarm dup2 floor ftime ftruncate gethostbyname getpagesize gettimeofday inet_ntoa isascii localtime_r memchr memmove memset mkdir pow regcomp rmdir select setenv setlocale socket strcasecmp strchr strcspn strdup strerror strrchr strspn strstr strtol strtoul strtoull utime])

AM_PATH_XML2([2.6.26])
if test "x$XML_CPPFLAGS" = "x"; then
	AC_MSG_ERROR([Could not find a usable libxml2 development environment!])
fi

AC_CHECK_HEADERS([zlib.h])
if test "x$ac_cv_header_zlib_h" != "xyes"; then
	AC_MSG_ERROR([Could not find a usable zlib development environment!])
fi

AC_CHECK_HEADERS([readline/readline.h])
if test "x$ac_cv_header_readline_readline_h" != "xyes"; then
	AC_MSG_ERROR([Could not find a usable readline development environment!])
fi

AC_CHECK_HEADERS([ncurses.h])
if test "x$ac_cv_header_ncurses_h" != "xyes"; then
	AC_MSG_ERROR([Could not find a usable ncurses development environment!])
fi

AC_MSG_CHECKING(whether to enable debugging)
AC_SUBST([idb_cppflags], [' '])
AC_ARG_WITH([debug],
	AS_HELP_STRING([--with-debug],[include debugging symbols/no optimization]))
if test "x$with_debug" = "xyes"; then
	AC_SUBST([idb_cxxflags],['-ggdb3 -Wall -DSAFE_MUTEX -DINFINIDB_DEBUG'])
	AC_SUBST([idb_cflags],['-ggdb3 -Wall -DSAFE_MUTEX -DINFINIDB_DEBUG'])
	AC_MSG_RESULT(yes)
else
	AC_SUBST([idb_cxxflags],['-g0 -O3 -fno-strict-aliasing -Wall'])
	AC_SUBST([idb_cflags],['-g0 -O3 -fno-strict-aliasing -Wall'])
	AC_MSG_RESULT(no)
fi
AC_SUBST([idb_ldflags],['-Wl,--rpath -Wl,/usr/local/Calpont/lib -Wl,--no-as-needed -Wl,--add-needed'])

AC_MSG_CHECKING(for CPU architecture)
uname=`uname -m`
AS_IF(
   [test `expr $uname : 'i.86'` -gt 0], [AC_SUBST([march_flags], ['-march=pentium4'])] [AC_MSG_RESULT(pentium4)],
   [AC_SUBST([march_flags], [' '])] [AC_MSG_RESULT(x86_64)]
)

AC_SUBST([libdir], ['${prefix}/Calpont/lib'])
AC_SUBST([bindir], ['${prefix}/Calpont/bin'])
AC_SUBST([includedir], ['${prefix}/Calpont/include'])
AC_SUBST([etcdir], ['${prefix}/Calpont/etc'])
AC_SUBST([sysconfdir], ['${prefix}/Calpont/etc'])
AC_SUBST([mandir], ['${prefix}/Calpont/man'])
AC_SUBST([sbindir], ['${prefix}/Calpont/sbin'])
AC_SUBST([sharedir], ['${prefix}/Calpont/share'])
AC_SUBST([postdir], ['${prefix}/Calpont/post'])
AC_SUBST([localdir], ['${prefix}/Calpont/local'])
AC_SUBST([mysqldir], ['${prefix}/Calpont/mysql'])
AC_SUBST([mibdir], ['${prefix}/Calpont/share/snmp/mibs'])
AC_SUBST([netsnmpdir], ['${prefix}/Calpont/include/net-snmp'])
AC_SUBST([netsnmpsysdir], ['${prefix}/Calpont/include/net-snmp/system'])
AC_SUBST([netsnmpmachdir], ['${prefix}/Calpont/include/net-snmp/machine'])
AC_SUBST([netsnmplibrdir], ['${prefix}/Calpont/include/net-snmp/library'])
AC_SUBST([netsnmpagntdir], ['${prefix}/Calpont/include/net-snmp/agent'])
AC_SUBST([toolsdir], ['${prefix}/Calpont/tools'])

AC_SUBST([netsnmp_libs], ['-lnetsnmpmibs -lnetsnmpagent -lnetsnmp -lnetsnmpmibs -lnetsnmphelpers'])

AC_SUBST([idb_common_libs], ['-lmessageqcpp -lloggingcpp -lconfigcpp -lidbboot -lboost_idb -lxml2 -lpthread -lrt'])
AC_SUBST([idb_oam_libs], ['-loamcpp -lsnmpmanager ${netsnmp_libs}'])
AC_SUBST([idb_brm_libs], ['-lbrm -lidbdatafile -lcacheutils -lrwlock ${idb_oam_libs} ${idb_common_libs}'])
AC_SUBST([idb_exec_libs], ['-ljoblist -lexecplan -lwindowfunction -ljoiner -lrowgroup -lfuncexp -ludfsdk -ldataconvert -lcommon -lcompress -lmysqlcl_idb -lquerystats -lquerytele -lthrift -lthreadpool ${idb_brm_libs}'])
AC_SUBST([idb_write_libs], ['-lddlpackageproc -lddlpackage -ldmlpackageproc -ldmlpackage -lwriteengine -lwriteengineclient -lidbdatafile -lcacheutils ${idb_exec_libs}'])
AC_SUBST([idb_common_includes], ['-I${prefix}/Calpont/include -I/usr/local/include -I/usr/local/include/libxml2 -I/usr/include/libxml2'])
AC_SUBST([idb_common_ldflags], ['-L${prefix}/Calpont/lib -L/usr/local/lib'])

AC_CONFIG_FILES([
	Makefile
	utils/Makefile
	utils/boost_idb/Makefile
	utils/startup/Makefile
	utils/common/Makefile
	utils/configcpp/Makefile
	utils/loggingcpp/Makefile
	utils/messageqcpp/Makefile
	utils/threadpool/Makefile
	utils/rwlock/Makefile
	utils/dataconvert/Makefile
	utils/joiner/Makefile
	utils/rowgroup/Makefile
	utils/cacheutils/Makefile
	utils/net-snmp/Makefile
	utils/funcexp/Makefile
	utils/udfsdk/Makefile
	utils/compress/Makefile
	utils/ddlcleanup/Makefile
	utils/batchloader/Makefile
	utils/mysqlcl_idb/Makefile
	utils/querystats/Makefile
	utils/jemalloc/Makefile
	utils/windowfunction/Makefile
	utils/idbdatafile/Makefile
	utils/idbhdfs/Makefile
	utils/idbhdfs/hdfs-12/Makefile
	utils/idbhdfs/hdfs-20/Makefile
	utils/winport/Makefile
	utils/thrift/Makefile
	utils/querytele/Makefile
	exemgr/Makefile
	ddlproc/Makefile
	dbcon/Makefile
	dbcon/ddlpackage/Makefile
	dbcon/ddlpackageproc/Makefile
	dbcon/dmlpackage/Makefile
	dbcon/dmlpackageproc/Makefile
	dbcon/execplan/Makefile
	dbcon/joblist/Makefile
	dbcon/mysql/Makefile
	dmlproc/Makefile
	oam/Makefile
	oam/etc/Makefile
	oam/install_scripts/Makefile
	oam/oamcpp/Makefile
	oam/post/Makefile
	oam/cloud/Makefile
	oamapps/Makefile
	oamapps/calpontConsole/Makefile
	oamapps/calpontDB/Makefile
	oamapps/postConfigure/Makefile
	oamapps/serverMonitor/Makefile
	oamapps/sessionWalker/Makefile
	oamapps/traphandler/Makefile
	oamapps/sendtrap/Makefile
	primitives/Makefile
	primitives/blockcache/Makefile
	primitives/linux-port/Makefile
	primitives/primproc/Makefile
	decomsvr/Makefile
	procmgr/Makefile
	procmon/Makefile
	snmpd/Makefile
	snmpd/etc/Makefile
	snmpd/snmpmanager/Makefile
	tools/Makefile
	tools/editem/Makefile
	tools/cplogger/Makefile
	tools/clearShm/Makefile
	tools/setConfig/Makefile
	tools/getConfig/Makefile
	tools/dbbuilder/Makefile
	tools/dbloadxml/Makefile
	tools/configMgt/Makefile
	tools/viewtablelock/Makefile
	tools/cleartablelock/Makefile
	tools/ddlcleanup/Makefile
	tools/idbmeminfo/Makefile
	versioning/Makefile
	versioning/BRM/Makefile
	writeengine/Makefile
	writeengine/shared/Makefile
	writeengine/index/Makefile
	writeengine/dictionary/Makefile
	writeengine/wrapper/Makefile
	writeengine/xml/Makefile
	writeengine/bulk/Makefile
	writeengine/client/Makefile
	writeengine/splitter/Makefile
	writeengine/server/Makefile
	writeengine/cpimport/Makefile
	writeengine/redistribute/Makefile
	net-snmp/Makefile
])
AC_OUTPUT
