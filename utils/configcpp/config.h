// A good set of defaults for the dev compile
#ifndef CONFIGCPP_CONFIG_H__
#define CONFIGCPP_CONFIG_H__

#ifndef HAVE_CONFIG_H

#ifndef _MSC_VER

#define HAVE_ALARM 1
#define HAVE_ALLOCA 1
#define HAVE_ALLOCA_H 1
#define HAVE_ARPA_INET_H 1
#define HAVE_DECL_STRERROR_R 1
#define HAVE_DLFCN_H 1
#define HAVE_DUP2 1
#define HAVE_FCNTL_H 1
#define HAVE_FLOOR 1
#define HAVE_FORK 1
#define HAVE_FTIME 1
#define HAVE_FTRUNCATE 1
#define HAVE_GETHOSTBYNAME 1
#define HAVE_GETPAGESIZE 1
#define HAVE_GETTIMEOFDAY 1
#define HAVE_INET_NTOA 1
#define HAVE_INTTYPES_H 1
#define HAVE_ISASCII 1
#define HAVE_LIMITS_H 1
#define HAVE_LOCALTIME_R 1
#define HAVE_MALLOC 1
#define HAVE_MALLOC_H 1
#define HAVE_MBSTATE_T 1
#define HAVE_MEMCHR 1
#define HAVE_MEMMOVE 1
#define HAVE_MEMORY_H 1
#define HAVE_MEMSET 1
#define HAVE_MKDIR 1
#define HAVE_NETDB_H 1
#define HAVE_NETINET_IN_H 1
#define HAVE_POW 1
#define HAVE_PTRDIFF_T 1
#define HAVE_REGCOMP 1
#define HAVE_RMDIR 1
#define HAVE_SELECT 1
#define HAVE_SETENV 1
#define HAVE_SETLOCALE 1
#define HAVE_SOCKET 1
#define HAVE_STDBOOL_H 1
#define HAVE_STDDEF_H 1
#define HAVE_STDINT_H 1
#define HAVE_STDLIB_H 1
#define HAVE_STRCASECMP 1
#define HAVE_STRCHR 1
#define HAVE_STRCSPN 1
#define HAVE_STRDUP 1
#define HAVE_STRERROR 1
#define HAVE_STRERROR_R 1
#define HAVE_STRFTIME 1
#define HAVE_STRINGS_H 1
#define HAVE_STRING_H 1
#define HAVE_STRRCHR 1
#define HAVE_STRSPN 1
#define HAVE_STRSTR 1
#define HAVE_STRTOL 1
#define HAVE_STRTOUL 1
#define HAVE_STRTOULL 1
#define HAVE_SYSLOG_H 1
#define HAVE_SYS_FILE_H 1
#define HAVE_SYS_MOUNT_H 1
#define HAVE_SYS_SELECT_H 1
#define HAVE_SYS_SOCKET_H 1
#define HAVE_SYS_STATFS_H 1
#define HAVE_SYS_STAT_H 1
#define HAVE_SYS_TIMEB_H 1
#define HAVE_SYS_TIME_H 1
#define HAVE_SYS_TYPES_H 1
#define HAVE_SYS_WAIT_H 1
#define HAVE_UNISTD_H 1
#define HAVE_UTIME 1
#define HAVE_UTIME_H 1
#define HAVE_VALUES_H 1
#define HAVE_VFORK 1
#define HAVE_WORKING_VFORK 1
#define LSTAT_FOLLOWS_SLASHED_SYMLINK 1
//#define PACKAGE "calpont"
//#define PACKAGE_BUGREPORT "support@calpont.com"
//#define PACKAGE_NAME "Calpont"
//#define PACKAGE_STRING "Calpont 1.0.0"
//#define PACKAGE_TARNAME "calpont"
//#define PACKAGE_VERSION "1.0.0"
#define PROTOTYPES 1
#define RETSIGTYPE void
#define SELECT_TYPE_ARG1 int
#define SELECT_TYPE_ARG234 (fd_set *)
#define SELECT_TYPE_ARG5 (struct timeval *)
#define STDC_HEADERS 1
#define STRERROR_R_CHAR_P 1
#define TIME_WITH_SYS_TIME 1
#define VERSION "1.0.0"
#define __PROTOTYPES 1
#define restrict __restrict

#else // _MSC_VER
#endif

#endif //!HAVE_CONFIG_H

#endif //!CONFIGCPP_CONFIG_H__

