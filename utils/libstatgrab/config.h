#if defined(HAVE_CONFIG_H) && !defined(USE_THIS_CONFIG_H)
#include "../../config.h"
#else
#define HAVE_DLFCN_H 1
#define HAVE_INTTYPES_H 1
#define HAVE_MEMORY_H 1
#define HAVE_STDINT_H 1
#define HAVE_STDLIB_H 1
#define HAVE_STRINGS_H 1
#define HAVE_STRING_H 1
#define HAVE_SYS_STAT_H 1
#define HAVE_SYS_TYPES_H 1
#define HAVE_UNISTD_H 1
#define STDC_HEADERS 1
#endif

#define HAVE_ATOLL 1
#define HAVE_MALLOC 1
#define HAVE_SETEGID 1
#define HAVE_SETEUID 1
#define HAVE_SETRESGID 1
#define HAVE_SETRESUID 1

/* Building on GNU/Linux */
#define LINUX 

/* Name of package */
#ifdef PACKAGE
#undef PACKAGE
#endif
#define PACKAGE "libstatgrab"

/* Define to the address where bug reports for this package should be sent. */
#ifdef PACKAGE_BUGREPORT
#undef PACKAGE_BUGREPORT
#endif
#define PACKAGE_BUGREPORT "bugs@i-scream.org"

/* Define to the full name of this package. */
#ifdef PACKAGE_NAME
#undef PACKAGE_NAME
#endif
#define PACKAGE_NAME "libstatgrab"

/* Define to the full name and version of this package. */
#ifdef PACKAGE_STRING
#undef PACKAGE_STRING
#endif
#define PACKAGE_STRING "libstatgrab 0.13"

/* Define to the one symbol short name of this package. */
#ifdef PACKAGE_TARNAME
#undef PACKAGE_TARNAME
#endif
#define PACKAGE_TARNAME "libstatgrab"

/* Define to the version of this package. */
#ifdef PACKAGE_VERSION
#undef PACKAGE_VERSION
#endif
#define PACKAGE_VERSION "0.13"

/* Version number of package */
#ifdef VERSION
#undef VERSION
#endif
#define VERSION "0.13"

