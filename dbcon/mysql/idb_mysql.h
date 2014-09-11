//One include file to deal with all the MySQL pollution of the
//  global namespace
//
// Don't include ANY mysql headers anywhere except here!
#ifndef IDB_MYSQL_H__
#define IDB_MYSQL_H__

#define MYSQL_SERVER 1 //needed for definition of struct THD in mysql_priv.h
#define USE_CALPONT_REGEX

#undef LOG_INFO

#ifdef _MSC_VER
#ifdef _DEBUG
#define SAFEMALLOC
#define DBUG_ON 1
#else
#define DBUG_OFF 1
#endif
#define MYSQL_DYNAMIC_PLUGIN
#define DONT_DEFINE_VOID
#endif

#include "mysql_priv.h"
#include "sql_select.h"

// Now clean up the pollution as best we can...
#undef min
#undef max
#undef UNKNOWN
#undef test
#undef pthread_mutex_init
#undef pthread_mutex_lock
#undef pthread_mutex_unlock
#undef pthread_mutex_destroy
#undef pthread_mutex_wait
#undef pthread_mutex_timedwait
#undef pthread_mutex_t
#undef pthread_cond_wait
#undef pthread_cond_timedwait
#undef pthread_mutex_trylock
#undef sleep
#undef getpid

#endif
// vim:ts=4 sw=4:

