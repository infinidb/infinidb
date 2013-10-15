# _AC_LIBOBJ_STRTOD
# -----------------
m4_define([_AC_LIBOBJ_STRTOD],
[AC_LIBOBJ(strtod)
AC_CHECK_FUNC(pow)
if test $ac_cv_func_pow = no; then
  AC_CHECK_LIB(m, pow,
	       [POW_LIB=-lm],
	       [AC_MSG_WARN([cannot find library containing definition of pow])])
fi
])# _AC_LIBOBJ_STRTOD


# AC_FUNC_STRTOD
# --------------
AN_FUNCTION([strtod], [AC_FUNC_STRTOD])
AC_DEFUN([AC_FUNC_STRTOD],
[AC_SUBST(POW_LIB)dnl
AC_CACHE_CHECK(for working strtod, ac_cv_func_strtod,
[AC_RUN_IFELSE(
[AC_LANG_SOURCE(
[[
#ifdef __cplusplus
extern "C" {
#endif
double strtod(const char *nptr, char **endptr);
void exit(int);
#ifdef __cplusplus
}
#endif
int
main()
{
  {
    /* Some versions of Linux strtod mis-parse strings with leading '+'.  */
    const char *string = " +69";
    char *term;
    double value;
    value = strtod (string, &term);
    if (value != 69 || term != (string + 4))
      exit (1);
  }

  {
    /* Under Solaris 2.4, strtod returns the wrong value for the
       terminating character under some conditions.  */
    const char *string = "NaN";
    char *term;
    strtod (string, &term);
    if (term != string && *(term - 1) == 0)
      exit (1);
  }
  exit (0);
}
]]
)],
	       ac_cv_func_strtod=yes,
	       ac_cv_func_strtod=no,
	       ac_cv_func_strtod=no
)])
if test $ac_cv_func_strtod = no; then
  _AC_LIBOBJ_STRTOD
fi
])


# AU::AM_FUNC_STRTOD
# ------------------
AU_ALIAS([AM_FUNC_STRTOD], [AC_FUNC_STRTOD])

# AC_FUNC_UTIME_NULL
# ------------------
AN_FUNCTION([utime], [AC_FUNC_UTIME_NULL])
AC_DEFUN([AC_FUNC_UTIME_NULL],
[AC_CACHE_CHECK(whether utime accepts a null argument, ac_cv_func_utime_null,
[# Sequent interprets utime(file, 0) to mean use start of epoch.  Wrong.
AC_RUN_IFELSE(
[AC_LANG_SOURCE(
[[
#include <sys/types.h>
#include <utime.h>
#include <sys/stat.h>
#include <unistd.h>
int
main()
{
  struct stat s, t;
  exit (!(stat ("conftest.data", &s) == 0
	  && utime ("conftest.data", (const utimbuf *)0) == 0
	  && stat ("conftest.data", &t) == 0
	  && t.st_mtime >= s.st_mtime
	  && t.st_mtime - s.st_mtime < 120));
}
]]
)],
	      ac_cv_func_utime_null=yes,
	      ac_cv_func_utime_null=no,
	      ac_cv_func_utime_null=no
)])
if test $ac_cv_func_utime_null = yes; then
  AC_DEFINE(HAVE_UTIME_NULL, 1,
	    [Define to 1 if `utime(file, NULL)' sets file's timestamp to the
	     present.])
fi
])# AC_FUNC_UTIME_NULL


# AU::AC_UTIME_NULL
# -----------------
AU_ALIAS([AC_UTIME_NULL], [AC_FUNC_UTIME_NULL])

