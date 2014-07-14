dnl @synopsis CXX_FLAGS_CHECK [compiler flags]
dnl @summary check whether compiler supports given flags or not
AC_DEFUN([CXX_FLAG_CHECK],
[dnl
	AC_MSG_CHECKING([if $CXX supports $1])
	ac_saved_cxxflags="$CXXFLAGS"
	CXXFLAGS="-Werror $1"
	AC_COMPILE_IFELSE([AC_LANG_PROGRAM([])],
		[AC_MSG_RESULT([yes])
		cxx_extra_flags="$cxx_extra_flags $1"],
		[AC_MSG_RESULT([no])]
	)
	CXXFLAGS="$ac_saved_cxxflags"
])

