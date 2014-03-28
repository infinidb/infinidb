/* Copyright (C) 2013 Calpont Corp.

   This program is free software; you can redistribute it and/or
   modify it under the terms of the GNU General Public License
   as published by the Free Software Foundation; version 2 of
   the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. */

#ifndef GLOB_H__
#define GLOB_H__

#include <crtdefs.h>

#define       GLOB_ERR        0x0001
#define       GLOB_MARK       0x0002
#define       GLOB_NOSORT     0x0004
#define       GLOB_DOOFFS     0x0008
#define       GLOB_NOCHECK    0x0010
#define       GLOB_APPEND     0x0020
#define       GLOB_NOESCAPE   0x0040
#define       GLOB_PERIOD     0x0080
#define       GLOB_ALTDIRFUNC 0x0100
#define       GLOB_BRACE      0x0200
#define       GLOB_NOMAGIC    0x0400
#define       GLOB_TILDE      0x0800
#define       GLOB_ONLYDIR    0x1000
#define       GLOB_MAGCHAR    0x2000

#define       GLOB_NOSPACE 0x01
#define       GLOB_ABORTED 0x02
#define       GLOB_NOMATCH 0x03

typedef struct
{
	size_t gl_pathc;    /* Count of paths matched so far  */
	char **gl_pathv;    /* List of matched pathnames.  */
	size_t gl_offs;     /* Slots to reserve in gl_pathv.  */
	size_t gl_alloccnt;
} glob_t;

#ifdef __cplusplus
extern "C" {
#endif

extern int glob(const char *pattern, int flags,
	int errfunc(const char *epath, int eerrno),
	glob_t *pglob);

extern void globfree(glob_t *pglob);

#ifdef __cplusplus
}
#endif
#endif
