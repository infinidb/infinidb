/*
 * i-scream libstatgrab
 * http://www.i-scream.org
 * Copyright (C) 2000-2004 i-scream
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA
 * 02111-1307 USA
 *
 * $Id: tools.h,v 1.26 2005/09/24 13:29:23 tdb Exp $
 */

#include <stdio.h>
#if defined(LINUX) || defined(CYGWIN)
#include <regex.h>
#endif
#if defined(FREEBSD) || defined(DFBSD)
#include <kvm.h>
#endif
#ifdef NETBSD
#include <uvm/uvm_extern.h>
#endif
#ifdef HPUX
#include <sys/param.h>
#include <sys/pstat.h>
#endif

#ifdef SOLARIS
const char *sg_get_svr_from_bsd(const char *bsd);
#endif

size_t sg_strlcat(char *dst, const char *src, size_t siz);
size_t sg_strlcpy(char *dst, const char *src, size_t siz);

int sg_update_string(char **dest, const char *src);
int sg_concat_string(char **dest, const char *src);

#if defined(LINUX) || defined(CYGWIN)
long long sg_get_ll_match(char *line, regmatch_t *match);
char *sg_get_string_match(char *line, regmatch_t *match);

char *sg_f_read_line(FILE *f, const char *string);
#endif

#if (defined(FREEBSD) && !defined(FREEBSD5)) || defined(DFBSD)
kvm_t *sg_get_kvm(void);
kvm_t *sg_get_kvm2(void);
#endif

#if defined(NETBSD) || defined(OPENBSD)
struct uvmexp *sg_get_uvmexp(void);
#endif

#ifdef HPUX
struct pst_static *sg_get_pstat_static(void);
#endif

void *sg_realloc(void *ptr, size_t size);
#define sg_malloc(size) sg_realloc(NULL, size)

