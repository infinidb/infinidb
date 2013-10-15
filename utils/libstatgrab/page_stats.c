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
 * $Id: page_stats.c,v 1.23 2005/09/24 13:29:22 tdb Exp $
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "statgrab.h"
#include "tools.h"
#include <time.h>
#ifdef SOLARIS
#include <kstat.h>
#include <sys/sysinfo.h>
#include <string.h>
#endif
#if defined(LINUX) || defined(CYGWIN)
#include <stdio.h>
#include <string.h>
#endif
#if defined(FREEBSD) || defined(DFBSD)
#include <sys/types.h>
#include <sys/sysctl.h>
#endif
#if defined(NETBSD) || defined(OPENBSD)
#include <sys/param.h>
#include <uvm/uvm.h>
#endif
#ifdef WIN32
#include "win32.h"
#endif

static sg_page_stats page_stats;
#ifndef WIN32
static int page_stats_uninit=1;
#endif

sg_page_stats *sg_get_page_stats(){
#ifdef SOLARIS
	kstat_ctl_t *kc;
	kstat_t *ksp;
	cpu_stat_t cs;
#endif
#if defined(LINUX) || defined(CYGWIN)
	FILE *f;
	char *line_ptr;
#endif
#if defined(FREEBSD) || defined(DFBSD)
	size_t size;
#endif
#if defined(NETBSD) || defined(OPENBSD)
	struct uvmexp *uvm;
#endif

	page_stats.systime = time(NULL);
	page_stats.pages_pagein=0;
	page_stats.pages_pageout=0;

#ifdef SOLARIS
	if ((kc = kstat_open()) == NULL) {
		sg_set_error(SG_ERROR_KSTAT_OPEN, NULL);
		return NULL;
	}
	for (ksp = kc->kc_chain; ksp!=NULL; ksp = ksp->ks_next) {
		if ((strcmp(ksp->ks_module, "cpu_stat")) != 0) continue;
		if (kstat_read(kc, ksp, &cs) == -1) {
			continue;
		}

		page_stats.pages_pagein+=(long long)cs.cpu_vminfo.pgpgin;
		page_stats.pages_pageout+=(long long)cs.cpu_vminfo.pgpgout;
	}

	kstat_close(kc);
#endif
#if defined(LINUX) || defined(CYGWIN)
	if ((f = fopen("/proc/vmstat", "r")) != NULL) {
		while ((line_ptr = sg_f_read_line(f, "")) != NULL) {
			long long value;

			if (sscanf(line_ptr, "%*s %lld", &value) != 1) {
				continue;
			}

			if (strncmp(line_ptr, "pgpgin ", 7) == 0) {
				page_stats.pages_pagein = value;
			} else if (strncmp(line_ptr, "pgpgout ", 8) == 0) {
				page_stats.pages_pageout = value;
			}
		}

		fclose(f);
	} else if ((f = fopen("/proc/stat", "r")) != NULL) {
		if ((line_ptr = sg_f_read_line(f, "page")) == NULL) {
			sg_set_error(SG_ERROR_PARSE, "page");
			fclose(f);
			return NULL;
		}

		if (sscanf(line_ptr, "page %lld %lld", &page_stats.pages_pagein, &page_stats.pages_pageout) != 2) {
			sg_set_error(SG_ERROR_PARSE, "page");
			fclose(f);
			return NULL;
		}

		fclose(f);
	} else {
		sg_set_error_with_errno(SG_ERROR_OPEN, "/proc/stat");
		return NULL;
	}
#endif
#if defined(FREEBSD) || defined(DFBSD)
	size = sizeof page_stats.pages_pagein;
	if (sysctlbyname("vm.stats.vm.v_swappgsin", &page_stats.pages_pagein, &size, NULL, 0) < 0){
		sg_set_error_with_errno(SG_ERROR_SYSCTLBYNAME,
		                        "vm.stats.vm.v_swappgsin");
		return NULL;
	}
	size = sizeof page_stats.pages_pageout;
	if (sysctlbyname("vm.stats.vm.v_swappgsout", &page_stats.pages_pageout, &size, NULL, 0) < 0){
		sg_set_error_with_errno(SG_ERROR_SYSCTLBYNAME,
		                        "vm.stats.vm.v_swappgsout");
		return NULL;
	}
#endif
#if defined(NETBSD) || defined(OPENBSD)
	if ((uvm = sg_get_uvmexp()) == NULL) {
		return NULL;
	}

	page_stats.pages_pagein = uvm->pgswapin;
	page_stats.pages_pageout = uvm->pgswapout;
#endif
#ifdef WIN32
	sg_set_error(SG_ERROR_UNSUPPORTED, "Win32");
	return NULL;
#endif

	return &page_stats;
}

sg_page_stats *sg_get_page_stats_diff(){
	static sg_page_stats page_stats_diff;
#ifndef WIN32
	sg_page_stats *page_ptr;

	if(page_stats_uninit){
		page_ptr=sg_get_page_stats();
		if(page_ptr==NULL){
			return NULL;
		}
		page_stats_uninit=0;
		return page_ptr;
	}

	page_stats_diff.pages_pagein=page_stats.pages_pagein;
	page_stats_diff.pages_pageout=page_stats.pages_pageout;
	page_stats_diff.systime=page_stats.systime;

	page_ptr=sg_get_page_stats();
	if(page_ptr==NULL){
		return NULL;
	}

	page_stats_diff.pages_pagein=page_stats.pages_pagein-page_stats_diff.pages_pagein;
	page_stats_diff.pages_pageout=page_stats.pages_pageout-page_stats_diff.pages_pageout;
	page_stats_diff.systime=page_stats.systime-page_stats_diff.systime;

#else /* WIN32 */
	if(read_counter_large(SG_WIN32_PAGEIN, &page_stats_diff.pages_pagein)) {
		sg_set_error(SG_ERROR_PDHREAD, PDH_PAGEIN);
		return NULL;
	}
	if(read_counter_large(SG_WIN32_PAGEOUT, &page_stats_diff.pages_pageout)) {
		sg_set_error(SG_ERROR_PDHREAD, PDH_PAGEIN);
		return NULL;
	}
	page_stats_diff.systime = 0;
#endif /* WIN32 */

	return &page_stats_diff;
}
