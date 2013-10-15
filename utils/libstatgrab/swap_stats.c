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
 * $Id: swap_stats.c,v 1.25 2007/07/05 16:46:06 tdb Exp $
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "statgrab.h"
#include "tools.h"
#ifdef SOLARIS
#include <sys/stat.h>
#include <sys/swap.h>
#include <unistd.h>
#endif
#if defined(LINUX) || defined(CYGWIN)
#include <stdio.h>
#include <string.h>
#endif
#if defined(FREEBSD) || defined(DFBSD)
#ifdef FREEBSD5
#include <sys/param.h>
#include <sys/sysctl.h>
#include <sys/user.h>
#else
#include <sys/types.h>
#include <kvm.h>
#endif
#include <unistd.h>
#endif
#if defined(NETBSD) || defined(OPENBSD)
#include <sys/param.h>
#include <sys/time.h>
#include <uvm/uvm.h>
#include <unistd.h>
#endif
#ifdef HPUX
#include <sys/param.h>
#include <sys/pstat.h>
#include <unistd.h>
#define SWAP_BATCH 5
#endif
#ifdef WIN32
#include <windows.h>
#endif

sg_swap_stats *sg_get_swap_stats(){

	static sg_swap_stats swap_stat;

#ifdef HPUX
	struct pst_swapinfo pstat_swapinfo[SWAP_BATCH];
	int swapidx = 0;
	int num, i;
#endif
#ifdef SOLARIS
	struct anoninfo ai;
	int pagesize;
#endif
#if defined(LINUX) || defined(CYGWIN)
	FILE *f;
	char *line_ptr;
	unsigned long long value;
#endif
#if defined(FREEBSD) || defined(DFBSD)
	int pagesize;
#ifdef FREEBSD5
	struct xswdev xsw;
	int mib[16], n;
	size_t mibsize, size;
#else
	struct kvm_swap swapinfo;
	kvm_t *kvmd;
#endif
#endif
#if defined(NETBSD) || defined(OPENBSD)
	struct uvmexp *uvm;
#endif
#ifdef WIN32
	MEMORYSTATUSEX memstats;
#endif

#ifdef HPUX
	swap_stat.total = 0;
	swap_stat.used = 0;
	swap_stat.free = 0;

	while (1) {
		num = pstat_getswap(pstat_swapinfo, sizeof pstat_swapinfo[0],
		                    SWAP_BATCH, swapidx);
		if (num == -1) {
			sg_set_error_with_errno(SG_ERROR_PSTAT,
			                        "pstat_getswap");
			return NULL;
		} else if (num == 0) {
			break;
		}

		for (i = 0; i < num; i++) {
			struct pst_swapinfo *si = &pstat_swapinfo[i];

			if ((si->pss_flags & SW_ENABLED) != SW_ENABLED) {
				continue;
			}
	
			if ((si->pss_flags & SW_BLOCK) == SW_BLOCK) {
				swap_stat.total += ((long long) si->pss_nblksavail) * 1024LL;
				swap_stat.used += ((long long) si->pss_nfpgs) * 1024LL;
				swap_stat.free = swap_stat.total - swap_stat.used;
			}
			if ((si->pss_flags & SW_FS) == SW_FS) {
				swap_stat.total += ((long long) si->pss_limit) * 1024LL;
				swap_stat.used += ((long long) si->pss_allocated) * 1024LL;
				swap_stat.free = swap_stat.total - swap_stat.used;
			}
		}
		swapidx = pstat_swapinfo[num - 1].pss_idx + 1;
	}
#endif
#ifdef SOLARIS
	if((pagesize=sysconf(_SC_PAGESIZE)) == -1){
		sg_set_error_with_errno(SG_ERROR_SYSCONF, "_SC_PAGESIZE");
		return NULL;
	}
	if (swapctl(SC_AINFO, &ai) == -1) {
		sg_set_error_with_errno(SG_ERROR_SWAPCTL, NULL);
		return NULL;
	}
	swap_stat.total = (long long)ai.ani_max * (long long)pagesize;
	swap_stat.used = (long long)ai.ani_resv * (long long)pagesize;
	swap_stat.free = swap_stat.total - swap_stat.used;
#endif
#if defined(LINUX) || defined(CYGWIN)
	if ((f = fopen("/proc/meminfo", "r")) == NULL) {
		sg_set_error_with_errno(SG_ERROR_OPEN, "/proc/meminfo");
		return NULL;
	}

	while ((line_ptr = sg_f_read_line(f, "")) != NULL) {
		if (sscanf(line_ptr, "%*s %llu kB", &value) != 1) {
			continue;
		}
		value *= 1024;

		if (strncmp(line_ptr, "SwapTotal:", 10) == 0) {
			swap_stat.total = value;
		} else if (strncmp(line_ptr, "SwapFree:", 9) == 0) {
			swap_stat.free = value;
		}
	}

	fclose(f);
	swap_stat.used = swap_stat.total - swap_stat.free;
#endif
#if defined(FREEBSD) || defined(DFBSD)
	pagesize=getpagesize();

#ifdef FREEBSD5
	swap_stat.total = 0;
	swap_stat.used = 0;

	mibsize = sizeof mib / sizeof mib[0];
	if (sysctlnametomib("vm.swap_info", mib, &mibsize) < 0) {
		sg_set_error_with_errno(SG_ERROR_SYSCTLNAMETOMIB,
		                        "vm.swap_info");
		return NULL;
	}
	for (n = 0; ; ++n) {
		mib[mibsize] = n;
		size = sizeof xsw;
		if (sysctl(mib, mibsize + 1, &xsw, &size, NULL, 0) < 0) {
			break;
		}
		if (xsw.xsw_version != XSWDEV_VERSION) {
			sg_set_error(SG_ERROR_XSW_VER_MISMATCH, NULL);
			return NULL;
		}
		swap_stat.total += (long long) xsw.xsw_nblks;
		swap_stat.used += (long long) xsw.xsw_used;
	}
	if (errno != ENOENT) {
		sg_set_error_with_errno(SG_ERROR_SYSCTL, "vm.swap_info");
		return NULL;
	}
#else
	if((kvmd = sg_get_kvm()) == NULL){
		return NULL;
	}
	if ((kvm_getswapinfo(kvmd, &swapinfo, 1,0)) == -1){
		sg_set_error(SG_ERROR_KVM_GETSWAPINFO, NULL);
		return NULL;
	}

	swap_stat.total = (long long)swapinfo.ksw_total;
	swap_stat.used = (long long)swapinfo.ksw_used;
#endif
	swap_stat.total *= pagesize;
	swap_stat.used *= pagesize;
	swap_stat.free = swap_stat.total - swap_stat.used;
#endif
#if defined(NETBSD) || defined(OPENBSD)
	if ((uvm = sg_get_uvmexp()) == NULL) {
		return NULL;
	}

	swap_stat.total = (long long)uvm->pagesize * (long long)uvm->swpages;
	swap_stat.used = (long long)uvm->pagesize * (long long)uvm->swpginuse;
	swap_stat.free = swap_stat.total - swap_stat.used;
#endif
#ifdef WIN32
	memstats.dwLength = sizeof(memstats);
	if (!GlobalMemoryStatusEx(&memstats)) {
		sg_set_error_with_errno(SG_ERROR_MEMSTATUS,
			"GloblaMemoryStatusEx");
		return NULL;
	}
	/* the PageFile stats include Phys memory "minus an overhead".
	 * Due to this unknown "overhead" there's no way to extract just page
	 * file use from these numbers */
	swap_stat.total = memstats.ullTotalPageFile;
	swap_stat.free = memstats.ullAvailPageFile;
	swap_stat.used = swap_stat.total - swap_stat.free;
#endif

	return &swap_stat;

}
