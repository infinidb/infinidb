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
 * $Id: memory_stats.c,v 1.35 2007/06/18 20:56:22 tdb Exp $
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "statgrab.h"
#include "tools.h"
#ifdef SOLARIS
#include <unistd.h>
#include <kstat.h>
#endif
#if defined(LINUX) || defined(CYGWIN)
#include <stdio.h>
#include <string.h>
#endif
#if defined(FREEBSD) || defined(DFBSD)
#include <sys/types.h>
#include <sys/sysctl.h>
#include <unistd.h>
#endif
#if defined(NETBSD)
#include <sys/param.h>
#include <sys/time.h>
#include <uvm/uvm.h>
#endif
#if defined(OPENBSD)
#include <sys/param.h>
#include <sys/types.h>
#include <sys/sysctl.h>
#include <sys/unistd.h>
#endif
#ifdef HPUX
#include <sys/param.h>
#include <sys/pstat.h>
#include <unistd.h>
#endif
#ifdef WIN32
#include <windows.h>
#include "win32.h"
#endif

sg_mem_stats *sg_get_mem_stats(){

	static sg_mem_stats mem_stat;

#ifdef HPUX
	struct pst_static *pstat_static;
	struct pst_dynamic pstat_dynamic;
	long long pagesize;
#endif
#ifdef SOLARIS
	kstat_ctl_t *kc;
	kstat_t *ksp;
	kstat_named_t *kn;
	long totalmem;
	int pagesize;
#endif
#if defined(LINUX) || defined(CYGWIN)
	char *line_ptr;
	unsigned long long value;
	FILE *f;
#endif
#if defined(FREEBSD) || defined(DFBSD)
	int mib[2];
	u_long physmem;
	size_t size;
	u_int free_count;
	u_int cache_count;
	u_int inactive_count;
	int pagesize;
#endif
#if defined(NETBSD)
	struct uvmexp *uvm;
#endif
#if defined(OPENBSD)
	int mib[2];
	struct vmtotal vmtotal;
	size_t size;
	int pagesize, page_multiplier;
#endif
#ifdef WIN32
	MEMORYSTATUSEX memstats;
#endif

#ifdef HPUX
	if((pagesize=sysconf(_SC_PAGESIZE)) == -1){
		sg_set_error_with_errno(SG_ERROR_SYSCONF, "_SC_PAGESIZE");
		return NULL;
	}

	if (pstat_getdynamic(&pstat_dynamic, sizeof(pstat_dynamic), 1, 0) == -1) {
		sg_set_error_with_errno(SG_ERROR_PSTAT, "pstat_dynamic");
		return NULL;
	}
	pstat_static = sg_get_pstat_static();
	if (pstat_static == NULL) {
		return NULL;
	}

	/* FIXME Does this include swap? */
	mem_stat.total = ((long long) pstat_static->physical_memory) * pagesize;
	mem_stat.free = ((long long) pstat_dynamic.psd_free) * pagesize;
	mem_stat.used = mem_stat.total - mem_stat.free;
#endif
#ifdef SOLARIS
	if((pagesize=sysconf(_SC_PAGESIZE)) == -1){
		sg_set_error_with_errno(SG_ERROR_SYSCONF, "_SC_PAGESIZE");
		return NULL;	
	}

	if((totalmem=sysconf(_SC_PHYS_PAGES)) == -1){
		sg_set_error_with_errno(SG_ERROR_SYSCONF, "_SC_PHYS_PAGES");
		return NULL;
	}

	if ((kc = kstat_open()) == NULL) {
		sg_set_error(SG_ERROR_KSTAT_OPEN, NULL);
		return NULL;
	}
	if((ksp=kstat_lookup(kc, "unix", 0, "system_pages")) == NULL){
		sg_set_error(SG_ERROR_KSTAT_LOOKUP, "unix,0,system_pages");
		return NULL;
	}
	if (kstat_read(kc, ksp, 0) == -1) {
		sg_set_error(SG_ERROR_KSTAT_READ, NULL);
		return NULL;
	}
	if((kn=kstat_data_lookup(ksp, "freemem")) == NULL){
		sg_set_error(SG_ERROR_KSTAT_DATA_LOOKUP, "freemem");
		return NULL;
	}
	kstat_close(kc);

	mem_stat.total = (long long)totalmem * (long long)pagesize;
	mem_stat.free = ((long long)kn->value.ul) * (long long)pagesize;
	mem_stat.used = mem_stat.total - mem_stat.free;
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

		if (strncmp(line_ptr, "MemTotal:", 9) == 0) {
			mem_stat.total = value;
		} else if (strncmp(line_ptr, "MemFree:", 8) == 0) {
			mem_stat.free = value;
		} else if (strncmp(line_ptr, "Cached:", 7) == 0) {
			mem_stat.cache = value;
		}
	}

	fclose(f);
	mem_stat.used = mem_stat.total - mem_stat.free;
#endif

#if defined(FREEBSD) || defined(DFBSD)
	/* Returns bytes */
	mib[0] = CTL_HW;
	mib[1] = HW_PHYSMEM;
	size = sizeof physmem;
	if (sysctl(mib, 2, &physmem, &size, NULL, 0) < 0) {
		sg_set_error_with_errno(SG_ERROR_SYSCTL, "CTL_HW.HW_PHYSMEM");
		return NULL;
	}
	mem_stat.total = physmem;

	/*returns pages*/
	size = sizeof free_count;
	if (sysctlbyname("vm.stats.vm.v_free_count", &free_count, &size, NULL, 0) < 0){
		sg_set_error_with_errno(SG_ERROR_SYSCTLBYNAME,
		                        "vm.stats.vm.v_free_count");
		return NULL;
	}

	size = sizeof inactive_count;
	if (sysctlbyname("vm.stats.vm.v_inactive_count", &inactive_count , &size, NULL, 0) < 0){
		sg_set_error_with_errno(SG_ERROR_SYSCTLBYNAME,
		                        "vm.stats.vm.v_inactive_count");
		return NULL;
	}

	size = sizeof cache_count;
	if (sysctlbyname("vm.stats.vm.v_cache_count", &cache_count, &size, NULL, 0) < 0){
		sg_set_error_with_errno(SG_ERROR_SYSCTLBYNAME,
		                        "vm.stats.vm.v_cache_count");
		return NULL;
	}

	/* Because all the vm.stats returns pages, I need to get the page size.
	 * After that I then need to multiple the anything that used vm.stats to
	 * get the system statistics by pagesize 
	 */
	pagesize = getpagesize();
	mem_stat.cache=cache_count*pagesize;

	/* Of couse nothing is ever that simple :) And I have inactive pages to
	 * deal with too. So I'm going to add them to free memory :)
	 */
	mem_stat.free=(free_count*pagesize)+(inactive_count*pagesize);
	mem_stat.used=physmem-mem_stat.free;
#endif

#if defined(NETBSD)
	if ((uvm = sg_get_uvmexp()) == NULL) {
		return NULL;
	}

	mem_stat.total = uvm->pagesize * uvm->npages;
	mem_stat.cache = uvm->pagesize * (uvm->filepages + uvm->execpages);
	mem_stat.free = uvm->pagesize * (uvm->free + uvm->inactive);
	mem_stat.used = mem_stat.total - mem_stat.free;
#endif

#if defined(OPENBSD)
	/* The code in this section is based on the code in the OpenBSD
	 * top utility, located at src/usr.bin/top/machine.c in the
	 * OpenBSD source tree.
	 *
	 * For fun, and like OpenBSD top, we will do the multiplication
	 * converting the memory stats in pages to bytes in base 2.
	 */

	/* All memory stats in OpenBSD are returned as the number of pages.
	 * To convert this into the number of bytes we need to know the
	 * page size on this system.
	 */
	pagesize = sysconf(_SC_PAGESIZE);

	/* The pagesize gives us the base 10 multiplier, so we need to work
	 * out what the base 2 multiplier is. This means dividing
	 * pagesize by 2 until we reach unity, and counting the number of
	 * divisions required.
	 */
	page_multiplier = 0;

	while (pagesize > 1) {
		page_multiplier++;
		pagesize >>= 1;
	}

	/* We can now ret the the raw VM stats (in pages) using the
	 * sysctl interface.
	 */
	mib[0] = CTL_VM;
	mib[1] = VM_METER;
	size = sizeof(vmtotal);

	if (sysctl(mib, 2, &vmtotal, &size, NULL, 0) < 0) {
		bzero(&vmtotal, sizeof(vmtotal));
		sg_set_error_with_errno(SG_ERROR_SYSCTL, "CTL_VM.VM_METER");
		return NULL;
	}

	/* Convert the raw stats to bytes, and return these to the caller
	 */
	mem_stat.used = (vmtotal.t_rm << page_multiplier);   /* total real mem in use */
	mem_stat.cache = 0;                                  /* no cache stats */
	mem_stat.free = (vmtotal.t_free << page_multiplier); /* free memory pages */
	mem_stat.total = (mem_stat.used + mem_stat.free);
#endif

#ifdef WIN32
	memstats.dwLength = sizeof(memstats);
	if (!GlobalMemoryStatusEx(&memstats)) {
		sg_set_error_with_errno(SG_ERROR_MEMSTATUS, NULL);
		return NULL;
	}
	mem_stat.free = memstats.ullAvailPhys;
	mem_stat.total = memstats.ullTotalPhys;
	mem_stat.used = mem_stat.total - mem_stat.free;
	if(read_counter_large(SG_WIN32_MEM_CACHE, &mem_stat.cache)) {
		mem_stat.cache = 0;
	}
#endif
	return &mem_stat;
}
