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
 * $Id: os_info.c,v 1.24 2006/10/09 14:09:38 tdb Exp $
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#ifndef WIN32
#include <sys/utsname.h>
#endif
#include "statgrab.h"
#include <stdlib.h>
#ifdef SOLARIS
#include <kstat.h>
#include <time.h>
#endif
#if defined(LINUX) || defined(CYGWIN)
#include <stdio.h>
#endif
#ifdef ALLBSD
#if defined(FREEBSD) || defined(DFBSD)
#include <sys/types.h>
#include <sys/sysctl.h>
#else
#include <sys/param.h>
#include <sys/sysctl.h>
#endif
#include <time.h>
#include <sys/time.h>
#endif
#ifdef HPUX
#include <sys/param.h>
#include <sys/pstat.h>
#include <time.h>
#endif
#ifdef WIN32
#include <windows.h>
#include "win32.h"
#define WINDOWS2000 "Windows 2000"
#define WINDOWSXP "Windows XP"
#define WINDOWS2003 "Windows Server 2003"
#define BUFSIZE 12
static int runonce = 0;
#endif

#include "tools.h"

#ifdef WIN32
static int home_or_pro(const OSVERSIONINFOEX osinfo, char **name)
{
	int r;

	if (osinfo.wSuiteMask & VER_SUITE_PERSONAL) {
		r = sg_concat_string(name, " Home Edition");
	} else {
		r = sg_concat_string(name, " Professional");
	}
	return r;
}

static char *get_os_name(const OSVERSIONINFOEX osinfo)
{
	char *name;
	char tmp[10];
	int r = 0;

	/* we only compile on 2k or newer, which is version 5
	 * Covers 2000, XP and 2003 */
	if (osinfo.dwMajorVersion != 5) {
		return "Unknown";
	}
	switch(osinfo.dwMinorVersion) {
		case 0: /* Windows 2000 */
			name = strdup(WINDOWS2000);
			if(name == NULL) {
				goto out;
			}
			if (osinfo.wProductType == VER_NT_WORKSTATION) {
				r = home_or_pro(osinfo, &name);
			} else if (osinfo.wSuiteMask & VER_SUITE_DATACENTER) {
				r = sg_concat_string(&name, " Datacenter Server");
			} else if (osinfo.wSuiteMask & VER_SUITE_ENTERPRISE) {
				r = sg_concat_string(&name, " Advanced Server");
			} else {
				r = sg_concat_string(&name, " Server");
			}
			break;
		case 1: /* Windows XP */
			name = strdup(WINDOWSXP);
			if(name == NULL) {
				goto out;
			}
			r = home_or_pro(osinfo, &name);
			break;
		case 2: /* Windows 2003 */
			name = strdup(WINDOWS2003);
			if(name == NULL) {
				goto out;
			}
			if (osinfo.wSuiteMask & VER_SUITE_DATACENTER) {
				r = sg_concat_string(&name, " Datacenter Edition");
			} else if (osinfo.wSuiteMask & VER_SUITE_ENTERPRISE) {
				r = sg_concat_string(&name, " Enterprise Edition");
			} else if (osinfo.wSuiteMask & VER_SUITE_BLADE) {
				r = sg_concat_string(&name, " Web Edition");
			} else {
				r = sg_concat_string(&name, " Standard Edition");
			}
			break;
		default:
			name = strdup("Windows 2000 based");
			break;
	}
	if(r != 0) {
		free (name);
		return NULL;
	}
	/* Add on service pack version */
	if (osinfo.wServicePackMajor != 0) {
		if(osinfo.wServicePackMinor == 0) {
			if(snprintf(tmp, sizeof(tmp), " SP%d", osinfo.wServicePackMajor) != -1) {
				r = sg_concat_string(&name, tmp);
			}
		} else {
			if(snprintf(tmp, sizeof(tmp), " SP%d.%d", osinfo.wServicePackMajor,
					osinfo.wServicePackMinor) != -1) {
				r = sg_concat_string(&name, tmp);
			}
		}
		if(r) {
			free(name);
			return NULL;
		}
	}
	return name;

out:
	/* strdup failed */
	sg_set_error_with_errno(SG_ERROR_MALLOC, NULL);
	return NULL;
}
#endif

sg_host_info *sg_get_host_info()
{
	static sg_host_info general_stat;
#ifndef WIN32
	static struct utsname os;
#endif

#ifdef HPUX
	struct pst_static *pstat_static;
	time_t currtime;
	long boottime;
#endif
#ifdef SOLARIS
	time_t boottime,curtime;
	kstat_ctl_t *kc;
	kstat_t *ksp;
	kstat_named_t *kn;
#endif
#if defined(LINUX) || defined(CYGWIN)
	FILE *f;
#endif
#ifdef ALLBSD
	int mib[2];
	struct timeval boottime;
	time_t curtime;
	size_t size;
#endif
#ifdef WIN32
	unsigned long nameln;
	char *name;
	long long result;
	OSVERSIONINFOEX osinfo;
	SYSTEM_INFO sysinfo;
	char *tmp_name;
	char tmp[10];
#endif

#ifndef WIN32 /* Trust windows to be different */
	if((uname(&os)) < 0){
		sg_set_error_with_errno(SG_ERROR_UNAME, NULL);
		return NULL;
	}

	general_stat.os_name = os.sysname;
	general_stat.os_release = os.release;
	general_stat.os_version = os.version;
	general_stat.platform = os.machine;
	general_stat.hostname = os.nodename;
#else /* WIN32 */
	if (!runonce) { 
		/* these settings are static after boot, so why get them 
		 * constantly? */

		/* get system name */
		nameln = MAX_COMPUTERNAME_LENGTH + 1;
		name = sg_malloc(nameln);
		if(name == NULL) {
			return NULL;
		}
		if(GetComputerName(name, &nameln) == 0) {
			free(name);
			sg_set_error(SG_ERROR_HOST, "GetComputerName");
			return NULL;
		}
		if(sg_update_string(&general_stat.hostname, name)) {
			free(name);
			return NULL;
		}
		free(name);

		/* get OS name, version and build */
		ZeroMemory(&osinfo, sizeof(OSVERSIONINFOEX));
		osinfo.dwOSVersionInfoSize = sizeof(osinfo);
		if(!GetVersionEx(&osinfo)) {
			sg_set_error(SG_ERROR_HOST, "GetVersionEx");
			return NULL;
		}

		/* Release - single number */
		if(snprintf(tmp, sizeof(tmp), "%ld", osinfo.dwBuildNumber) == -1) {
			free(tmp);
			return NULL;
		}
		if(sg_update_string(&general_stat.os_release, tmp)) {
			free(tmp);
			return NULL;
		}

		/* Version */
		/* usually a single digit . single digit, eg 5.0 */
		if(snprintf(tmp, sizeof(tmp), "%ld.%ld", osinfo.dwMajorVersion,
					osinfo.dwMinorVersion) == -1) {
			free(tmp);
			return NULL;
		}
		if(sg_update_string(&general_stat.os_version, tmp)) {
			free(tmp);
			return NULL;
		}

		/* OS name */
		tmp_name = get_os_name(osinfo);
		if(tmp_name == NULL) {
			return NULL;
		}
		if(sg_update_string(&general_stat.os_name, tmp_name)) {
			free(tmp_name);
			return NULL;
		}
		free(tmp_name);
		runonce = 1;

		/* Platform */
		GetSystemInfo(&sysinfo);
		switch(sysinfo.wProcessorArchitecture) {
			case PROCESSOR_ARCHITECTURE_INTEL:
				if(sg_update_string(&general_stat.platform,
							"Intel")) {
					return NULL;
				}
				break;
			case PROCESSOR_ARCHITECTURE_IA64:
				if(sg_update_string(&general_stat.platform,
							"IA64")) {
					return NULL;
				}
				break;
			case PROCESSOR_ARCHITECTURE_AMD64:
				if(sg_update_string(&general_stat.platform,
							"AMD64")) {
					return NULL;
				}
				break;
			default:
				if(sg_update_string(&general_stat.platform,
							"Unknown")){
					return NULL;
				}
				break;
		}
	}
#endif /* WIN32 */

	/* get uptime */
#ifdef HPUX
	pstat_static = sg_get_pstat_static();
	if (pstat_static == NULL) {
		return NULL;
	}

	currtime = time(NULL);

	boottime = pstat_static->boot_time;

	general_stat.uptime = currtime - boottime;
#endif
#ifdef SOLARIS
	if ((kc = kstat_open()) == NULL) {
		sg_set_error(SG_ERROR_KSTAT_OPEN, NULL);
		return NULL;
	}
	if((ksp=kstat_lookup(kc, "unix", -1, "system_misc"))==NULL){
		sg_set_error(SG_ERROR_KSTAT_LOOKUP, "unix,-1,system_misc");
		kstat_close(kc);
		return NULL;
	}
	if (kstat_read(kc, ksp, 0) == -1) {
		sg_set_error(SG_ERROR_KSTAT_READ, NULL);
		kstat_close(kc);
		return NULL;
	}
	if((kn=kstat_data_lookup(ksp, "boot_time")) == NULL){
		sg_set_error(SG_ERROR_KSTAT_DATA_LOOKUP, "boot_time");
		kstat_close(kc);
		return NULL;
	}
	boottime=(kn->value.ui32);

	kstat_close(kc);

	time(&curtime);
	general_stat.uptime = curtime - boottime;
#endif
#if defined(LINUX) || defined(CYGWIN)
	if ((f=fopen("/proc/uptime", "r")) == NULL) {
		sg_set_error_with_errno(SG_ERROR_OPEN, "/proc/uptime");
		return NULL;
	}
	if((fscanf(f,"%lu %*d",&general_stat.uptime)) != 1){
		sg_set_error(SG_ERROR_PARSE, NULL);
		return NULL;
	}
	fclose(f);
#endif
#ifdef ALLBSD
	mib[0] = CTL_KERN;
	mib[1] = KERN_BOOTTIME;
	size = sizeof boottime;
	if (sysctl(mib, 2, &boottime, &size, NULL, 0) < 0){
		sg_set_error_with_errno(SG_ERROR_SYSCTL,
		                        "CTL_KERN.KERN_BOOTTIME");
		return NULL;
	}
	time(&curtime);
	general_stat.uptime=curtime-boottime.tv_sec;
#endif
#ifdef WIN32
	if(read_counter_large(SG_WIN32_UPTIME, &result)) {
		sg_set_error(SG_ERROR_PDHREAD, PDH_UPTIME);
		return NULL;
	}
	general_stat.uptime = (time_t) result;
#endif

	return &general_stat;
	
}
