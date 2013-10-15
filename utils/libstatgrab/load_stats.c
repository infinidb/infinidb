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
 * $Id: load_stats.c,v 1.19 2006/10/09 14:09:38 tdb Exp $ 
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdio.h>
#include <stdlib.h>
#include "statgrab.h"
#ifdef SOLARIS
#ifdef HAVE_SYS_LOADAVG_H
#include <sys/loadavg.h>
#else
#include <kstat.h>
#endif
#endif
#ifdef HPUX
#include <sys/param.h>
#include <sys/pstat.h>
#endif

sg_load_stats *sg_get_load_stats(){

#if !defined(CYGWIN) && !defined(WIN32)
	static sg_load_stats load_stat;

#ifdef HPUX
	struct pst_dynamic pstat_dynamic;
#else
	double loadav[3];
#endif
#endif /* not CYGWIN or WIN32 */

#ifdef CYGWIN
	sg_set_error(SG_ERROR_UNSUPPORTED, "Cygwin");
	return NULL;
#elif defined(WIN32)
	sg_set_error(SG_ERROR_UNSUPPORTED, "Win32");
	return NULL;
#else

#if defined(SOLARIS) && !defined(HAVE_SYS_LOADAVG_H)

	kstat_ctl_t *kc;	
	kstat_t *ksp;
	kstat_named_t *kn;

	if ((kc = kstat_open()) == NULL) {
		sg_set_error(SG_ERROR_KSTAT_OPEN, NULL);
		return NULL;
	}

	if((ksp=kstat_lookup(kc, "unix", 0, "system_misc")) == NULL){
		sg_set_error(SG_ERROR_KSTAT_LOOKUP, "unix,0,system_misc");
		kstat_close(kc);
		return NULL;
	}

	if (kstat_read(kc, ksp, 0) == -1) {
		sg_set_error(SG_ERROR_KSTAT_READ, NULL);
		kstat_close(kc);
		return NULL;
	}

	kstat_close(kc);

	if((kn=kstat_data_lookup(ksp, "avenrun_1min")) == NULL){
		sg_set_error(SG_ERROR_KSTAT_DATA_LOOKUP, "avenrun_1min");
		return NULL;
	}
	load_stat.min1 = (double)kn->value.ui32 / (double)256;

	if((kn=kstat_data_lookup(ksp, "avenrun_5min")) == NULL){
		sg_set_error(SG_ERROR_KSTAT_DATA_LOOKUP, "avenrun_5min");
		return NULL;
	}
	load_stat.min5 = (double)kn->value.ui32 / (double)256;

	if((kn=kstat_data_lookup(ksp, "avenrun_15min")) == NULL){
		sg_set_error(SG_ERROR_KSTAT_DATA_LOOKUP, "avenrun_15min");
		return NULL;
	}
	load_stat.min15 = (double)kn->value.ui32 / (double)256;
#elif defined(HPUX)
	if (pstat_getdynamic(&pstat_dynamic, sizeof(pstat_dynamic), 1, 0) == -1) {
		sg_set_error_with_errno(SG_ERROR_PSTAT, "pstat_dynamic");
		return NULL;
	}

	load_stat.min1=pstat_dynamic.psd_avg_1_min;
	load_stat.min5=pstat_dynamic.psd_avg_5_min;
	load_stat.min15=pstat_dynamic.psd_avg_15_min;
#else
	getloadavg(loadav,3);

	load_stat.min1=loadav[0];
	load_stat.min5=loadav[1];
	load_stat.min15=loadav[2];
#endif

	return &load_stat;
#endif
}
