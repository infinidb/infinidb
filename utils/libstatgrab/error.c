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
 * $Id: error.c,v 1.17 2005/09/24 13:29:22 tdb Exp $
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdlib.h>
#include <string.h>
#include <errno.h>

#include "statgrab.h"
#include "tools.h"

static sg_error error = SG_ERROR_NONE;
#define ERROR_ARG_MAX 256
static char error_arg[ERROR_ARG_MAX];
static int errno_value = 0;

void sg_set_error(sg_error code, const char *arg) {
	errno_value = 0;
	error = code;
	if (arg != NULL) {
		sg_strlcpy(error_arg, arg, sizeof error_arg);
	}
	else {
		/* FIXME is this the best idea? */
		error_arg[0] = '\0';
	}
}

void sg_set_error_with_errno(sg_error code, const char *arg) {
	sg_set_error(code, arg);
	errno_value = errno;
}

sg_error sg_get_error() {
	return error;
}

const char *sg_get_error_arg() {
	return error_arg;
}

int sg_get_error_errno() {
	return errno_value;
}

const char *sg_str_error(sg_error code) {
	switch (code) {
	case SG_ERROR_NONE:
		return "no error";
	case SG_ERROR_ASPRINTF:
		return "asprintf failed";
	case SG_ERROR_DEVSTAT_GETDEVS:
		return "devstat_getdevs failed";
	case SG_ERROR_DEVSTAT_SELECTDEVS:
		return "devstat_selectdevs failed";
	case SG_ERROR_ENOENT:
		return "system call received ENOENT";
	case SG_ERROR_GETIFADDRS:
		return "getifaddress failed";
	case SG_ERROR_GETMNTINFO:
		return "getmntinfo failed";
	case SG_ERROR_GETPAGESIZE:
		return "getpagesize failed";
	case SG_ERROR_HOST:
		return "gather host information faile";
	case SG_ERROR_KSTAT_DATA_LOOKUP:
		return "kstat_data_lookup failed";
	case SG_ERROR_KSTAT_LOOKUP:
		return "kstat_lookup failed";
	case SG_ERROR_KSTAT_OPEN:
		return "kstat_open failed";
	case SG_ERROR_KSTAT_READ:
		return "kstat_read failed";
	case SG_ERROR_KVM_GETSWAPINFO:
		return "kvm_getswapinfo failed";
	case SG_ERROR_KVM_OPENFILES:
		return "kvm_openfiles failed";
	case SG_ERROR_MALLOC:
		return "malloc failed";
	case SG_ERROR_OPEN:
		return "failed to open file";
	case SG_ERROR_OPENDIR:
		return "failed to open directory";
	case SG_ERROR_PARSE:
		return "failed to parse input";
	case SG_ERROR_SETEGID:
		return "setegid failed";
	case SG_ERROR_SETEUID:
		return "seteuid failed";
	case SG_ERROR_SETMNTENT:
		return "setmntent failed";
	case SG_ERROR_SOCKET:
		return "socket failed";
	case SG_ERROR_SWAPCTL:
		return "swapctl failed";
	case SG_ERROR_SYSCONF:
		return "sysconf failed";
	case SG_ERROR_SYSCTL:
		return "sysctl failed";
	case SG_ERROR_SYSCTLBYNAME:
		return "sysctlbyname failed";
	case SG_ERROR_SYSCTLNAMETOMIB:
		return "sysctlnametomib failed";
	case SG_ERROR_UNAME:
		return "uname failed";
	case SG_ERROR_UNSUPPORTED:
		return "unsupported function";
	case SG_ERROR_XSW_VER_MISMATCH:
		return "XSW version mismatch";
	case SG_ERROR_PSTAT:
		return "pstat failed";
	case SG_ERROR_PDHOPEN:
		return "PDH open failed";
	case SG_ERROR_PDHCOLLECT:
		return "PDH snapshot failed";
	case SG_ERROR_PDHADD:
		return "PDH add counter failed";
	case SG_ERROR_PDHREAD:
		return "PDH read counter failed";
	case SG_ERROR_DEVICES:
		return "failed to get device list";
	case SG_ERROR_PERMISSION:
		return "access violation";
	case SG_ERROR_DISKINFO:
		return "disk function failed";
	case SG_ERROR_MEMSTATUS:
		return "memory status failed";
	}
	return "unknown error";
}

