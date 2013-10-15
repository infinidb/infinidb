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
 * $Id: statgrab.h,v 1.58 2006/03/11 13:11:21 tdb Exp $
 */

#ifndef STATGRAB_H
#define STATGRAB_H

#ifdef __cplusplus
extern "C" {
#endif

#include <sys/types.h>

/* FIXME typedefs for 32/64-bit types */
/* FIXME maybe tidy up field names? */
/* FIXME comments for less obvious fields */

int sg_init(void);
int sg_snapshot();
int sg_shutdown();
int sg_drop_privileges(void);

typedef enum {
	SG_ERROR_NONE = 0,
	SG_ERROR_ASPRINTF,
	SG_ERROR_DEVICES,
	SG_ERROR_DEVSTAT_GETDEVS,
	SG_ERROR_DEVSTAT_SELECTDEVS,
	SG_ERROR_DISKINFO,
	SG_ERROR_ENOENT,
	SG_ERROR_GETIFADDRS,
	SG_ERROR_GETMNTINFO,
	SG_ERROR_GETPAGESIZE,
	SG_ERROR_HOST,
	SG_ERROR_KSTAT_DATA_LOOKUP,
	SG_ERROR_KSTAT_LOOKUP,
	SG_ERROR_KSTAT_OPEN,
	SG_ERROR_KSTAT_READ,
	SG_ERROR_KVM_GETSWAPINFO,
	SG_ERROR_KVM_OPENFILES,
	SG_ERROR_MALLOC,
	SG_ERROR_MEMSTATUS,
	SG_ERROR_OPEN,
	SG_ERROR_OPENDIR,
	SG_ERROR_PARSE,
	SG_ERROR_PDHADD,
	SG_ERROR_PDHCOLLECT,
	SG_ERROR_PDHOPEN,
	SG_ERROR_PDHREAD,
	SG_ERROR_PERMISSION,
	SG_ERROR_PSTAT,
	SG_ERROR_SETEGID,
	SG_ERROR_SETEUID,
	SG_ERROR_SETMNTENT,
	SG_ERROR_SOCKET,
	SG_ERROR_SWAPCTL,
	SG_ERROR_SYSCONF,
	SG_ERROR_SYSCTL,
	SG_ERROR_SYSCTLBYNAME,
	SG_ERROR_SYSCTLNAMETOMIB,
	SG_ERROR_UNAME,
	SG_ERROR_UNSUPPORTED,
	SG_ERROR_XSW_VER_MISMATCH
} sg_error;

void sg_set_error(sg_error code, const char *arg);
void sg_set_error_with_errno(sg_error code, const char *arg);
sg_error sg_get_error();
const char *sg_get_error_arg();
int sg_get_error_errno();
const char *sg_str_error(sg_error code);

typedef struct {
	char *os_name;
	char *os_release;
	char *os_version;
	char *platform;
	char *hostname;
	time_t uptime;
} sg_host_info;

sg_host_info *sg_get_host_info();

typedef struct {
	long long user;
	long long kernel;
	long long idle;
	long long iowait;
	long long swap;
	long long nice;
	long long total;
	time_t systime;
} sg_cpu_stats;

sg_cpu_stats *sg_get_cpu_stats();
sg_cpu_stats *sg_get_cpu_stats_diff();

typedef struct {
	float user;
	float kernel;
	float idle;
	float iowait;
	float swap;
	float nice;
	time_t time_taken;
} sg_cpu_percents;

sg_cpu_percents *sg_get_cpu_percents();

typedef struct {
	long long total;
	long long free;
	long long used;
	long long cache;
} sg_mem_stats;

sg_mem_stats *sg_get_mem_stats();

typedef struct {
	double min1;
	double min5;
	double min15;
} sg_load_stats;

sg_load_stats *sg_get_load_stats();

typedef struct {
	char *name_list;
	int num_entries;
} sg_user_stats;

sg_user_stats *sg_get_user_stats();

typedef struct {
	long long total;
	long long used;
	long long free;
} sg_swap_stats;

sg_swap_stats *sg_get_swap_stats();

typedef struct {
	char *device_name;
	char *fs_type;
	char *mnt_point;
	long long size;
	long long used;
	long long avail;
	long long total_inodes;
	long long used_inodes;
	long long free_inodes;
	long long avail_inodes;
	long long io_size;
	long long block_size;
	long long total_blocks;
	long long free_blocks;
	long long used_blocks;
	long long avail_blocks;
} sg_fs_stats;

sg_fs_stats *sg_get_fs_stats(int *entries);

int sg_fs_compare_device_name(const void *va, const void *vb);
int sg_fs_compare_mnt_point(const void *va, const void *vb);

typedef struct {
	char *disk_name;
	long long read_bytes;
	long long write_bytes;
	time_t systime;
} sg_disk_io_stats;

sg_disk_io_stats *sg_get_disk_io_stats(int *entries);
sg_disk_io_stats *sg_get_disk_io_stats_diff(int *entries);

int sg_disk_io_compare_name(const void *va, const void *vb);

typedef struct {
	char *interface_name;
	long long tx;
	long long rx;
	long long ipackets;
	long long opackets;
	long long ierrors;
	long long oerrors;
	long long collisions;
	time_t systime;
} sg_network_io_stats;

sg_network_io_stats *sg_get_network_io_stats(int *entries);
sg_network_io_stats *sg_get_network_io_stats_diff(int *entries);

int sg_network_io_compare_name(const void *va, const void *vb);

typedef enum {
	SG_IFACE_DUPLEX_FULL,
	SG_IFACE_DUPLEX_HALF,
	SG_IFACE_DUPLEX_UNKNOWN
} sg_iface_duplex;

typedef struct {
	char *interface_name;
	int speed;	/* In megabits/sec */
	sg_iface_duplex duplex;
#ifdef SG_ENABLE_DEPRECATED
	sg_iface_duplex dup;
#endif
	int up;
} sg_network_iface_stats;

sg_network_iface_stats *sg_get_network_iface_stats(int *entries);

int sg_network_iface_compare_name(const void *va, const void *vb);

typedef struct {
	long long pages_pagein;
	long long pages_pageout;
	time_t systime;
} sg_page_stats;

sg_page_stats *sg_get_page_stats();
sg_page_stats *sg_get_page_stats_diff();

typedef enum {
	SG_PROCESS_STATE_RUNNING,
	SG_PROCESS_STATE_SLEEPING,
	SG_PROCESS_STATE_STOPPED,
	SG_PROCESS_STATE_ZOMBIE,
	SG_PROCESS_STATE_UNKNOWN
} sg_process_state;

typedef struct {
	char *process_name;
	char *proctitle;

	pid_t pid;
	pid_t parent; /* Parent pid */
	pid_t pgid;   /* process id of process group leader */

/* Windows does not have uid_t or gid_t types */
#ifndef WIN32
	uid_t uid;
	uid_t euid;
	gid_t gid;
	gid_t egid;
#else
	int uid;
	int euid;
	int gid;
	int egid;
#endif

	unsigned long long proc_size; /* in bytes */
	unsigned long long proc_resident; /* in bytes */
	time_t time_spent; /* time running in seconds */
	double cpu_percent;
	int nice;
	sg_process_state state;
} sg_process_stats;

sg_process_stats *sg_get_process_stats(int *entries);

int sg_process_compare_name(const void *va, const void *vb);
int sg_process_compare_pid(const void *va, const void *vb);
int sg_process_compare_uid(const void *va, const void *vb);
int sg_process_compare_gid(const void *va, const void *vb);
int sg_process_compare_size(const void *va, const void *vb);
int sg_process_compare_res(const void *va, const void *vb);
int sg_process_compare_cpu(const void *va, const void *vb);
int sg_process_compare_time(const void *va, const void *vb);

typedef struct {
	int total;
	int running;
	int sleeping;
	int stopped;
	int zombie;
} sg_process_count;

sg_process_count *sg_get_process_count();

#ifdef SG_ENABLE_DEPRECATED
#include "statgrab_deprecated.h"
#endif

#ifdef __cplusplus
}
#endif

#endif
