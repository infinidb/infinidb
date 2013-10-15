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
 * $Id: statgrab_deprecated.h,v 1.1 2004/04/06 16:37:34 tdb Exp $
 */

#include <sys/types.h>

int statgrab_init(void);
int statgrab_drop_privileges(void);

typedef sg_host_info general_stat_t;

general_stat_t *get_general_stats();

typedef sg_cpu_stats cpu_states_t;

cpu_states_t *get_cpu_totals();
cpu_states_t *get_cpu_diff();

typedef sg_cpu_percents cpu_percent_t;

cpu_percent_t *cpu_percent_usage();

typedef sg_mem_stats mem_stat_t;

mem_stat_t *get_memory_stats();

typedef sg_load_stats load_stat_t;

load_stat_t *get_load_stats();

typedef sg_user_stats user_stat_t;

user_stat_t *get_user_stats();

typedef sg_swap_stats swap_stat_t;

swap_stat_t *get_swap_stats();

typedef sg_fs_stats disk_stat_t;

disk_stat_t *get_disk_stats(int *entries);

typedef sg_disk_io_stats diskio_stat_t;

diskio_stat_t *get_diskio_stats(int *entries);
diskio_stat_t *get_diskio_stats_diff(int *entries);

typedef sg_network_io_stats network_stat_t;

network_stat_t *get_network_stats(int *entries);
network_stat_t *get_network_stats_diff(int *entries);

/* Changed in statgrab.h 1.33 */
typedef enum{
	FULL_DUPLEX,
	HALF_DUPLEX,
	UNKNOWN_DUPLEX
}statgrab_duplex;

typedef sg_network_iface_stats network_iface_stat_t;

network_iface_stat_t *get_network_iface_stats(int *entries);

typedef sg_page_stats page_stat_t;

page_stat_t *get_page_stats();
page_stat_t *get_page_stats_diff();

typedef sg_process_count process_stat_t;

process_stat_t *get_process_stats();

