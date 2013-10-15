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
 * $Id: statgrab_deprecated.c,v 1.2 2004/04/06 21:08:03 tdb Exp $
 */

#define SG_ENABLE_DEPRECATED
#include "statgrab.h"

int statgrab_init() {
	return sg_init();
}

int statgrab_drop_privileges() {
	return sg_drop_privileges();
}

general_stat_t *get_general_stats() {
	return sg_get_host_info();
}

cpu_states_t *get_cpu_totals() {
	return sg_get_cpu_stats();
}

cpu_states_t *get_cpu_diff() {
	return sg_get_cpu_stats_diff();
}

cpu_percent_t *cpu_percent_usage() {
	return sg_get_cpu_percents();
}

mem_stat_t *get_memory_stats() {
	return sg_get_mem_stats();
}

load_stat_t *get_load_stats() {
	return sg_get_load_stats();
}

user_stat_t *get_user_stats() {
	return sg_get_user_stats();
}

swap_stat_t *get_swap_stats() {
	return sg_get_swap_stats();
}

disk_stat_t *get_disk_stats(int *entries) {
	return sg_get_fs_stats(entries);
}

diskio_stat_t *get_diskio_stats(int *entries) {
	return sg_get_disk_io_stats(entries);
}

diskio_stat_t *get_diskio_stats_diff(int *entries) {
	return sg_get_disk_io_stats_diff(entries);
}

network_stat_t *get_network_stats(int *entries) {
	return sg_get_network_io_stats(entries);
}

network_stat_t *get_network_stats_diff(int *entries) {
	return sg_get_network_io_stats_diff(entries);
}

network_iface_stat_t *get_network_iface_stats(int *entries) {
	return sg_get_network_iface_stats(entries);
}

page_stat_t *get_page_stats() {
	return sg_get_page_stats();
}

page_stat_t *get_page_stats_diff() {
	return sg_get_page_stats_diff();
}

process_stat_t *get_process_stats() {
	return sg_get_process_count();
}

