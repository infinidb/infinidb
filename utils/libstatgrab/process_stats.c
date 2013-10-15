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
 * $Id: process_stats.c,v 1.82 2006/10/09 14:47:58 tdb Exp $
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "statgrab.h"
#include "tools.h"
#include "vector.h"
#if defined(SOLARIS) || defined(LINUX)
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <dirent.h>
#endif
#include <string.h>

#ifdef SOLARIS
#include <procfs.h>
#include <limits.h>
#define PROC_LOCATION "/proc"
#define MAX_FILE_LENGTH PATH_MAX
#endif
#ifdef LINUX
#include <limits.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#define PROC_LOCATION "/proc"
#define MAX_FILE_LENGTH PATH_MAX
#endif
#ifdef ALLBSD
#include <errno.h>
#include <stdlib.h>
#include <sys/param.h>
#include <sys/sysctl.h>
#if defined(FREEBSD) || defined(DFBSD)
#include <sys/user.h>
#else
#include <sys/proc.h>
#endif
#include <string.h>
#include <paths.h>
#include <fcntl.h>
#include <limits.h>
#if (defined(FREEBSD) && !defined(FREEBSD5)) || defined(DFBSD)
#include <kvm.h>
#endif
#include <unistd.h>
#ifdef NETBSD2
#include <sys/lwp.h>
#endif
#endif
#ifdef HPUX
#include <sys/param.h>
#include <sys/pstat.h>
#include <unistd.h>
#define PROCESS_BATCH 30
#endif
#ifdef WIN32
#include <windows.h>
#include <psapi.h>
#endif

static void proc_state_init(sg_process_stats *s) {
	s->process_name = NULL;
	s->proctitle = NULL;
}

static void proc_state_destroy(sg_process_stats *s) {
	free(s->process_name);
	free(s->proctitle);
}

sg_process_stats *sg_get_process_stats(int *entries){
	VECTOR_DECLARE_STATIC(proc_state, sg_process_stats, 64,
			      proc_state_init, proc_state_destroy);
	int proc_state_size = 0;
	sg_process_stats *proc_state_ptr;
#ifdef HPUX
	struct pst_status pstat_procinfo[PROCESS_BATCH];
	long procidx = 0;
	long long pagesize;
	int num, i;
#endif
#ifdef ALLBSD
	int mib[4];
	size_t size;
	struct kinfo_proc *kp_stats;
	int procs, i;
	char *proctitle;
#if (defined(FREEBSD) && !defined(FREEBSD5)) || defined(DFBSD)
	kvm_t *kvmd;
	char **args, **argsp;
	int argslen = 0;
#else
	long buflen;
	char *p, *proctitletmp;
#endif
#ifdef NETBSD2
	int lwps;
	struct kinfo_lwp *kl_stats;
#endif
#endif
#if defined(SOLARIS) || defined(LINUX)
	DIR *proc_dir;
	struct dirent *dir_entry;
	char filename[MAX_FILE_LENGTH];
	FILE *f;
#ifdef SOLARIS
	psinfo_t process_info;
#endif
#ifdef LINUX
	char s;
	/* If someone has a executable of 4k filename length, they deserve to get it truncated :) */
	char ps_name[4096];
	char *ptr;
	VECTOR_DECLARE_STATIC(psargs, char, 128, NULL, NULL);
	unsigned long stime, utime, starttime;
	int x;
	int fn;
	int len;
	int rc;
	time_t uptime;
	long tickspersec;
#endif

#ifdef LINUX
	if ((f=fopen("/proc/uptime", "r")) == NULL) {
		sg_set_error_with_errno(SG_ERROR_OPEN, "/proc/uptime");
		return NULL;
	}
	if((fscanf(f,"%lu %*d",&uptime)) != 1){
		sg_set_error(SG_ERROR_PARSE, NULL);
		return NULL;
	}
	fclose(f);
#endif

	if((proc_dir=opendir(PROC_LOCATION))==NULL){
		sg_set_error_with_errno(SG_ERROR_OPENDIR, PROC_LOCATION);
		return NULL;
	}

	while((dir_entry=readdir(proc_dir))!=NULL){
		if(atoi(dir_entry->d_name) == 0) continue;

#ifdef SOLARIS
		snprintf(filename, MAX_FILE_LENGTH, "/proc/%s/psinfo", dir_entry->d_name);
#endif
#ifdef LINUX
		snprintf(filename, MAX_FILE_LENGTH, "/proc/%s/stat", dir_entry->d_name);
#endif
		if((f=fopen(filename, "r"))==NULL){
			/* Open failed.. Process since vanished, or the path was too long.
			 * Ah well, move onwards to the next one */
			continue;
		}
#ifdef SOLARIS
		fread(&process_info, sizeof(psinfo_t), 1, f);
		fclose(f);
#endif

		if (VECTOR_RESIZE(proc_state, proc_state_size + 1) < 0) {
			return NULL;
		}
		proc_state_ptr = proc_state+proc_state_size;

#ifdef SOLARIS		
		proc_state_ptr->pid = process_info.pr_pid;
		proc_state_ptr->parent = process_info.pr_ppid;
		proc_state_ptr->pgid = process_info.pr_pgid;
		proc_state_ptr->uid = process_info.pr_uid;
		proc_state_ptr->euid = process_info.pr_euid;
		proc_state_ptr->gid = process_info.pr_gid;
		proc_state_ptr->egid = process_info.pr_egid;
		proc_state_ptr->proc_size = (process_info.pr_size) * 1024;
		proc_state_ptr->proc_resident = (process_info.pr_rssize) * 1024;
		proc_state_ptr->time_spent = process_info.pr_time.tv_sec;
		proc_state_ptr->cpu_percent = (process_info.pr_pctcpu * 100.0) / 0x8000;
		proc_state_ptr->nice = (int)process_info.pr_lwp.pr_nice - 20;
		if (sg_update_string(&proc_state_ptr->process_name,
				     process_info.pr_fname) < 0) {
			return NULL;
		}
		if (sg_update_string(&proc_state_ptr->proctitle,
				     process_info.pr_psargs) < 0) {
			return NULL;
		}

		switch (process_info.pr_lwp.pr_state) {
		case 1:
			proc_state_ptr->state = SG_PROCESS_STATE_SLEEPING;
			break;
		case 2:
		case 5:
			proc_state_ptr->state = SG_PROCESS_STATE_RUNNING; 
			break;
		case 3:
			proc_state_ptr->state = SG_PROCESS_STATE_ZOMBIE; 
			break;
		case 4:
			proc_state_ptr->state = SG_PROCESS_STATE_STOPPED; 
			break;
		}
#endif
#ifdef LINUX
		x = fscanf(f, "%d %4096s %c %d %d %*d %*d %*d %*u %*u %*u %*u %*u %lu %lu %*d %*d %*d %d %*d %*d %lu %llu %llu %*u %*u %*u %*u %*u %*u %*u %*u %*u %*u %*u %*u %*u %*d %*d\n", &(proc_state_ptr->pid), ps_name, &s, &(proc_state_ptr->parent), &(proc_state_ptr->pgid), &utime, &stime, &(proc_state_ptr->nice), &starttime, &(proc_state_ptr->proc_size), &(proc_state_ptr->proc_resident));
		/* +3 becuase man page says "Resident  Set Size: number of pages the process has in real memory, minus 3 for administrative purposes." */
		proc_state_ptr->proc_resident = (proc_state_ptr->proc_resident + 3) * getpagesize();
		switch (s) {
		case 'S':
			proc_state_ptr->state = SG_PROCESS_STATE_SLEEPING;
			break;
		case 'R':
			proc_state_ptr->state = SG_PROCESS_STATE_RUNNING;
			break;
		case 'Z':
			proc_state_ptr->state = SG_PROCESS_STATE_ZOMBIE;
			break;
		case 'T':
		case 'D':
			proc_state_ptr->state = SG_PROCESS_STATE_STOPPED;
			break;
		}
	
		/* pa_name[0] should = '(' */
		ptr = strchr(&ps_name[1], ')');	
		if(ptr !=NULL) *ptr='\0';

		if (sg_update_string(&proc_state_ptr->process_name,
				     &ps_name[1]) < 0) {
			return NULL;
		}

		/* cpu */
		proc_state_ptr->cpu_percent = (100.0 * (utime + stime)) / ((uptime * 100.0) - starttime);
		tickspersec = sysconf (_SC_CLK_TCK);
		if (tickspersec < 0) {
			proc_state_ptr->time_spent = 0;
		}
		else {
			proc_state_ptr->time_spent = (utime + stime) / tickspersec;
		}

		fclose(f);

		/* uid / gid */
		snprintf(filename, MAX_FILE_LENGTH, "/proc/%s/status", dir_entry->d_name);
		if ((f=fopen(filename, "r")) == NULL) {
			/* Open failed.. Process since vanished, or the path was too long.
			 * Ah well, move onwards to the next one */
			continue;
		}

		if((ptr=sg_f_read_line(f, "Uid:"))==NULL){
			fclose(f);
			continue;
		}
		sscanf(ptr, "Uid:\t%d\t%d\t%*d\t%*d\n", &(proc_state_ptr->uid), &(proc_state_ptr->euid));

		if((ptr=sg_f_read_line(f, "Gid:"))==NULL){
			fclose(f);
			continue;
		}
		sscanf(ptr, "Gid:\t%d\t%d\t%*d\t%*d\n", &(proc_state_ptr->gid), &(proc_state_ptr->egid));

		fclose(f);

		/* proctitle */	
		snprintf(filename, MAX_FILE_LENGTH, "/proc/%s/cmdline", dir_entry->d_name);

		if((fn=open(filename, O_RDONLY)) == -1){
			/* Open failed.. Process since vanished, or the path was too long.
			 * Ah well, move onwards to the next one */
			continue;
		}

#define READ_BLOCK_SIZE 128
		len = 0;
		do {
			if (VECTOR_RESIZE(psargs, len + READ_BLOCK_SIZE) < 0) {
				return NULL;
			}
			rc = read(fn, psargs + len, READ_BLOCK_SIZE);
			if (rc > 0) {
				len += rc;
			}
		} while (rc == READ_BLOCK_SIZE);
		close(fn);

		if (rc == -1) {
			/* Read failed; move on. */
			continue;
		}

		/* Turn \0s into spaces within the command line. */
		ptr = psargs;
		for(x = 0; x < len; x++) {
			if (*ptr == '\0') *ptr = ' ';
			ptr++;
		}

		if (len == 0) {
			/* We want psargs to be NULL. */
			if (VECTOR_RESIZE(psargs, 0) < 0) {
				return NULL;
			}
		} else {
			/* Not empty, so append a \0. */
			if (VECTOR_RESIZE(psargs, len + 1) < 0) {
				return NULL;
			}
			psargs[len] = '\0';
		}

		if (sg_update_string(&proc_state_ptr->proctitle, psargs) < 0) {
			return NULL;
		}
#endif

		proc_state_size++;
	}
	closedir(proc_dir);
#endif

#ifdef ALLBSD
	mib[0] = CTL_KERN;
	mib[1] = KERN_PROC;
	mib[2] = KERN_PROC_ALL;

	if(sysctl(mib, 3, NULL, &size, NULL, 0) < 0) {
		sg_set_error_with_errno(SG_ERROR_SYSCTL,
		                        "CTL_KERN.KERN_PROC.KERN_PROC_ALL");
		return NULL;
	}

	procs = size / sizeof(struct kinfo_proc);

	kp_stats = sg_malloc(size);
	if(kp_stats == NULL) {
		return NULL;
	}
	memset(kp_stats, 0, size);

	if(sysctl(mib, 3, kp_stats, &size, NULL, 0) < 0) {
		sg_set_error_with_errno(SG_ERROR_SYSCTL,
		                        "CTL_KERN.KERN_PROC.KERN_PROC_ALL");
		free(kp_stats);
		return NULL;
	}

#if (defined(FREEBSD) && !defined(FREEBSD5)) || defined(DFBSD)
	kvmd = sg_get_kvm2();
#endif

	for (i = 0; i < procs; i++) {
		const char *name;

#ifdef FREEBSD5
		if (kp_stats[i].ki_stat == 0) {
#else
		if (kp_stats[i].kp_proc.p_stat == 0) {
#endif
			/* FreeBSD 5 deliberately overallocates the array that
			 * the sysctl returns, so we'll get a few junk
			 * processes on the end that we have to ignore. (Search
			 * for "overestimate by 5 procs" in
			 * src/sys/kern/kern_proc.c for more details.) */
			continue;
		}

		if (VECTOR_RESIZE(proc_state, proc_state_size + 1) < 0) {
			return NULL;
		}
		proc_state_ptr = proc_state+proc_state_size;

#ifdef FREEBSD5
		name = kp_stats[i].ki_comm;
#elif defined(DFBSD)
		name = kp_stats[i].kp_thread.td_comm;
#else
		name = kp_stats[i].kp_proc.p_comm;
#endif
		if (sg_update_string(&proc_state_ptr->process_name, name) < 0) {
			return NULL;
		}

#if defined(FREEBSD5) || defined(NETBSD) || defined(OPENBSD)

#ifdef FREEBSD5
		mib[2] = KERN_PROC_ARGS;
		mib[3] = kp_stats[i].ki_pid;
#else
		mib[1] = KERN_PROC_ARGS;
		mib[2] = kp_stats[i].kp_proc.p_pid;
		mib[3] = KERN_PROC_ARGV;
#endif

		free(proc_state_ptr->proctitle);
		proc_state_ptr->proctitle = NULL;

/* Starting size - we'll double this straight away */
#define PROCTITLE_START_SIZE 64
		buflen = PROCTITLE_START_SIZE;
		size = buflen;
		proctitle = NULL;

		do {
			if((long) size >= buflen) {
				buflen *= 2;
				size = buflen;
				proctitletmp = sg_realloc(proctitle, buflen);
				if(proctitletmp == NULL) {
					free(proctitle);
					proctitle = NULL;
					proc_state_ptr->proctitle = NULL;
					size = 0;
					break;
				}
				proctitle = proctitletmp;
				bzero(proctitle, buflen);
			}

			if(sysctl(mib, 4, proctitle, &size, NULL, 0) < 0) {
				free(proctitle);
				proctitle = NULL;
				proc_state_ptr->proctitle = NULL;
				size = 0;
				break;
			}
		} while((long) size >= buflen);

		if(size > 0) {
			proc_state_ptr->proctitle = sg_malloc(size+1);
			if(proc_state_ptr->proctitle == NULL) {
				return NULL;
			}
			p = proctitle;
#ifdef OPENBSD
			/* On OpenBSD, this value has the argv pointers (which
			 * are terminated by a NULL) at the front, so we have
			 * to skip over them to get to the strings. */
			while (*(char ***)p != NULL) {
				p += sizeof(char **);
			}
			p += sizeof(char **);
#endif
			proc_state_ptr->proctitle[0] = '\0';
			do {
				sg_strlcat(proc_state_ptr->proctitle, p, size+1);
				sg_strlcat(proc_state_ptr->proctitle, " ", size+1);
				p += strlen(p) + 1;
			} while (p < proctitle + size);
			free(proctitle);
			proctitle = NULL;
			/* remove trailing space */
			proc_state_ptr->proctitle[strlen(proc_state_ptr->proctitle)-1] = '\0';
		}
		else {
			if(proctitle != NULL) {
				free(proctitle);
				proctitle = NULL;
			}
			proc_state_ptr->proctitle = NULL;
		}
#else
		free(proc_state_ptr->proctitle);
		proc_state_ptr->proctitle = NULL;
		if(kvmd != NULL) {
			args = kvm_getargv(kvmd, &(kp_stats[i]), 0);
			if(args != NULL) {
				argsp = args;
				while(*argsp != NULL) {
					argslen += strlen(*argsp) + 1;
					argsp++;
				}
				proctitle = sg_malloc(argslen + 1);
				proctitle[0] = '\0';
				if(proctitle == NULL) {
					return NULL;
				}
				while(*args != NULL) {
					sg_strlcat(proctitle, *args, argslen + 1);
					sg_strlcat(proctitle, " ", argslen + 1);
					args++;
				}
				/* remove trailing space */
				proctitle[strlen(proctitle)-1] = '\0';
				proc_state_ptr->proctitle = proctitle;
			}
			else {
				proc_state_ptr->proctitle = NULL;
			}
		}
		else {
			proc_state_ptr->proctitle = NULL;
		}
#endif

#ifdef FREEBSD5
		proc_state_ptr->pid = kp_stats[i].ki_pid;
		proc_state_ptr->parent = kp_stats[i].ki_ppid;
		proc_state_ptr->pgid = kp_stats[i].ki_pgid;
#else
		proc_state_ptr->pid = kp_stats[i].kp_proc.p_pid;
		proc_state_ptr->parent = kp_stats[i].kp_eproc.e_ppid;
		proc_state_ptr->pgid = kp_stats[i].kp_eproc.e_pgid;
#endif

#ifdef FREEBSD5
		proc_state_ptr->uid = kp_stats[i].ki_ruid;
		proc_state_ptr->euid = kp_stats[i].ki_uid;
		proc_state_ptr->gid = kp_stats[i].ki_rgid;
		proc_state_ptr->egid = kp_stats[i].ki_svgid;
#elif defined(DFBSD)
		proc_state_ptr->uid = kp_stats[i].kp_eproc.e_ucred.cr_ruid;
		proc_state_ptr->euid = kp_stats[i].kp_eproc.e_ucred.cr_svuid;
		proc_state_ptr->gid = kp_stats[i].kp_eproc.e_ucred.cr_rgid;
		proc_state_ptr->egid = kp_stats[i].kp_eproc.e_ucred.cr_svgid;
#else
		proc_state_ptr->uid = kp_stats[i].kp_eproc.e_pcred.p_ruid;
		proc_state_ptr->euid = kp_stats[i].kp_eproc.e_pcred.p_svuid;
		proc_state_ptr->gid = kp_stats[i].kp_eproc.e_pcred.p_rgid;
		proc_state_ptr->egid = kp_stats[i].kp_eproc.e_pcred.p_svgid;
#endif

#ifdef FREEBSD5
		proc_state_ptr->proc_size = kp_stats[i].ki_size;
		/* This is in pages */
		proc_state_ptr->proc_resident =
			kp_stats[i].ki_rssize * getpagesize();
		/* This is in microseconds */
		proc_state_ptr->time_spent = kp_stats[i].ki_runtime / 1000000;
		proc_state_ptr->cpu_percent =
			((double)kp_stats[i].ki_pctcpu / FSCALE) * 100.0;
		proc_state_ptr->nice = kp_stats[i].ki_nice;
#else
		proc_state_ptr->proc_size =
			kp_stats[i].kp_eproc.e_vm.vm_map.size;
		/* This is in pages */
		proc_state_ptr->proc_resident =
			kp_stats[i].kp_eproc.e_vm.vm_rssize * getpagesize();
#if defined(NETBSD) || defined(OPENBSD)
		proc_state_ptr->time_spent =
			kp_stats[i].kp_proc.p_rtime.tv_sec;
#elif defined(DFBSD)
		proc_state_ptr->time_spent = 
			( kp_stats[i].kp_thread.td_uticks +
			kp_stats[i].kp_thread.td_sticks +
			kp_stats[i].kp_thread.td_iticks ) / 1000000;
#else
		/* This is in microseconds */
		proc_state_ptr->time_spent =
			kp_stats[i].kp_proc.p_runtime / 1000000;
#endif
		proc_state_ptr->cpu_percent =
			((double)kp_stats[i].kp_proc.p_pctcpu / FSCALE) * 100.0;
		proc_state_ptr->nice = kp_stats[i].kp_proc.p_nice;
#endif

#ifdef NETBSD2
		{
			size_t size;
			int mib[5];

			mib[0] = CTL_KERN;
			mib[1] = KERN_LWP;
			mib[2] = kp_stats[i].kp_proc.p_pid;
			mib[3] = sizeof(struct kinfo_lwp);
			mib[4] = 0;

			if(sysctl(mib, 5, NULL, &size, NULL, 0) < 0) {
				sg_set_error_with_errno(SG_ERROR_SYSCTL, "CTL_KERN.KERN_LWP.pid.structsize.0");
				return NULL;
			}

			lwps = size / sizeof(struct kinfo_lwp);
			mib[4] = lwps;

			kl_stats = sg_malloc(size);
			if(kl_stats == NULL) {
				return NULL;
			}

			if(sysctl(mib, 5, kl_stats, &size, NULL, 0) < 0) {
				sg_set_error_with_errno(SG_ERROR_SYSCTL, "CTL_KERN.KERN_LWP.pid.structsize.buffersize");
				return NULL;
			}
		}

		switch(kp_stats[i].kp_proc.p_stat) {
		case SIDL:
			proc_state_ptr->state = SG_PROCESS_STATE_RUNNING;
			break;
		case SACTIVE:
			{
				int i;

				for(i = 0; i < lwps; i++) {
					switch(kl_stats[i].l_stat) {
					case LSONPROC:
					case LSRUN:
						proc_state_ptr->state = SG_PROCESS_STATE_RUNNING;
						goto end;
					case LSSLEEP:
						proc_state_ptr->state = SG_PROCESS_STATE_SLEEPING;
						goto end;
					case LSSTOP:
					case LSSUSPENDED:
						proc_state_ptr->state = SG_PROCESS_STATE_STOPPED;
						goto end;
					}
					proc_state_ptr->state = SG_PROCESS_STATE_UNKNOWN;
				}
				end: ;
			}
			break;
		case SSTOP:
			proc_state_ptr->state = SG_PROCESS_STATE_STOPPED;
			break;
		case SZOMB:
			proc_state_ptr->state = SG_PROCESS_STATE_ZOMBIE;
			break;
		default:
			proc_state_ptr->state = SG_PROCESS_STATE_UNKNOWN;
			break;
		}

		free(kl_stats);
#else
#ifdef FREEBSD5
		switch (kp_stats[i].ki_stat) {
#else
		switch (kp_stats[i].kp_proc.p_stat) {
#endif
		case SIDL:
		case SRUN:
#ifdef SONPROC
		case SONPROC: /* NetBSD */
#endif
			proc_state_ptr->state = SG_PROCESS_STATE_RUNNING;
			break;
		case SSLEEP:
#ifdef SWAIT
		case SWAIT: /* FreeBSD 5 */
#endif
#ifdef SLOCK
		case SLOCK: /* FreeBSD 5 */
#endif
			proc_state_ptr->state = SG_PROCESS_STATE_SLEEPING;
			break;
		case SSTOP:
			proc_state_ptr->state = SG_PROCESS_STATE_STOPPED;
			break;
		case SZOMB:
#ifdef SDEAD
		case SDEAD: /* OpenBSD & NetBSD */
#endif
			proc_state_ptr->state = SG_PROCESS_STATE_ZOMBIE;
			break;
		default:
			proc_state_ptr->state = SG_PROCESS_STATE_UNKNOWN;
			break;
		}
#endif
		proc_state_size++;
	}

	free(kp_stats);
#endif

#ifdef HPUX
	if ((pagesize = sysconf(_SC_PAGESIZE)) == -1) {
		sg_set_error_with_errno(SG_ERROR_SYSCONF, "_SC_PAGESIZE");
		return NULL;
	}

	while (1) {
		num = pstat_getproc(pstat_procinfo, sizeof pstat_procinfo[0],
		                    PROCESS_BATCH, procidx);
		if (num == -1) {
			sg_set_error_with_errno(SG_ERROR_PSTAT,
			                        "pstat_getproc");
			return NULL;
		} else if (num == 0) {
			break;
		}

		for (i = 0; i < num; i++) {
			struct pst_status *pi = &pstat_procinfo[i];

			if (VECTOR_RESIZE(proc_state, proc_state_size + 1) < 0) {
				return NULL;
			}
			proc_state_ptr = proc_state+proc_state_size;
	
			proc_state_ptr->pid = pi->pst_pid;
			proc_state_ptr->parent = pi->pst_ppid;
			proc_state_ptr->pgid = pi->pst_pgrp;
			proc_state_ptr->uid = pi->pst_uid;
			proc_state_ptr->euid = pi->pst_euid;
			proc_state_ptr->gid = pi->pst_gid;
			proc_state_ptr->egid = pi->pst_egid;
			proc_state_ptr->proc_size = (pi->pst_dsize + pi->pst_tsize + pi->pst_ssize) * pagesize;
			proc_state_ptr->proc_resident = pi->pst_rssize * pagesize;
			proc_state_ptr->time_spent = pi->pst_time;
			proc_state_ptr->cpu_percent = (pi->pst_pctcpu * 100.0) / 0x8000;
			proc_state_ptr->nice = pi->pst_nice;
	
			if (sg_update_string(&proc_state_ptr->process_name,
					     pi->pst_ucomm) < 0) {
				return NULL;
			}
			if (sg_update_string(&proc_state_ptr->proctitle,
					     pi->pst_cmd) < 0) {
				return NULL;
			}
	
			switch (pi->pst_stat) {
			case PS_SLEEP:
				proc_state_ptr->state = SG_PROCESS_STATE_SLEEPING;
				break;
			case PS_RUN:
				proc_state_ptr->state = SG_PROCESS_STATE_RUNNING;
				break;
			case PS_STOP:
				proc_state_ptr->state = SG_PROCESS_STATE_STOPPED;
				break;
			case PS_ZOMBIE:
				proc_state_ptr->state = SG_PROCESS_STATE_ZOMBIE;
				break;
			case PS_IDLE:
			case PS_OTHER:
				proc_state_ptr->state = SG_PROCESS_STATE_UNKNOWN;
				break;
			}
	
			proc_state_size++;
		}
		procidx = pstat_procinfo[num - 1].pst_idx + 1;
	}
#endif

#ifdef CYGWIN
	sg_set_error(SG_ERROR_UNSUPPORTED, "Cygwin");
	return NULL;
#endif
#ifdef WIN32
	/* FIXME The data needed for this is probably do able with the 
	 * "performance registry". Although using this appears to be a black
	 * art and closely guarded secret.
	 * This is not directly used in ihost, so not considered a priority */
	sg_set_error(SG_ERROR_UNSUPPORTED, "Win32");
	return NULL;
#endif

	*entries = proc_state_size;
	return proc_state;
}

sg_process_count *sg_get_process_count() {
	static sg_process_count process_stat;
#ifndef WIN32
	sg_process_stats *ps;
	int ps_size, x;
#else
	DWORD aProcesses[1024];
	DWORD cbNeeded;
#endif

	process_stat.sleeping = 0;
	process_stat.running = 0;
	process_stat.zombie = 0;
	process_stat.stopped = 0;
	process_stat.total = 0;

#ifndef WIN32
	ps = sg_get_process_stats(&ps_size);
	if (ps == NULL) {
		return NULL;
	}

	for(x = 0; x < ps_size; x++) {
		switch (ps->state) {
		case SG_PROCESS_STATE_RUNNING:
			process_stat.running++;
			break;
		case SG_PROCESS_STATE_SLEEPING:
			process_stat.sleeping++;
			break;
		case SG_PROCESS_STATE_STOPPED:
			process_stat.stopped++;
			break;
		case SG_PROCESS_STATE_ZOMBIE:
			process_stat.zombie++;
			break;
		default:
			/* currently no mapping for SG_PROCESS_STATE_UNKNOWN in
			 * sg_process_count */
			break;
		}
		ps++;
	}

	process_stat.total = ps_size;
#else
	if (!EnumProcesses(aProcesses, sizeof(aProcesses), &cbNeeded))
		return NULL;
	process_stat.total = cbNeeded / sizeof(DWORD);
#endif

	return &process_stat;
}

int sg_process_compare_name(const void *va, const void *vb) {
	const sg_process_stats *a = (sg_process_stats *)va;
	const sg_process_stats *b = (sg_process_stats *)vb;

	return strcmp(a->process_name, b->process_name);
}

int sg_process_compare_pid(const void *va, const void *vb) {
	const sg_process_stats *a = (sg_process_stats *)va;
	const sg_process_stats *b = (sg_process_stats *)vb;

	if (a->pid < b->pid) {
		return -1;
	} else if (a->pid == b->pid) {
		return 0;
	} else {
		return 1;
	}
}

int sg_process_compare_uid(const void *va, const void *vb) {
	const sg_process_stats *a = (sg_process_stats *)va;
	const sg_process_stats *b = (sg_process_stats *)vb;

	if (a->uid < b->uid) {
		return -1;
	} else if (a->uid == b->uid) {
		return 0;
	} else {
		return 1;
	}
}

int sg_process_compare_gid(const void *va, const void *vb) {
	const sg_process_stats *a = (sg_process_stats *)va;
	const sg_process_stats *b = (sg_process_stats *)vb;

	if (a->gid < b->gid) {
		return -1;
	} else if (a->gid == b->gid) {
		return 0;
	} else {
		return 1;
	}
}

int sg_process_compare_size(const void *va, const void *vb) {
	const sg_process_stats *a = (sg_process_stats *)va;
	const sg_process_stats *b = (sg_process_stats *)vb;

	if (a->proc_size < b->proc_size) {
		return -1;
	} else if (a->proc_size == b->proc_size) {
		return 0;
	} else {
		return 1;
	}
}

int sg_process_compare_res(const void *va, const void *vb) {
	const sg_process_stats *a = (sg_process_stats *)va;
	const sg_process_stats *b = (sg_process_stats *)vb;

	if (a->proc_resident < b->proc_resident) {
		return -1;
	} else if (a->proc_resident == b->proc_resident) {
		return 0;
	} else {
		return 1;
	}
}

int sg_process_compare_cpu(const void *va, const void *vb) {
	const sg_process_stats *a = (sg_process_stats *)va;
	const sg_process_stats *b = (sg_process_stats *)vb;

	if (a->cpu_percent < b->cpu_percent) {
		return -1;
	} else if (a->cpu_percent == b->cpu_percent) {
		return 0;
	} else {
		return 1;
	}
}

int sg_process_compare_time(const void *va, const void *vb) {
	const sg_process_stats *a = (sg_process_stats *)va;
	const sg_process_stats *b = (sg_process_stats *)vb;

	if (a->time_spent < b->time_spent) {
		return -1;
	} else if (a->time_spent == b->time_spent) {
		return 0;
	} else {
		return 1;
	}
}

