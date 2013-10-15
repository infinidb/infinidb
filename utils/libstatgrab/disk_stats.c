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
 * $Id: disk_stats.c,v 1.85 2006/10/09 14:09:38 tdb Exp $
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "statgrab.h"
#include "vector.h"
#include "tools.h"

#ifdef SOLARIS
#include <sys/mnttab.h>
#include <sys/statvfs.h>
#include <kstat.h>
#define VALID_FS_TYPES {"ufs", "tmpfs", "vxfs", "nfs"}
#endif

#if defined(LINUX) || defined(CYGWIN)
#include <mntent.h>
#include <sys/vfs.h>
#include <sys/statvfs.h>
#endif

#ifdef LINUX
#define VALID_FS_TYPES {"adfs", "affs", "befs", "bfs", "efs", "ext2", \
			"ext3", "vxfs", "hfs", "hfsplus", "hpfs", "jffs", \
			"jffs2", "minix", "msdos", "ntfs", "qnx4", "ramfs", \
			"rootfs", "reiserfs", "sysv", "v7", "udf", "ufs", \
			"umsdos", "vfat", "xfs", "jfs", "nfs"}
#endif

#ifdef CYGWIN
#define VALID_FS_TYPES {"user"}
#endif

#ifdef ALLBSD
#include <sys/param.h>
#include <sys/ucred.h>
#include <sys/mount.h>
#endif
#if defined(FREEBSD) || defined(DFBSD)
#include <sys/dkstat.h>
#include <devstat.h>
#define VALID_FS_TYPES {"hpfs", "msdosfs", "ntfs", "udf", "ext2fs", \
			"ufs", "mfs", "nfs"}
#endif
#if defined(NETBSD) || defined(OPENBSD)
#include <sys/param.h>
#include <sys/sysctl.h>
#include <sys/disk.h>
#define VALID_FS_TYPES {"ffs", "mfs", "msdos", "lfs", "adosfs", "ext2fs", \
			"ntfs", "nfs"}
#endif

#ifdef HPUX
#include <sys/param.h>
#include <sys/pstat.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/vfs.h>
#include <mntent.h>
#include <dirent.h>
#include <stdio.h>
#include <time.h>
#define VALID_FS_TYPES {"vxfs", "hfs", "nfs"}
#define DISK_BATCH 30
#endif

#ifdef WIN32
#include "win32.h"
/*#define VALID_FS_TYPES {"NTFS", "FAT", "FAT32"} unused*/
#define BUFSIZE 512
#endif

#ifdef ALLBSD
#define SG_MP_FSTYPENAME(mp) (mp)->f_fstypename
#define SG_MP_DEVNAME(mp)    (mp)->f_mntfromname
#define SG_MP_MOUNTP(mp)     (mp)->f_mntonname
#define SG_FS_FRSIZE(fs)     (long long) (*(fs))->f_bsize
#define SG_FS_BSIZE(fs)      (long long) (*(fs))->f_iosize
#define SG_FS_BLOCKS(fs)     (long long) (*(fs))->f_blocks
#define SG_FS_BFREE(fs)      (long long) (*(fs))->f_bfree
#define SG_FS_BAVAIL(fs)     (long long) (*(fs))->f_bavail
#define SG_FS_FILES(fs)      (long long) (*(fs))->f_files
#define SG_FS_FFREE(fs)      (long long) (*(fs))->f_ffree
#define SG_FS_FAVAIL(fs)     -1LL
#endif
#if defined(LINUX) || defined(CYGWIN) || defined(HPUX)
#define SG_MP_FSTYPENAME(mp) (mp)->mnt_type
#define SG_MP_DEVNAME(mp)    (mp)->mnt_fsname
#define SG_MP_MOUNTP(mp)     (mp)->mnt_dir
#define SG_FS_FRSIZE(fs)     (long long) (fs).f_frsize
#define SG_FS_BSIZE(fs)      (long long) (fs).f_bsize
#define SG_FS_BLOCKS(fs)     (long long) (fs).f_blocks
#define SG_FS_BFREE(fs)      (long long) (fs).f_bfree
#define SG_FS_BAVAIL(fs)     (long long) (fs).f_bavail
#define SG_FS_FILES(fs)      (long long) (fs).f_files
#define SG_FS_FFREE(fs)      (long long) (fs).f_ffree
#define SG_FS_FAVAIL(fs)     (long long) (fs).f_favail
#endif
#ifdef SOLARIS
#define SG_MP_FSTYPENAME(mp) (mp).mnt_fstype
#define SG_MP_DEVNAME(mp)    (mp).mnt_special
#define SG_MP_MOUNTP(mp)     (mp).mnt_mountp
#define SG_FS_FRSIZE(fs)     (long long) (fs).f_frsize
#define SG_FS_BSIZE(fs)      (long long) (fs).f_bsize
#define SG_FS_BLOCKS(fs)     (long long) (fs).f_blocks
#define SG_FS_BFREE(fs)      (long long) (fs).f_bfree
#define SG_FS_BAVAIL(fs)     (long long) (fs).f_bavail
#define SG_FS_FILES(fs)      (long long) (fs).f_files
#define SG_FS_FFREE(fs)      (long long) (fs).f_ffree
#define SG_FS_FAVAIL(fs)     (long long) (fs).f_favail
#endif

static void disk_stat_init(sg_fs_stats *d) {
	d->device_name = NULL;
	d->fs_type = NULL;
	d->mnt_point = NULL;
}

static void disk_stat_destroy(sg_fs_stats *d) {
	free(d->device_name);
	free(d->fs_type);
	free(d->mnt_point);
}

#ifndef WIN32 /* not used by WIN32, so stop compiler throwing warnings */
static int is_valid_fs_type(const char *type) {
	const char *types[] = VALID_FS_TYPES;
	int i;

	for (i = 0; i < (int) (sizeof types / sizeof *types); i++) {
		if (strcmp(types[i], type) == 0) {
			return 1;
		}
	}
	return 0;
}
#endif

sg_fs_stats *sg_get_fs_stats(int *entries){
	VECTOR_DECLARE_STATIC(disk_stats, sg_fs_stats, 10,
			      disk_stat_init, disk_stat_destroy);

	int num_disks=0;
#if defined(LINUX) || defined (SOLARIS) || defined(CYGWIN) || defined(HPUX)
	FILE *f;
#endif

	sg_fs_stats *disk_ptr;

#ifdef SOLARIS
	struct mnttab mp;
	struct statvfs fs;
#endif
#if defined(LINUX) || defined(CYGWIN) || defined(HPUX)
	struct mntent *mp;
	struct statvfs fs;
#endif
#ifdef ALLBSD
	int nummnt;
#ifdef HAVE_STATVFS
	struct statvfs *mp, **fs;
#else
	struct statfs *mp, **fs;
#endif
#endif
#ifdef WIN32
	char lp_buf[MAX_PATH];
	char volume_name_buf[BUFSIZE];
	char filesys_name_buf[BUFSIZE];
	char drive[4] = " :\\";
	char *p;
	lp_buf[0]='\0';
#endif

#ifdef ALLBSD
	nummnt=getmntinfo(&mp, MNT_WAIT);
	if (nummnt<=0){
		sg_set_error_with_errno(SG_ERROR_GETMNTINFO, NULL);
		return NULL;
	}
	for(fs = &mp; nummnt--; (*fs)++){
#endif

#if defined(LINUX) || defined(CYGWIN) || defined(HPUX)
#ifdef MNT_MNTTAB
	if ((f=setmntent(MNT_MNTTAB, "r" ))==NULL){
#else
	if ((f=setmntent("/etc/mtab", "r" ))==NULL){
#endif
		sg_set_error(SG_ERROR_SETMNTENT, NULL);
		return NULL;
	}

	while((mp=getmntent(f))){
		if((statvfs(mp->mnt_dir, &fs)) !=0){
			continue;
		}	

#endif

#ifdef SOLARIS
	if ((f=fopen("/etc/mnttab", "r" ))==NULL){
		sg_set_error_with_errno(SG_ERROR_OPEN, "/etc/mnttab");
		return NULL;
	}
	while((getmntent(f, &mp)) == 0){
		if ((statvfs(mp.mnt_mountp, &fs)) !=0){
			continue;
		}
#endif

#ifdef WIN32
	if (!(GetLogicalDriveStrings(BUFSIZE-1, lp_buf))) {
		sg_set_error(SG_ERROR_GETMNTINFO, "GetLogicalDriveStrings");
		return NULL;
	}
	p = lp_buf;
	do {
		// Copy drive letter to template string
		*drive = *p;
		// Only interested in harddrives.
		int drive_type = GetDriveType(drive);

		if(drive_type == DRIVE_FIXED) {
#else
		if(is_valid_fs_type(SG_MP_FSTYPENAME(mp))){
#endif
			if (VECTOR_RESIZE(disk_stats, num_disks + 1) < 0) {
				return NULL;
			}
			disk_ptr=disk_stats+num_disks;

#ifndef WIN32
			/* Maybe make this char[bigenough] and do strncpy's and put a null in the end? 
			 * Downside is its a bit hungry for a lot of mounts, as MNT_MAX_SIZE would prob 
			 * be upwards of a k each 
			 */
			if (sg_update_string(&disk_ptr->device_name, SG_MP_DEVNAME(mp)) < 0) {
				return NULL;
			}
			if (sg_update_string(&disk_ptr->fs_type, SG_MP_FSTYPENAME(mp)) < 0) {
				return NULL;
			}
			if (sg_update_string(&disk_ptr->mnt_point, SG_MP_MOUNTP(mp)) < 0) {
				return NULL;
			}

			disk_ptr->size  = SG_FS_FRSIZE(fs) * SG_FS_BLOCKS(fs);
			disk_ptr->avail = SG_FS_FRSIZE(fs) * SG_FS_BAVAIL(fs);
			disk_ptr->used  = (disk_ptr->size) - (SG_FS_FRSIZE(fs) * SG_FS_BFREE(fs));
		
			disk_ptr->total_inodes = SG_FS_FILES(fs);
			disk_ptr->free_inodes  = SG_FS_FFREE(fs);
			/* Linux, FreeBSD don't have a "available" inodes */
			disk_ptr->used_inodes  = disk_ptr->total_inodes - disk_ptr->free_inodes;
			disk_ptr->avail_inodes = SG_FS_FAVAIL(fs);

			disk_ptr->io_size      = SG_FS_BSIZE(fs);
			disk_ptr->block_size   = SG_FS_FRSIZE(fs);
			disk_ptr->total_blocks = SG_FS_BLOCKS(fs);
			disk_ptr->free_blocks  = SG_FS_BFREE(fs);
			disk_ptr->avail_blocks = SG_FS_BAVAIL(fs);
			disk_ptr->used_blocks  = disk_ptr->total_blocks - disk_ptr->free_blocks;
#else
			if(!GetVolumeInformation(drive, volume_name_buf, BUFSIZE,
						NULL, NULL, NULL, 
						filesys_name_buf, BUFSIZE)) {
				sg_set_error_with_errno(SG_ERROR_DISKINFO, 
					"GetVolumeInformation");
				return NULL;
			}

			if (sg_update_string(&disk_ptr->device_name, 
						volume_name_buf) < 0) {
				return NULL;
			}
			if (sg_update_string(&disk_ptr->fs_type, 
						filesys_name_buf) < 0) {
				return NULL;
			}
			if (sg_update_string(&disk_ptr->mnt_point,
						drive) < 0) {
				return NULL;
			}
			if (!GetDiskFreeSpaceEx(drive, NULL,
					(PULARGE_INTEGER)&disk_ptr->size,
					(PULARGE_INTEGER)&disk_ptr->avail)) {
				sg_set_error_with_errno(SG_ERROR_DISKINFO,
					"GetDiskFreeSpaceEx");
				return NULL;
			}
			disk_ptr->used = disk_ptr->size - disk_ptr->avail;
			disk_ptr->total_inodes = 0;
			disk_ptr->free_inodes  = 0;
			disk_ptr->used_inodes  = 0;
			disk_ptr->avail_inodes = 0;

			/* I dunno what to do with these... so have nothing */
			disk_ptr->io_size = 0;
			disk_ptr->block_size = 0;
			disk_ptr->total_blocks = 0;
			disk_ptr->free_blocks = 0;
			disk_ptr->avail_blocks = 0;
			disk_ptr->used_blocks = 0;
#endif
			num_disks++;
		}
#ifdef WIN32
		while(*p++);
	} while(*p);
#else
	}
#endif

	*entries=num_disks;	

	/* If this fails, there is very little i can do about it, so
	   I'll ignore it :) */
#if defined(LINUX) || defined(CYGWIN) || defined(HPUX)
	endmntent(f);
#endif
#if defined(SOLARIS)
	fclose(f);
#endif

	return disk_stats;

}

int sg_fs_compare_device_name(const void *va, const void *vb) {
	const sg_fs_stats *a = (const sg_fs_stats *)va;
	const sg_fs_stats *b = (const sg_fs_stats *)vb;

	return strcmp(a->device_name, b->device_name);
}

int sg_fs_compare_mnt_point(const void *va, const void *vb) {
	const sg_fs_stats *a = (const sg_fs_stats *)va;
	const sg_fs_stats *b = (const sg_fs_stats *)vb;

	return strcmp(a->mnt_point, b->mnt_point);
}

static void diskio_stat_init(sg_disk_io_stats *d) {
	d->disk_name = NULL;
}

static void diskio_stat_destroy(sg_disk_io_stats *d) {
	free(d->disk_name);
}

VECTOR_DECLARE_STATIC(diskio_stats, sg_disk_io_stats, 10,
		      diskio_stat_init, diskio_stat_destroy);

#ifdef LINUX
typedef struct {
	int major;
	int minor;
} partition;
#endif

sg_disk_io_stats *sg_get_disk_io_stats(int *entries){
	int num_diskio;
#ifndef LINUX
	sg_disk_io_stats *diskio_stats_ptr;
#endif

#ifdef HPUX
	long long rbytes = 0, wbytes = 0;
	struct dirent *dinfo = NULL;
	struct stat lstatinfo;
	struct pst_diskinfo pstat_diskinfo[DISK_BATCH];
	char fullpathbuf[1024] = {0};
	dev_t diskid;
	DIR *dh = NULL;
	int diskidx = 0;
	int num, i;
#endif
#ifdef SOLARIS
	kstat_ctl_t *kc;
	kstat_t *ksp;
	kstat_io_t kios;
#endif
#ifdef LINUX
	FILE *f;
	char *line_ptr;
	int major, minor;
	int has_pp_stats = 1;
	VECTOR_DECLARE_STATIC(parts, partition, 16, NULL, NULL);
	int i, n;
	time_t now;
	const char *format;
	static regex_t not_part_re, part_re;
	static int re_compiled = 0;
#endif
#if defined(FREEBSD) || defined(DFBSD)
	static struct statinfo stats;
	static int stats_init = 0;
	int counter;
	struct device_selection *dev_sel = NULL;
	int n_selected, n_selections;
	long sel_gen;
	struct devstat *dev_ptr;
#endif
#ifdef NETBSD
	struct disk_sysctl *stats;
#endif
#ifdef OPENBSD
	int diskcount;
	char *disknames, *name, *bufpp;
	char **dk_name;
	struct diskstats *stats;
#endif
#ifdef NETBSD
#define MIBSIZE 3
#endif
#ifdef OPENBSD
#define MIBSIZE 2
#endif
#if defined(NETBSD) || defined(OPENBSD)
	int num_disks, i;
	int mib[MIBSIZE];
	size_t size;
#endif
#ifdef WIN32
	char *name;
	long long rbytes;
	long long wbytes;
#endif

	num_diskio=0;

#ifdef HPUX
	while (1) {
		num = pstat_getdisk(pstat_diskinfo, sizeof pstat_diskinfo[0],
		                    DISK_BATCH, diskidx);
		if (num == -1) {
			sg_set_error_with_errno(SG_ERROR_PSTAT,
			                        "pstat_getdisk");
			return NULL;
		} else if (num == 0) {
			break;
		}

		for (i = 0; i < num; i++) {
			struct pst_diskinfo *di = &pstat_diskinfo[i];

			/* Skip "disabled" disks. */
			if (di->psd_status == 0) {
				continue;
			}
	
			/* We can't seperate the reads from the writes, we'll
			 * just give the same to each. (This value is in
			 * 64-byte chunks according to the pstat header file,
			 * and can wrap to be negative.)
			 */
			rbytes = wbytes = ((unsigned long) di->psd_dkwds) * 64LL;
	
			/* Skip unused disks. */
			if (rbytes == 0 && wbytes == 0) {
				continue;
			}
	
			if (VECTOR_RESIZE(diskio_stats, num_diskio + 1) < 0) {
				return NULL;
			}
	
			diskio_stats_ptr = diskio_stats + num_diskio;
	
			diskio_stats_ptr->read_bytes = rbytes;
			diskio_stats_ptr->write_bytes = wbytes;
	
			diskio_stats_ptr->systime = time(NULL);
	
			num_diskio++;
	
			/* FIXME This should use a static cache, like the Linux
			 * code below. */
			if (diskio_stats_ptr->disk_name == NULL) {
				dh = opendir("/dev/dsk");
				if (dh == NULL) {
					continue;
				}
	
				diskid = (di->psd_dev.psd_major << 24) | di->psd_dev.psd_minor;
				while (1) {
					dinfo = readdir(dh);
					if (dinfo == NULL) {
						break;
					}
					snprintf(fullpathbuf, sizeof(fullpathbuf), "/dev/dsk/%s", dinfo->d_name);
					if (lstat(fullpathbuf, &lstatinfo) < 0) {
						continue;
					}
	
					if (lstatinfo.st_rdev == diskid) {
						if (sg_update_string(&diskio_stats_ptr->disk_name, dinfo->d_name) < 0) {
							return NULL;
						}
						break;
					}
				}
				closedir(dh);
	
				if (diskio_stats_ptr->disk_name == NULL) {
					if (sg_update_string(&diskio_stats_ptr->disk_name, di->psd_hw_path.psh_name) < 0) {
						return NULL;
					}
				}
			}
		}
		diskidx = pstat_diskinfo[num - 1].psd_idx + 1;
	}
#endif
#ifdef OPENBSD
	mib[0] = CTL_HW;
	mib[1] = HW_DISKCOUNT;

	size = sizeof(diskcount);
	if (sysctl(mib, MIBSIZE, &diskcount, &size, NULL, 0) < 0) {
		sg_set_error_with_errno(SG_ERROR_SYSCTL, "CTL_HW.HW_DISKCOUNT");
		return NULL;
	}

	mib[0] = CTL_HW;
	mib[1] = HW_DISKNAMES;

	if (sysctl(mib, MIBSIZE, NULL, &size, NULL, 0) < 0) {
		sg_set_error_with_errno(SG_ERROR_SYSCTL, "CTL_HW.HW_DISKNAMES");
		return NULL;
	}

	disknames = sg_malloc(size);
	if (disknames == NULL) {
		return NULL;
	}

	if (sysctl(mib, MIBSIZE, disknames, &size, NULL, 0) < 0) {
		sg_set_error_with_errno(SG_ERROR_SYSCTL, "CTL_HW.HW_DISKNAMES");
		return NULL;
	}

	dk_name = sg_malloc(diskcount * sizeof(char *));
	bufpp = disknames;
	for (i = 0; i < diskcount && (name = strsep(&bufpp, ",")) != NULL; i++) {
		dk_name[i] = name;
	}
#endif

#if defined(NETBSD) || defined(OPENBSD)
	mib[0] = CTL_HW;
	mib[1] = HW_DISKSTATS;
#ifdef NETBSD
	mib[2] = sizeof(struct disk_sysctl);
#endif

	if (sysctl(mib, MIBSIZE, NULL, &size, NULL, 0) < 0) {
		sg_set_error_with_errno(SG_ERROR_SYSCTL, "CTL_HW.HW_DISKSTATS");
		return NULL;
	}

#ifdef NETBSD
	num_disks = size / sizeof(struct disk_sysctl);
#else
	num_disks = size / sizeof(struct diskstats);
#endif

	stats = sg_malloc(size);
	if (stats == NULL) {
		return NULL;
	}

	if (sysctl(mib, MIBSIZE, stats, &size, NULL, 0) < 0) {
		sg_set_error_with_errno(SG_ERROR_SYSCTL, "CTL_HW.HW_DISKSTATS");
		return NULL;
	}

	for (i = 0; i < num_disks; i++) {
		const char *name;
		u_int64_t rbytes, wbytes;

#ifdef NETBSD
#ifdef HAVE_DK_RBYTES
		rbytes = stats[i].dk_rbytes;
		wbytes = stats[i].dk_wbytes;
#else
		/* Before 2.0, NetBSD merged reads and writes. */
		rbytes = wbytes = stats[i].dk_bytes;
#endif
#else
#ifdef HAVE_DS_RBYTES
		rbytes = stats[i].ds_rbytes;
		wbytes = stats[i].ds_wbytes;
#else
		/* Before 3.5, OpenBSD merged reads and writes */
		rbytes = wbytes = stats[i].ds_bytes;
#endif
#endif

		/* Don't keep stats for disks that have never been used. */
		if (rbytes == 0 && wbytes == 0) {
			continue;
		}

		if (VECTOR_RESIZE(diskio_stats, num_diskio + 1) < 0) {
			return NULL;
		}
		diskio_stats_ptr = diskio_stats + num_diskio;
		
		diskio_stats_ptr->read_bytes = rbytes;
		diskio_stats_ptr->write_bytes = wbytes;
#ifdef NETBSD
		name = stats[i].dk_name;
#else
		name = dk_name[i];
#endif
		if (sg_update_string(&diskio_stats_ptr->disk_name, name) < 0) {
			return NULL;
		}
		diskio_stats_ptr->systime = time(NULL);
	
		num_diskio++;	
	}

	free(stats);
#ifdef OPENBSD
	free(dk_name);
	free(disknames);
#endif
#endif

#if defined(FREEBSD) || defined(DFBSD)
	if (!stats_init) {
		stats.dinfo=sg_malloc(sizeof(struct devinfo));
		if(stats.dinfo==NULL) return NULL;
		bzero(stats.dinfo, sizeof(struct devinfo));
		stats_init = 1;
	}
#ifdef FREEBSD5
	if ((devstat_getdevs(NULL, &stats)) < 0) {
		/* FIXME devstat functions return a string error in
		   devstat_errbuf */
		sg_set_error(SG_ERROR_DEVSTAT_GETDEVS, NULL);
		return NULL;
	}
	/* Not aware of a get all devices, so i said 999. If we ever
	 * find a machine with more than 999 disks, then i'll change
	 * this number :)
	 */
	if (devstat_selectdevs(&dev_sel, &n_selected, &n_selections, &sel_gen, stats.dinfo->generation, stats.dinfo->devices, stats.dinfo->numdevs, NULL, 0, NULL, 0, DS_SELECT_ONLY, 999, 1) < 0) {
		sg_set_error(SG_ERROR_DEVSTAT_SELECTDEVS, NULL);
		return NULL;
	}
#else
	if ((getdevs(&stats)) < 0) {
		sg_set_error(SG_ERROR_DEVSTAT_GETDEVS, NULL);
		return NULL;
	}
	/* Not aware of a get all devices, so i said 999. If we ever
	 * find a machine with more than 999 disks, then i'll change
	 * this number :)
	 */
	if (selectdevs(&dev_sel, &n_selected, &n_selections, &sel_gen, stats.dinfo->generation, stats.dinfo->devices, stats.dinfo->numdevs, NULL, 0, NULL, 0, DS_SELECT_ONLY, 999, 1) < 0) {
		sg_set_error(SG_ERROR_DEVSTAT_SELECTDEVS, NULL);
		return NULL;
	}
#endif

	for(counter=0;counter<stats.dinfo->numdevs;counter++){
		dev_ptr=&stats.dinfo->devices[dev_sel[counter].position];

		/* Throw away devices that have done nothing, ever.. Eg "odd" 
		 * devices.. like mem, proc.. and also doesn't report floppy
		 * drives etc unless they are doing stuff :)
		 */
#ifdef FREEBSD5
		if((dev_ptr->bytes[DEVSTAT_READ]==0) && (dev_ptr->bytes[DEVSTAT_WRITE]==0)) continue;
#else
		if((dev_ptr->bytes_read==0) && (dev_ptr->bytes_written==0)) continue;
#endif

		if (VECTOR_RESIZE(diskio_stats, num_diskio + 1) < 0) {
			return NULL;
		}
		diskio_stats_ptr=diskio_stats+num_diskio;

#ifdef FREEBSD5		
		diskio_stats_ptr->read_bytes=dev_ptr->bytes[DEVSTAT_READ];
		diskio_stats_ptr->write_bytes=dev_ptr->bytes[DEVSTAT_WRITE];
#else
		diskio_stats_ptr->read_bytes=dev_ptr->bytes_read;
		diskio_stats_ptr->write_bytes=dev_ptr->bytes_written;
#endif
		if(diskio_stats_ptr->disk_name!=NULL) free(diskio_stats_ptr->disk_name);
		if (asprintf((&diskio_stats_ptr->disk_name), "%s%d", dev_ptr->device_name, dev_ptr->unit_number) == -1) {
			sg_set_error_with_errno(SG_ERROR_ASPRINTF, NULL);
			return NULL;
		}
		diskio_stats_ptr->systime=time(NULL);

		num_diskio++;
	}
	free(dev_sel);

#endif
#ifdef SOLARIS
	if ((kc = kstat_open()) == NULL) {
		sg_set_error(SG_ERROR_KSTAT_OPEN, NULL);
		return NULL;
	}

	for (ksp = kc->kc_chain; ksp; ksp = ksp->ks_next) {
		if (!strcmp(ksp->ks_class, "disk")) {

			if(ksp->ks_type != KSTAT_TYPE_IO) continue;
			/* We dont want metadevices appearins as num_diskio */
			if(strcmp(ksp->ks_module, "md")==0) continue;
			if((kstat_read(kc, ksp, &kios))==-1){	
			}
			
			if (VECTOR_RESIZE(diskio_stats, num_diskio + 1) < 0) {
				kstat_close(kc);
				return NULL;
			}
			diskio_stats_ptr=diskio_stats+num_diskio;
			
			diskio_stats_ptr->read_bytes=kios.nread;
			diskio_stats_ptr->write_bytes=kios.nwritten;
			if (sg_update_string(&diskio_stats_ptr->disk_name,
					     sg_get_svr_from_bsd(ksp->ks_name)) < 0) {
				kstat_close(kc);
				return NULL;
			}
			diskio_stats_ptr->systime=time(NULL);

			num_diskio++;
		}
	}

	kstat_close(kc);
#endif

#ifdef LINUX
	num_diskio = 0;
	n = 0;

	/* Read /proc/partitions to find what devices exist. Recent 2.4 kernels
	   have statistics in here too, so we can use those directly.
	   2.6 kernels have /proc/diskstats instead with almost (but not quite)
	   the same format. */

	f = fopen("/proc/diskstats", "r");
	format = " %d %d %99s %*d %*d %lld %*d %*d %*d %lld";
	if (f == NULL) {
		f = fopen("/proc/partitions", "r");
		format = " %d %d %*d %99s %*d %*d %lld %*d %*d %*d %lld";
	}
	if (f == NULL) goto out;
	now = time(NULL);

	if (!re_compiled) {
		if (regcomp(&part_re, "^(.*/)?[^/]*[0-9]$", REG_EXTENDED | REG_NOSUB) != 0) {
			sg_set_error(SG_ERROR_PARSE, NULL);
			goto out;
		}
		if (regcomp(&not_part_re, "^(.*/)?[^/0-9]+[0-9]+d[0-9]+$", REG_EXTENDED | REG_NOSUB) != 0) {
			sg_set_error(SG_ERROR_PARSE, NULL);
			goto out;
		}
		re_compiled = 1;
	}

	while ((line_ptr = sg_f_read_line(f, "")) != NULL) {
		char name[100];
		long long rsect, wsect;

		int nr = sscanf(line_ptr, format,
			&major, &minor, name, &rsect, &wsect);
		if (nr < 3) continue;

		/* Skip device names ending in numbers, since they're
		   partitions, unless they match the c0d0 pattern that some
		   RAID devices use. */
		/* FIXME: For 2.6+, we should probably be using sysfs to detect
		   this... */
		if ((regexec(&part_re, name, 0, NULL, 0) == 0)
		    && (regexec(&not_part_re, name, 0, NULL, 0) != 0)) {
			continue;
		}

		if (nr < 5) {
			has_pp_stats = 0;
			rsect = 0;
			wsect = 0;
		}

		if (VECTOR_RESIZE(diskio_stats, n + 1) < 0) {
			goto out;
		}
		if (VECTOR_RESIZE(parts, n + 1) < 0) {
			goto out;
		}

		if (sg_update_string(&diskio_stats[n].disk_name, name) < 0) {
			goto out;
		}
		diskio_stats[n].read_bytes = rsect * 512;
		diskio_stats[n].write_bytes = wsect * 512;
		diskio_stats[n].systime = now;
		parts[n].major = major;
		parts[n].minor = minor;

		n++;
	}

	fclose(f);
	f = NULL;

	if (!has_pp_stats) {
		/* This is an older kernel where /proc/partitions doesn't
		   contain stats. Read what we can from /proc/stat instead, and
		   fill in the appropriate bits of the list allocated above. */

		f = fopen("/proc/stat", "r");
		if (f == NULL) goto out;
		now = time(NULL);

		line_ptr = sg_f_read_line(f, "disk_io:");
		if (line_ptr == NULL) goto out;

		while((line_ptr=strchr(line_ptr, ' '))!=NULL){
			long long rsect, wsect;

			if (*++line_ptr == '\0') break;

			if((sscanf(line_ptr,
				"(%d,%d):(%*d, %*d, %lld, %*d, %lld)",
				&major, &minor, &rsect, &wsect)) != 4) {
					continue;
			}

			/* Find the corresponding device from earlier.
			   Just to add to the fun, "minor" is actually the disk
			   number, not the device minor, so we need to figure
			   out the real minor number based on the major!
			   This list is not exhaustive; if you're running
			   an older kernel you probably don't have fancy
			   I2O hardware anyway... */
			switch (major) {
			case 3:
			case 21:
			case 22:
			case 33:
			case 34:
			case 36:
			case 56:
			case 57:
			case 88:
			case 89:
			case 90:
			case 91:
				minor *= 64;
				break;
			case 9:
			case 43:
				break;
			default:
				minor *= 16;
				break;
			}
			for (i = 0; i < n; i++) {
				if (major == parts[i].major
					&& minor == parts[i].minor)
					break;
			}
			if (i == n) continue;

			/* We read the number of blocks. Blocks are stored in
			   512 bytes */
			diskio_stats[i].read_bytes = rsect * 512;
			diskio_stats[i].write_bytes = wsect * 512;
			diskio_stats[i].systime = now;
		}
	}

	num_diskio = n;
out:
	if (f != NULL) fclose(f);
#endif

#ifdef CYGWIN
	sg_set_error(SG_ERROR_UNSUPPORTED, "Cygwin");
	return NULL;
#endif

#ifdef WIN32
	sg_set_error(SG_ERROR_NONE, NULL);

	while((name = get_diskio(num_diskio, &rbytes, &wbytes)) != NULL) {
		if (VECTOR_RESIZE(diskio_stats, num_diskio+1)) {
			return NULL;
		}

		diskio_stats_ptr = diskio_stats + num_diskio;

		if (sg_update_string(&diskio_stats_ptr->disk_name, name) < 0) {
			return NULL;
		}
		sg_update_string(&name, NULL);
		diskio_stats_ptr->read_bytes = rbytes;
		diskio_stats_ptr->write_bytes = wbytes;

		diskio_stats_ptr->systime = 0;

		num_diskio++;
	}
#endif

	*entries=num_diskio;

	return diskio_stats;
}

sg_disk_io_stats *sg_get_disk_io_stats_diff(int *entries){
#ifndef WIN32
	VECTOR_DECLARE_STATIC(diff, sg_disk_io_stats, 1,
			      diskio_stat_init, diskio_stat_destroy);
	sg_disk_io_stats *src = NULL, *dest;
	int i, j, diff_count, new_count;

	if (diskio_stats == NULL) {
		/* No previous stats, so we can't calculate a difference. */
		return sg_get_disk_io_stats(entries);
	}

	/* Resize the results array to match the previous stats. */
	diff_count = VECTOR_SIZE(diskio_stats);
	if (VECTOR_RESIZE(diff, diff_count) < 0) {
		return NULL;
	}

	/* Copy the previous stats into the result. */
	for (i = 0; i < diff_count; i++) {
		src = &diskio_stats[i];
		dest = &diff[i];

		if (sg_update_string(&dest->disk_name, src->disk_name) < 0) {
			return NULL;
		}
		dest->read_bytes = src->read_bytes;
		dest->write_bytes = src->write_bytes;
		dest->systime = src->systime;
	}

	/* Get a new set of stats. */
	if (sg_get_disk_io_stats(&new_count) == NULL) {
		return NULL;
	}

	/* For each previous stat... */
	for (i = 0; i < diff_count; i++) {
		dest = &diff[i];

		/* ... find the corresponding new stat ... */
		for (j = 0; j < new_count; j++) {
			/* Try the new stat in the same position first,
			   since that's most likely to be it. */
			src = &diskio_stats[(i + j) % new_count];
			if (strcmp(src->disk_name, dest->disk_name) == 0) {
				break;
			}
		}
		if (j == new_count) {
			/* No match found. */
			continue;
		}

		/* ... and subtract the previous stat from it to get the
		   difference. */
		dest->read_bytes = src->read_bytes - dest->read_bytes;
		dest->write_bytes = src->write_bytes - dest->write_bytes;
		dest->systime = src->systime - dest->systime;
	}

	*entries = diff_count;
	return diff;
#else /* WIN32 */
	return sg_get_disk_io_stats(entries);
#endif
}

int sg_disk_io_compare_name(const void *va, const void *vb) {
	const sg_disk_io_stats *a = (const sg_disk_io_stats *)va;
	const sg_disk_io_stats *b = (const sg_disk_io_stats *)vb;

	return strcmp(a->disk_name, b->disk_name);
}

