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
 * $Id: user_stats.c,v 1.28 2005/09/24 13:29:23 tdb Exp $
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "statgrab.h"
#include "vector.h"
#ifdef ALLBSD
#include <sys/types.h>
#endif
#if defined(NETBSD) || defined(OPENBSD)
#include <limits.h>
#endif
#ifdef OPENBSD
#include <sys/param.h>
#endif
#ifndef WIN32
#include <utmp.h>
#endif
#ifdef CYGWIN
#include <sys/unistd.h>
#endif
#ifdef HPUX
#include <utmp.h>
#endif
#ifdef WIN32
#include <windows.h>
#include <lm.h>
#endif

sg_user_stats *sg_get_user_stats(){
	int num_users = 0, pos = 0, new_pos;
	VECTOR_DECLARE_STATIC(name_list, char, 128, NULL, NULL);
	static sg_user_stats user_stats;
#ifdef ALLBSD
	struct utmp entry;
	FILE *f;

	if ((f=fopen(_PATH_UTMP, "r")) == NULL){
		sg_set_error_with_errno(SG_ERROR_OPEN, _PATH_UTMP);
		return NULL;
	}
	while((fread(&entry, sizeof(entry),1,f)) != 0){
		if (entry.ut_name[0] == '\0') continue;

		new_pos = pos + strlen(entry.ut_name) + 1;
		if (VECTOR_RESIZE(name_list, new_pos) < 0) {
			return NULL;
		}

		strcpy(name_list + pos, entry.ut_name);
		name_list[new_pos - 1] = ' ';
		pos = new_pos;
		num_users++;
	}
	fclose(f);
#elif defined (WIN32)
	LPWKSTA_USER_INFO_0 buf = NULL;
	LPWKSTA_USER_INFO_0 tmp_buf;
	unsigned long entries_read = 0;
	unsigned long entries_tot = 0;
	unsigned long resumehandle = 0;
	NET_API_STATUS nStatus;
	int i;
	char name[256];

	do {
		nStatus = NetWkstaUserEnum(NULL, 0, (LPBYTE*)&buf,
				MAX_PREFERRED_LENGTH, &entries_read,
				&entries_tot, &resumehandle);
		if((nStatus == NERR_Success) || (nStatus == ERROR_MORE_DATA)) {
			if((tmp_buf = buf) == NULL) {
				continue;
			}
			for (i=0; i<entries_read; i++) {
				//assert(tmp_buf != NULL);
				if (tmp_buf == NULL) {
					sg_set_error(SG_ERROR_PERMISSION, "User list");
					break;
				}
				/* It's in unicode. We are not. Convert */
				WideCharToMultiByte(CP_ACP, 0, tmp_buf->wkui0_username, -1, name, sizeof(name), NULL, NULL);

				new_pos = pos + strlen(name) + 1;
				if(VECTOR_RESIZE(name_list, new_pos) < 0) {
					NetApiBufferFree(buf);
					return NULL;
				}
				strcpy(name_list + pos, name);
				name_list[new_pos - 1] = ' ';
				pos = new_pos;

				tmp_buf++;
				num_users++;
			}
		} else {
			sg_set_error(SG_ERROR_PERMISSION, "User enum");
			return NULL;
		}
		if (buf != NULL) {
			NetApiBufferFree(buf);
			buf=NULL;
		}
	} while (nStatus == ERROR_MORE_DATA);
	if (buf != NULL) {
		NetApiBufferFree(buf);
	}
#else
	/* This works on everything else. */
	struct utmp *entry;

	setutent();
	while((entry=getutent()) != NULL) {
		if (entry->ut_type != USER_PROCESS) continue;

		new_pos = pos + strlen(entry->ut_user) + 1;
		if (VECTOR_RESIZE(name_list, new_pos) < 0) {
			return NULL;
		}

		strcpy(name_list + pos, entry->ut_user);
		name_list[new_pos - 1] = ' ';
		pos = new_pos;
		num_users++;
	}
	endutent();
#endif

	/* Remove the extra space at the end, and append a \0. */
	if (num_users != 0) {
		pos--;
	}
	if (VECTOR_RESIZE(name_list, pos + 1) < 0) {
		return NULL;
	}
	name_list[pos] = '\0';

	user_stats.num_entries = num_users;
	user_stats.name_list = name_list;
	return &user_stats;
}
