/* Copyright (C) 2014 InfiniDB, Inc.
   Copyright (C) 1998-2003 Daniel Veillard

   This program is free software; you can redistribute it and/or
   modify it under the terms of the GNU General Public License
   as published by the Free Software Foundation; version 2 of
   the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. */

#include <stdio.h>
#include <stdlib.h>
#include <tchar.h>
#include <io.h>
#include <string.h>

#include "glob.h"

#define ALLOC_SIZE 128

int glob(const char *pattern, int flags,
                int errfunc(const char *epath, int eerrno),
                glob_t *pglob)
{
	size_t nalloc;
	intptr_t file; 
	struct _finddata_t filedata; 

	if (!(flags & GLOB_APPEND))
	{
		pglob->gl_pathv = NULL;
		pglob->gl_alloccnt = 0;
		pglob->gl_pathc = 0;
	}

	if (pglob->gl_alloccnt == 0)
	{
		nalloc = pglob->gl_offs / ALLOC_SIZE;
		if ((nalloc == 0) || ((pglob->gl_offs % ALLOC_SIZE) != 0))
			nalloc++;
		nalloc *= ALLOC_SIZE;
		nalloc++;
		pglob->gl_pathv = calloc(nalloc, sizeof(char*));
		if (pglob->gl_pathv == NULL)
			return GLOB_NOSPACE;
		pglob->gl_alloccnt = nalloc;
	}

	pglob->gl_pathc += pglob->gl_offs;

	file = _findfirst(pattern,&filedata);
	if (file != -1)
	{
		do
		{
			if (!strcmp(filedata.name, ".") ||
				!strcmp(filedata.name, ".."))
				continue;

			if (pglob->gl_pathc >= pglob->gl_alloccnt)
			{
				char** tmpv;
				nalloc = pglob->gl_alloccnt + ALLOC_SIZE;
				tmpv = calloc(nalloc, sizeof(char*));
				if (tmpv == NULL)
					return GLOB_NOSPACE;
				memcpy(tmpv, pglob->gl_pathv, pglob->gl_alloccnt * sizeof(char*));
				free(pglob->gl_pathv);
				pglob->gl_pathv = tmpv;
				pglob->gl_alloccnt = nalloc;
			}

			pglob->gl_pathv[pglob->gl_pathc] = _strdup(filedata.name);
			if (pglob->gl_pathv[pglob->gl_pathc] == NULL)
				return GLOB_NOSPACE;
			pglob->gl_pathc++;
		} while (_findnext(file,&filedata) == 0);
	}

	_findclose(file);

	return (pglob->gl_pathc > pglob->gl_offs ? 0 : GLOB_NOMATCH);
}

void globfree(glob_t *pglob)
{
	size_t i;

	if (pglob == NULL) return;

	for (i = 0; i < pglob->gl_pathc; i++)
		if (pglob->gl_pathv[i] != NULL)
			free(pglob->gl_pathv[i]);

	free(pglob->gl_pathv);
}
