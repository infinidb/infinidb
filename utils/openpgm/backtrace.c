/* vim:ts=8:sts=8:sw=4:noai:noexpandtab
 *
 * Dump back trace to stderr and try gdb.
 *
 * Copyright (c) 2006-2007 Miru Limited.
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
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */


#include <execinfo.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>

#include <glib.h>

#include "pgm/backtrace.h"


/* globals */

void
on_sigsegv (
	G_GNUC_UNUSED int	signum
	)
{
	void* array[256];
	char** names;
	char cmd[1024];
	int i, size;
	gchar *out, *err;
	gint exit_status;

	fprintf (stderr, "\n======= Backtrace: =========\n");

	size = backtrace (array, G_N_ELEMENTS(array));
	names = backtrace_symbols (array, size);

	for (i = 0; i < size; i++)
		fprintf (stderr, "%s\n", names[i]);

	free (names);
	fflush (stderr);

#ifndef G_PLATFORM_WIN32
	sprintf (cmd, "gdb --ex 'attach %ld' --ex 'info threads' --ex 'thread apply all bt' --batch", (long)getpid ());
	if ( g_spawn_command_line_sync (cmd, &out, &err, &exit_status, NULL) )
	{
		fprintf (stderr, "======= GDB Backtrace: =========\n");
		fprintf (stderr, "%s\n", out);
	}
#endif

	abort ();
}

/* eof */

