/* vim:ts=8:sts=8:sw=4:noai:noexpandtab
 *
 * Re-entrant safe signal handling.
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

#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <string.h>
#include <unistd.h>

#include <glib.h>

#include "pgm/signal.h"


/* globals */

static pgm_sighandler_t g_signal_list[NSIG];
static int g_signal_pipe[2];
static GIOChannel* g_signal_io = NULL;

static void on_signal (int);
static gboolean on_io_signal (GIOChannel*, GIOCondition, gpointer);


/* install signal handler and return unix fd to add to event loop
 */

pgm_sighandler_t
pgm_signal_install (
	int		signum,
	pgm_sighandler_t	handler
	)
{
	if (g_signal_io == NULL)
	{
		if (pipe (g_signal_pipe))
		{
			return SIG_ERR;
		}

/* write-end */
		int fd_flags = fcntl (g_signal_pipe[1], F_GETFL);
		if (fd_flags < 0)
		{
			return SIG_ERR;
		}
		if (fcntl (g_signal_pipe[1], F_SETFL, fd_flags | O_NONBLOCK))
		{
			return SIG_ERR;
		}

/* read-end */
		fd_flags = fcntl (g_signal_pipe[0], F_GETFL);
		if (fd_flags < 0)
		{
			return SIG_ERR;
		}
		if (fcntl (g_signal_pipe[0], F_SETFL, fd_flags | O_NONBLOCK))
		{
			return SIG_ERR;
		}

/* add to evm */
		g_signal_io = g_io_channel_unix_new (g_signal_pipe[0]);
		g_io_add_watch (g_signal_io, G_IO_IN, on_io_signal, NULL);
	}

	g_signal_list[signum] = handler;
	return signal (signum, on_signal);
}

/* process signal from operating system
 */

static void
on_signal (
	int		signum
	)
{
	if (write (g_signal_pipe[1], &signum, sizeof(signum)) != sizeof(signum))
	{
		g_critical ("Unix signal %s (%i) lost", strsignal(signum), signum);
	}
}

/* process signal from pipe
 */

static gboolean
on_io_signal (
	GIOChannel*	source,
	G_GNUC_UNUSED GIOCondition	cond,
	G_GNUC_UNUSED gpointer		user_data
	)
{
	int signum;
	gsize bytes_read;

	bytes_read = read (g_io_channel_unix_get_fd (source), &signum, sizeof(signum));

	if (bytes_read == sizeof(signum))
	{
		g_signal_list[signum] (signum);
	}
	else
	{
		g_critical ("Lost data in signal pipe, read %" G_GSIZE_FORMAT " byte%s expected %" G_GSIZE_FORMAT ".",
				bytes_read, bytes_read > 1 ? "s" : "", sizeof(signum));
	}

	return TRUE;
}

/* eof */

