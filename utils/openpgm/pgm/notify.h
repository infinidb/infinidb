/* vim:ts=8:sts=4:sw=4:noai:noexpandtab
 * 
 * Low kernel overhead event notify mechanism, or standard pipes.
 *
 * Copyright (c) 2008 Miru Limited.
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

#ifndef __PGM_NOTIFY_H__
#define __PGM_NOTIFY_H__

#ifdef CONFIG_HAVE_EVENTFD
#	include <sys/eventfd.h>
#endif
#include <unistd.h>
#include <fcntl.h>

#include <glib.h>


G_BEGIN_DECLS

struct pgm_notify_t {
#ifdef CONFIG_HAVE_EVENTFD
	int eventfd;
#else
	int pipefd[2];
#endif /* CONFIG_HAVE_EVENTFD */
};

typedef struct pgm_notify_t pgm_notify_t;

static inline int pgm_notify_init (pgm_notify_t* notify)
{
	g_assert (notify);

#ifdef CONFIG_HAVE_EVENTFD
	int retval = eventfd (0, 0);
	if (-1 == retval) {
		return retval;
	}
	notify->eventfd = retval;
	int fd_flags = fcntl (notify->eventfd, F_GETFL);
	if (fd_flags != -1) {
		retval = fcntl (notify->eventfd, F_SETFL, fd_flags | O_NONBLOCK);
	}
	return 0;
#else
	int retval = pipe (notify->pipefd);
	g_assert (0 == retval);

/* set non-blocking */
/* write-end */
	int fd_flags = fcntl (notify->pipefd[1], F_GETFL);
	if (fd_flags != -1) {
		retval = fcntl (notify->pipefd[1], F_SETFL, fd_flags | O_NONBLOCK);
	}
	g_assert (notify->pipefd[1]);
/* read-end */
	fd_flags = fcntl (notify->pipefd[0], F_GETFL);
	if (fd_flags != -1) {
		retval = fcntl (notify->pipefd[0], F_SETFL, fd_flags | O_NONBLOCK);
	}
	g_assert (notify->pipefd[0]);
	return retval;
#endif
}

static inline int pgm_notify_destroy (pgm_notify_t* notify)
{
	g_assert (notify);

#ifdef CONFIG_HAVE_EVENTFD
	if (notify->eventfd) {
		close (notify->eventfd);
		notify->eventfd = 0;
	}
#else
	if (notify->pipefd[0]) {
		close (notify->pipefd[0]);
		notify->pipefd[0] = 0;
	}
	if (notify->pipefd[1]) {
		close (notify->pipefd[1]);
		notify->pipefd[1] = 0;
	}
#endif
	return 0;
}

static inline int pgm_notify_send (pgm_notify_t* notify)
{
	g_assert (notify);

#ifdef CONFIG_HAVE_EVENTFD
	g_assert (notify->eventfd);
	uint64_t u = 1;
	ssize_t s = write (notify->eventfd, &u, sizeof(u));
	return (s == sizeof(u));
#else
	g_assert (notify->pipefd[1]);
	const char one = '1';
	return (1 == write (notify->pipefd[1], &one, sizeof(one)));
#endif
}

static inline int pgm_notify_read (pgm_notify_t* notify)
{
	g_assert (notify);

#ifdef CONFIG_HAVE_EVENTFD
	g_assert (notify->eventfd);
	uint64_t u;
	return (sizeof(u) == read (notify->eventfd, &u, sizeof(u)));
#else
	g_assert (notify->pipefd[0]);
	char buf;
	return (sizeof(buf) == read (notify->pipefd[0], &buf, sizeof(buf)));
#endif
}

static inline void pgm_notify_clear (pgm_notify_t* notify)
{
	g_assert (notify);

#ifdef CONFIG_HAVE_EVENTFD
	g_assert (notify->eventfd);
	uint64_t u;
	while (sizeof(u) == read (notify->eventfd, &u, sizeof(u)));
#else
	g_assert (notify->pipefd[0]);
	char buf;
	while (sizeof(buf) == read (notify->pipefd[0], &buf, sizeof(buf)));
#endif
}

static inline int pgm_notify_get_fd (pgm_notify_t* notify)
{
	g_assert (notify);

#ifdef CONFIG_HAVE_EVENTFD
	g_assert (notify->eventfd);
	return notify->eventfd;
#else
	g_assert (notify->pipefd[0]);
	return notify->pipefd[0];
#endif
}

G_END_DECLS

#endif /* __PGM_NOTIFY_H__ */
