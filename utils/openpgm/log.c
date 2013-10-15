/* vim:ts=8:sts=8:sw=4:noai:noexpandtab
 *
 * basic logging.
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


#include <stdio.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/time.h>
#include <netdb.h>

#include "pgm/log.h"


/* globals */

#define TIME_FORMAT		"%Y-%m-%d %H:%M:%S "

static int g_timezone = 0;
static char g_hostname[NI_MAXHOST + 1];

static void log_handler (const gchar*, GLogLevelFlags, const gchar*, gpointer);


/* calculate time zone offset in seconds
 */

gboolean
log_init ( void )
{
/* time zone offset */
	time_t t = time(NULL);
	struct tm sgmt, *gmt = &sgmt;
	*gmt = *gmtime(&t);
	struct tm* loc = localtime(&t);
	g_timezone = (loc->tm_hour - gmt->tm_hour) * 60 * 60 +
		     (loc->tm_min  - gmt->tm_min) * 60;
	int dir = loc->tm_year - gmt->tm_year;
	if (!dir) dir = loc->tm_yday - gmt->tm_yday;
	g_timezone += dir * 24 * 60 * 60;

//	printf ("timezone offset %u seconds.\n", g_timezone);

	gethostname (g_hostname, sizeof(g_hostname));

	g_log_set_handler ("Pgm",		G_LOG_LEVEL_MASK, log_handler, NULL);
	g_log_set_handler ("Pgm-Http",		G_LOG_LEVEL_MASK, log_handler, NULL);
	g_log_set_handler ("Pgm-Snmp",		G_LOG_LEVEL_MASK, log_handler, NULL);
	g_log_set_handler (NULL,		G_LOG_LEVEL_MASK, log_handler, NULL);

	return 0;
}

/* log callback
 */
static void
log_handler (
	const gchar*	log_domain,
	G_GNUC_UNUSED GLogLevelFlags	log_level,
	const gchar*	message,
	G_GNUC_UNUSED gpointer		unused_data
	)
{
	struct iovec iov[7];
	struct iovec* v = iov;
	time_t now;
	time (&now);
	const struct tm* time_ptr = localtime(&now);
	char tbuf[1024];
	strftime(tbuf, sizeof(tbuf), TIME_FORMAT, time_ptr);
	v->iov_base = tbuf;
	v->iov_len = strlen(tbuf);
	v++;
	v->iov_base = g_hostname;
	v->iov_len = strlen(g_hostname);
	v++;

	if (log_domain) {
		v->iov_base = " ";
		v->iov_len = 1;
		v++;
		v->iov_base = log_domain;
		v->iov_len = strlen(log_domain);
		v++;
	}

	v->iov_base = ": ";
	v->iov_len = 2;
	v++;
	v->iov_base = message;
	v->iov_len = strlen(message);
	v++;
	v->iov_base = "\n";
	v->iov_len = 1;
	v++;

	writev (STDOUT_FILENO, iov, v - iov);
}

/* eof */
