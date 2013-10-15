/* vim:ts=8:sts=8:sw=4:noai:noexpandtab
 *
 * Rate regulation.
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
#include <glib.h>
#include <sys/types.h>
#include <sys/socket.h>

#include "pgm/timer.h"
#include "pgm/rate_control.h"


struct rate_t {
	gint rate_per_sec;
	guint iphdr_len;

	gint rate_limit;		/* signed for math */
	pgm_time_t last_rate_check;
	GStaticMutex mutex;
};

typedef struct rate_t rate_t;

/* globals */


int
pgm_rate_create (
	gpointer*	bucket_,
	guint		rate_per_sec,		/* 0 = disable */
	guint		iphdr_len
	)
{
	g_return_val_if_fail (bucket_ != NULL, -EINVAL);

	rate_t* bucket = g_malloc0 (sizeof(rate_t));
	bucket->rate_per_sec = (gint)rate_per_sec;
	bucket->iphdr_len = iphdr_len;
	g_static_mutex_init (&bucket->mutex);
	*bucket_ = bucket;

	return 0;
}

#define BUCKET	((rate_t*)bucket)

int
pgm_rate_destroy (
	gpointer	bucket
	)
{
	g_return_val_if_fail (bucket != NULL, -EINVAL);

	g_static_mutex_free (&BUCKET->mutex);
	g_free(bucket);

	return 0;
}

/* return when leaky bucket permits unless non-blocking.
 */

int
pgm_rate_check (
	gpointer	bucket,
	guint		data_size,
	int		flags		/* MSG_DONTWAIT = non-blocking */
	)
{
	if (!bucket || !(BUCKET->rate_per_sec > 0)) return 0;

	g_static_mutex_lock (&BUCKET->mutex);
	pgm_time_t time_since_last_rate_check = pgm_time_update_now() - BUCKET->last_rate_check;

	BUCKET->rate_limit += (double)BUCKET->rate_per_sec * (double)pgm_to_secs((double)time_since_last_rate_check);
/* per milli-second */
	if (BUCKET->rate_limit > (BUCKET->rate_per_sec / 1000)) 
		BUCKET->rate_limit = BUCKET->rate_per_sec / 1000;
	BUCKET->last_rate_check = pgm_time_now;

	gint new_rate_limit = BUCKET->rate_limit - ( BUCKET->iphdr_len + data_size );
	if (flags & MSG_DONTWAIT &&
		new_rate_limit < 0)
	{
		g_static_mutex_unlock (&BUCKET->mutex);
		errno = EAGAIN;
		return -1;
	}

	BUCKET->rate_limit = new_rate_limit;
	if (BUCKET->rate_limit < 0) {
		gint sleep_amount;
		do {
			g_thread_yield();
			time_since_last_rate_check = pgm_time_update_now() - BUCKET->last_rate_check;
			sleep_amount = (double)BUCKET->rate_per_sec * (double)pgm_to_secs((double)time_since_last_rate_check);
		} while (sleep_amount + BUCKET->rate_limit < 0);
		BUCKET->rate_limit += sleep_amount;
		BUCKET->last_rate_check = pgm_time_now;
	} 
	g_static_mutex_unlock (&BUCKET->mutex);

	return 0;
}

/* eof */
