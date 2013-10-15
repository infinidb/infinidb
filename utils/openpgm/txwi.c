/* vim:ts=8:sts=8:sw=4:noai:noexpandtab
 *
 * A basic transmit window: pointer array implementation.
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
#include <getopt.h>
#include <signal.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/time.h>

//#define TXW_DEBUG

#ifndef TXW_DEBUG
#define G_DISABLE_ASSERT
#endif

#include <glib.h>

#include "pgm/txwi.h"
#include "pgm/sn.h"

#ifndef TXW_DEBUG
#define g_trace(...)		while (0)
#else
#define g_trace(...)		g_debug(__VA_ARGS__)
#endif


#define ABS_IN_TXW(w,x) \
	( \
		!pgm_txw_empty ( (w) ) && \
		pgm_uint32_gte ( (x), (w)->trail ) && pgm_uint32_lte ( (x), (w)->lead ) \
	)

#define IN_TXW(w,x)	( pgm_uint32_gte ( (x), (w)->trail ) )

#define TXW_PACKET_OFFSET(w,x)		( (x) % pgm_txw_len( (w) ) )
#define TXW_PACKET(w,x) \
	( (pgm_txw_packet_t*)g_ptr_array_index((w)->pdata, TXW_PACKET_OFFSET((w), (x))) )
#define TXW_SET_PACKET(w,x,v) \
	do { \
		register int _o = TXW_PACKET_OFFSET((w), (x)); \
		g_ptr_array_index((w)->pdata, _o) = (v); \
	} while (0)

#ifdef TXW_DEBUG
#define ASSERT_TXW_BASE_INVARIANT(w) \
	{ \
		g_assert ( (w) != NULL ); \
\
/* does the array exist */ \
		g_assert ( (w)->pdata != NULL && (w)->pdata->len > 0 ); \
\
/* packet size has been set */ \
		g_assert ( (w)->max_tpdu > 0 ) ; \
\
/* all pointers are within window bounds */ \
		if ( !pgm_txw_empty ( (w) ) ) /* empty: trail = lead + 1, hence wrap around */ \
		{ \
			g_assert ( TXW_PACKET_OFFSET( (w), (w)->lead ) < (w)->pdata->len ); \
			g_assert ( TXW_PACKET_OFFSET( (w), (w)->trail ) < (w)->pdata->len ); \
			g_assert ( (w)->bytes_in_window > 0 ); \
			g_assert ( (w)->packets_in_window > 0 ); \
		} else { \
			g_assert ( (w)->bytes_in_window == 0 ); \
			g_assert ( (w)->packets_in_window == 0 ); \
		} \
\
	}

#define ASSERT_TXW_POINTER_INVARIANT(w) \
	{ \
/* are trail & lead points valid */ \
		if ( !pgm_txw_empty ( (w) ) ) \
		{ \
			g_assert ( NULL != TXW_PACKET( (w) , (w)->trail ) );    /* trail points to something */ \
			g_assert ( NULL != TXW_PACKET( (w) , (w)->lead ) );     /* lead points to something */ \
		} \
	} 
#else
#define ASSERT_TXW_BASE_INVARIANT(w)	while(0)
#define ASSERT_TXW_POINTER_INVARIANT(w)	while(0)
#endif

/* globals */

static void _list_iterator (gpointer, gpointer);

static inline gpointer pgm_txw_alloc_packet (pgm_txw_t*);
static inline int pgm_txw_pkt_free1 (pgm_txw_t*, pgm_txw_packet_t*);
static inline int pgm_txw_pop (pgm_txw_t*);


pgm_txw_t*
pgm_txw_init (
	guint16		tpdu_length,
	guint32		preallocate_size,
	guint32		txw_sqns,		/* transmit window size in sequence numbers */
	guint		txw_secs,		/* size in seconds */
	guint		txw_max_rte		/* max bandwidth */
	)
{
	g_trace ("init (tpdu %i pre-alloc %i txw_sqns %i txw_secs %i txw_max_rte %i).\n",
		tpdu_length, preallocate_size, txw_sqns, txw_secs, txw_max_rte);

	pgm_txw_t* t = g_slice_alloc0 (sizeof(pgm_txw_t));
	t->pdata = g_ptr_array_new ();

	t->max_tpdu = tpdu_length;

	for (guint32 i = 0; i < preallocate_size; i++)
	{
		gpointer data   = g_slice_alloc (t->max_tpdu);
		gpointer packet = g_slice_alloc (sizeof(pgm_txw_packet_t));
		g_trash_stack_push (&t->trash_data, data);
		g_trash_stack_push (&t->trash_packet, packet);
	}

/* calculate transmit window parameters */
	if (txw_sqns)
	{
	}
	else if (txw_secs && txw_max_rte)
	{
		txw_sqns = (txw_secs * txw_max_rte) / t->max_tpdu;
	}

	g_ptr_array_set_size (t->pdata, txw_sqns);

/* empty state for transmission group boundaries to align.
 *
 * trail = 0, lead = -1	
 */
	t->lead = -1;
	t->trail = t->lead + 1;

	t->retransmit_queue = g_queue_new();
	g_static_mutex_init (&t->retransmit_mutex);

#ifdef TXW_DEBUG
	guint memory = sizeof(pgm_txw_t) +
/* pointer array */
			sizeof(GPtrArray) + sizeof(guint) +
			*(guint*)( (char*)t->pdata + sizeof(gpointer) + sizeof(guint) ) +
/* pre-allocated data & packets */
			preallocate_size * (t->max_tpdu + sizeof(pgm_txw_packet_t));

	g_trace ("memory usage: %ub (%uMb)", memory, memory / (1024 * 1024));
#endif

	ASSERT_TXW_BASE_INVARIANT(t);
	ASSERT_TXW_POINTER_INVARIANT(t);
	return t;
}

int
pgm_txw_shutdown (
	pgm_txw_t*	t
	)
{
	g_trace ("shutdown.");

	ASSERT_TXW_BASE_INVARIANT(t);
	ASSERT_TXW_POINTER_INVARIANT(t);

/* pointer array */
	if (t->pdata)
	{
		g_ptr_array_foreach (t->pdata, _list_iterator, t);
		g_ptr_array_free (t->pdata, TRUE);
		t->pdata = NULL;
	}

	if (t->trash_data)
	{
		gpointer *p = NULL;

/* gcc recommends parentheses around assignment used as truth value */
		while ( (p = g_trash_stack_pop (&t->trash_data)) )
		{
			g_slice_free1 (t->max_tpdu, p);
		}

		g_assert ( t->trash_data == NULL );
	}

	if (t->trash_packet)
	{
		gpointer *p = NULL;
		while ( (p = g_trash_stack_pop (&t->trash_packet)) )
		{
			g_slice_free1 (sizeof(pgm_txw_packet_t), p);
		}

		g_assert ( t->trash_packet == NULL );
	}

	if (t->retransmit_queue)
	{
		g_queue_free (t->retransmit_queue);
		t->retransmit_queue = NULL;
	}
	g_static_mutex_free (&t->retransmit_mutex);

/* window */
	g_slice_free1 (sizeof(pgm_txw_t), t);

	return 0;
}

static void
_list_iterator (
	gpointer	data,
	gpointer	user_data
	)
{
	if (data == NULL) return;

	g_assert ( user_data != NULL);

	pgm_txw_pkt_free1 ((pgm_txw_t*)user_data, (pgm_txw_packet_t*)data);
}

static inline gpointer
pgm_txw_alloc_packet (
	pgm_txw_t*	t
	)
{
	gpointer p;
	if (t->trash_packet) {
		p = g_trash_stack_pop (&t->trash_packet);
		memset (p, 0, sizeof(pgm_txw_packet_t));
	} else {
		p = g_slice_alloc0 (sizeof(pgm_txw_packet_t));
	}
	return p;
}

static inline int
pgm_txw_pkt_free1 (
	pgm_txw_t*		t,
	pgm_txw_packet_t*	tp
	)
{
	ASSERT_TXW_BASE_INVARIANT(t);
	g_assert ( tp != NULL );

//	g_slice_free1 (tp->length, tp->data);
	g_trash_stack_push (&t->trash_data, tp->data);
	tp->data = NULL;

//	g_slice_free1 (sizeof(struct txw), tp);
	g_trash_stack_push (&t->trash_packet, tp);

	ASSERT_TXW_BASE_INVARIANT(t);
	return 0;
}

int
pgm_txw_push (
	pgm_txw_t*	t,
	gpointer	packet,
	guint16		length
	)
{
	ASSERT_TXW_BASE_INVARIANT(t);
	ASSERT_TXW_POINTER_INVARIANT(t);

	g_trace ("#%u: push: window ( trail %u lead %u )",
		t->lead+1, t->trail, t->lead);

/* check for full window */
	if ( pgm_txw_full (t) )
	{
//		g_trace ("full :o");

/* transmit window advancement scheme dependent action here */
		pgm_txw_pop (t);
	}

	t->lead++;

/* add to window */
	pgm_txw_packet_t* tp = pgm_txw_alloc_packet (t);
	tp->data		= packet;
	tp->length		= length;
	tp->sequence_number	= t->lead;

	TXW_SET_PACKET(t, tp->sequence_number, tp);
	g_trace ("#%u: adding packet", tp->sequence_number);

	t->bytes_in_window += length;
	t->packets_in_window++;

	ASSERT_TXW_BASE_INVARIANT(t);
	ASSERT_TXW_POINTER_INVARIANT(t);
	return 0;
}

/* the packet is not removed from the window
 */

int
pgm_txw_peek (
	pgm_txw_t*	t,
	guint32		sequence_number,
	gpointer*	packet,
	guint16*	length
	)
{
	ASSERT_TXW_BASE_INVARIANT(t);
	ASSERT_TXW_POINTER_INVARIANT(t);

/* check if sequence number is in window */
	if ( !ABS_IN_TXW(t, sequence_number) )
	{
		g_trace ("%u not in window.", sequence_number);
		return -1;
	}

	const pgm_txw_packet_t* tp = TXW_PACKET(t, sequence_number);
	*packet = tp->data;
	*length	= tp->length;

	ASSERT_TXW_BASE_INVARIANT(t);
	ASSERT_TXW_POINTER_INVARIANT(t);
	return 0;
}

static inline int
pgm_txw_pop (
	pgm_txw_t*	t
	)
{
	ASSERT_TXW_BASE_INVARIANT(t);
	ASSERT_TXW_POINTER_INVARIANT(t);

	if ( pgm_txw_empty (t) )
	{
		g_trace ("window is empty");
		return -1;
	}

	pgm_txw_packet_t* tp = TXW_PACKET(t, t->trail);

/* packet is waiting for retransmission */
	g_static_mutex_lock (&t->retransmit_mutex);
	if (tp->link_.data)
	{
		g_queue_unlink (t->retransmit_queue, &tp->link_);
		tp->link_.data = NULL;
	}
	g_static_mutex_unlock (&t->retransmit_mutex);

	t->bytes_in_window -= tp->length;
	t->packets_in_window--;

	pgm_txw_pkt_free1 (t, tp);
	TXW_SET_PACKET(t, t->trail, NULL);

	t->trail++;

	ASSERT_TXW_BASE_INVARIANT(t);
	ASSERT_TXW_POINTER_INVARIANT(t);
	return 0;
}

/* Try to add a sequence number to the retransmit queue, ignore if
 * already there or no longer in the transmit window.
 *
 * For parity NAKs, we deal on the transmission group sequence number
 * rather than the packet sequence number.  To simplify managment we
 * use the leading window packet to store the details of the entire
 * transmisison group.  Parity NAKs are ignored if the packet count is
 * less than or equal to the count already queued for retransmission.
 *
 * returns > 0 if sequence number added to queue.
 */

int
pgm_txw_retransmit_push (
	pgm_txw_t*	t,
	guint32		sequence_number,
	gboolean	is_parity,		/* parity NAK ⇒ sequence_number = transmission group | packet count */
	guint		tg_sqn_shift
	)
{
	ASSERT_TXW_BASE_INVARIANT(t);
	ASSERT_TXW_POINTER_INVARIANT(t);

	if (is_parity)
	{
		const guint32 tg_sqn_mask = 0xffffffff << tg_sqn_shift;

/* check if transmission group is in window */
		const guint32 nak_tg_sqn = sequence_number & tg_sqn_mask;	/* left unshifted */
		const guint32 nak_pkt_cnt = sequence_number & ~tg_sqn_mask;

printf ("nak_tg_sqn %i nak_pkt_cnt %i\n", (int)nak_tg_sqn, (int)nak_pkt_cnt);
		
		if ( !ABS_IN_TXW(t, nak_tg_sqn) )
		{
			g_trace ("transmission group lead, %u not in window.", sequence_number);
			return -1;
		}

		pgm_txw_packet_t* tp = TXW_PACKET(t, nak_tg_sqn);

		g_static_mutex_lock (&t->retransmit_mutex);
		if (tp->link_.data)
		{
			if (tp->pkt_cnt_requested >= nak_pkt_cnt)
			{
				g_static_mutex_unlock (&t->retransmit_mutex);
				g_trace ("%u already queued for retransmission.", sequence_number);
				return -1;
			}

/* more parity packets requested than currently scheduled, simply bump up the count */
			tp->pkt_cnt_requested = nak_pkt_cnt;
			return 0;
		}

/* new request */
		tp->pkt_cnt_requested++;
		tp->link_.data = tp;
		g_queue_push_head_link (t->retransmit_queue, &tp->link_);
		g_static_mutex_unlock (&t->retransmit_mutex);

	}
	else
	{

/* check if sequence number is in window */

		if ( !ABS_IN_TXW(t, sequence_number) )
		{
			g_trace ("%u not in window.", sequence_number);
			return -1;
		}

		pgm_txw_packet_t* tp = TXW_PACKET(t, sequence_number);

		g_static_mutex_lock (&t->retransmit_mutex);
		if (tp->link_.data)
		{
			g_static_mutex_unlock (&t->retransmit_mutex);
			g_trace ("%u already queued for retransmission.", sequence_number);
			return -1;
		}

		tp->link_.data = tp;
		g_queue_push_head_link (t->retransmit_queue, &tp->link_);
		g_static_mutex_unlock (&t->retransmit_mutex);

	}

	ASSERT_TXW_BASE_INVARIANT(t);
	ASSERT_TXW_POINTER_INVARIANT(t);
	return 1;
}

/* try to peek a packet from the retransmit queue
 *
 * return 0 if packet peeked.
 */

int
pgm_txw_retransmit_try_peek (
	pgm_txw_t*	t,
	guint32*	sequence_number,
	gpointer*	packet,
	guint16*	length,
	gboolean*	is_parity,
	guint*		rs_h			/* parity packet offset */
	)
{
	ASSERT_TXW_BASE_INVARIANT(t);
	ASSERT_TXW_POINTER_INVARIANT(t);

	g_static_mutex_lock (&t->retransmit_mutex);
	const GList* tail_link = g_queue_peek_tail_link (t->retransmit_queue);
	if (!tail_link) {
		g_static_mutex_unlock (&t->retransmit_mutex);
		return -1;
	}

	const pgm_txw_packet_t* tp = tail_link->data;
	*sequence_number = tp->sequence_number;

	if (tp->pkt_cnt_requested)
	{
		*is_parity = TRUE;
		*rs_h = tp->pkt_cnt_sent;
	}
	else
	{
		*is_parity = FALSE;
		*packet = tp->data;
		*length	= tp->length;
	}

	g_static_mutex_unlock (&t->retransmit_mutex);

	ASSERT_TXW_BASE_INVARIANT(t);
	ASSERT_TXW_POINTER_INVARIANT(t);
	return 0;
}

/* try to pop a packet from the retransmit queue (peek from window).
 *
 * parity NAKs specify a count of parity packets to transmit, as such
 * the count needs to be only reduced by one and the NAK node left on
 * the queue if count remains non-zero.
 *
 * return 0 if packet popped
 */

int
pgm_txw_retransmit_try_pop (
	pgm_txw_t*	t,
	guint32*	sequence_number,
	gpointer*	packet,
	guint16*	length,
	gboolean*	is_parity,
	guint*		rs_h,			/* parity packet offset */
	guint		rs_2t			/* maximum h */
	)
{
	ASSERT_TXW_BASE_INVARIANT(t);
	ASSERT_TXW_POINTER_INVARIANT(t);

	g_static_mutex_lock (&t->retransmit_mutex);
	GList* tail_link = g_queue_peek_tail_link (t->retransmit_queue);
	if (!tail_link) {
		g_static_mutex_unlock (&t->retransmit_mutex);
		return -1;
	}

	pgm_txw_packet_t* tp = tail_link->data;
	*sequence_number = tp->sequence_number;

	if (tp->pkt_cnt_requested) {
		*is_parity = TRUE;
		*rs_h = tp->pkt_cnt_sent;

/* remove if all requested parity packets have been sent */
		if (--(tp->pkt_cnt_requested) == 0)
		{
			g_queue_pop_tail_link (t->retransmit_queue);
			tail_link->data = NULL;
		}

/* wrap around parity generation in unlikely event that more packets requested than are available
 * under the Reed-Solomon code definition.
 */
		if (++(tp->pkt_cnt_sent) == rs_2t) {
			tp->pkt_cnt_sent = 0;
		}
	}
	else
	{
		*is_parity = FALSE;

/* selective NAK, therefore pop node */
		*packet = tp->data;
		*length	= tp->length;
		g_queue_pop_tail_link (t->retransmit_queue);
		tail_link->data = NULL;
	}

	g_static_mutex_unlock (&t->retransmit_mutex);

	ASSERT_TXW_BASE_INVARIANT(t);
	ASSERT_TXW_POINTER_INVARIANT(t);
	return 0;
}

/* pop a previously peeked retransmit entry
 */

int
pgm_txw_retransmit_pop (
	pgm_txw_t*	t,
	guint		rs_2t			/* maximum h */
	)
{
	ASSERT_TXW_BASE_INVARIANT(t);
	ASSERT_TXW_POINTER_INVARIANT(t);

	g_static_mutex_lock (&t->retransmit_mutex);
	GList* tail_link = g_queue_peek_tail_link (t->retransmit_queue);
	g_assert (tail_link);

	pgm_txw_packet_t* tp = tail_link->data;

	if (tp->pkt_cnt_requested)
	{
/* remove if all requested parity packets have been sent */
		if (--(tp->pkt_cnt_requested) == 0)
		{
			g_queue_pop_tail_link (t->retransmit_queue);
			tail_link->data = NULL;
		}

/* wrap around parity generation in unlikely event that more packets requested than are available
 * under the Reed-Solomon code definition.
 */
		if (++(tp->pkt_cnt_sent) == rs_2t) {
			tp->pkt_cnt_sent = 0;
		}
	}
	else
	{
		g_queue_pop_tail_link (t->retransmit_queue);
		tail_link->data = NULL;
	}

	g_static_mutex_unlock (&t->retransmit_mutex);

	ASSERT_TXW_BASE_INVARIANT(t);
	ASSERT_TXW_POINTER_INVARIANT(t);
	return 0;
}

/* eof */
