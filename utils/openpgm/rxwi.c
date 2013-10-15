/* vim:ts=8:sts=8:sw=4:noai:noexpandtab
 *
 * A basic receive window: pointer array implementation.
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
#include <sys/uio.h>

//#define RXW_DEBUG

#ifndef RXW_DEBUG
#define G_DISABLE_ASSERT
#endif

#include <glib.h>

#include "pgm/rxwi.h"
#include "pgm/sn.h"
#include "pgm/timer.h"

#ifndef RXW_DEBUG
#define g_trace(...)		while (0)
#else
#define g_trace(...)		g_debug(__VA_ARGS__)
#endif


/* globals */

/* local globals */

#define IN_TXW(w,x)	( pgm_uint32_gte ( (x), (w)->rxw_trail ) )
#define IN_RXW(w,x) \
	( \
		pgm_uint32_gte ( (x), (w)->rxw_trail ) && pgm_uint32_lte ( (x), (w)->lead ) \
	)

#define ABS_IN_RXW(w,x) \
	( \
		!pgm_rxw_empty( (w) ) && \
		pgm_uint32_gte ( (x), (w)->trail ) && pgm_uint32_lte ( (x), (w)->lead ) \
	)

#define RXW_PACKET_OFFSET(w,x)		( (x) % pgm_rxw_len ((w)) ) 
#define RXW_PACKET(w,x) \
	( (pgm_rxw_packet_t*)g_ptr_array_index((w)->pdata, RXW_PACKET_OFFSET((w), (x))) )
#define RXW_SET_PACKET(w,x,v) \
	do { \
		register int _o = RXW_PACKET_OFFSET((w), (x)); \
		g_ptr_array_index((w)->pdata, _o) = (v); \
	} while (0)

/* is (a) greater than (b) wrt. leading edge of receive window (w) */
#define SLIDINGWINDOW_GT(w,a,b) \
	( \
		pgm_rxw_empty( (w) ) ? \
		( \
			( (gint32)(a) - (gint32)( (w)->trail ) ) > ( (gint32)(b) - (gint32)( (w)->trail ) ) \
		) \
			: \
		( \
			( (gint32)(a) - (gint32)( (w)->lead ) ) > ( (gint32)(b) - (gint32)( (w)->lead ) ) \
		) \
	)

#ifdef RXW_DEBUG
#define ASSERT_RXW_BASE_INVARIANT(w) \
	{ \
		g_assert ( (w) != NULL ); \
\
/* does the array exist */ \
		g_assert ( (w)->pdata != NULL && (w)->pdata->len > 0 ); \
\
/* do the trash stacks point somewhere (the transport object) */ \
		g_assert ( (w)->trash_data != NULL ); \
		g_assert ( (w)->trash_packet != NULL ); \
\
/* is the trash mutex pointing somewhere */ \
		g_assert ( (w)->trash_mutex != NULL ); \
\
/* the state queues exist */ \
		g_assert ( (w)->backoff_queue ); \
		g_assert ( (w)->wait_ncf_queue ); \
		g_assert ( (w)->wait_data_queue ); \
\
/* packet size has been set */ \
		g_assert ( (w)->max_tpdu > 0 ) ; \
\
/* all pointers are within window bounds */ \
		if ( !pgm_rxw_empty( (w) ) ) /* empty: trail = lead + 1, hence wrap around */ \
		{ \
			g_assert ( RXW_PACKET_OFFSET( (w), (w)->lead ) < (w)->pdata->len ); \
			g_assert ( RXW_PACKET_OFFSET( (w), (w)->trail ) < (w)->pdata->len ); \
		} \
\
	}

#define ASSERT_RXW_POINTER_INVARIANT(w) \
	{ \
/* are trail & lead points valid */ \
		if ( !pgm_rxw_empty( (w) ) ) \
		{ \
			g_assert ( NULL != RXW_PACKET( (w) , (w)->trail ) );	/* trail points to something */ \
			g_assert ( NULL != RXW_PACKET( (w) , (w)->lead ) );	/* lead points to something */ \
\
/* queue's contain at least one packet */ \
			if ( !(w)->is_waiting ) \
			{ \
				g_assert ( ( (w)->backoff_queue->length + \
					     (w)->wait_ncf_queue->length + \
					     (w)->wait_data_queue->length + \
					     (w)->lost_count + \
					     (w)->fragment_count + \
					     (w)->parity_count + \
					     (w)->committed_count + \
					     (w)->parity_data_count ) > 0 ); \
			} \
		} \
		else \
		{ \
			g_assert ( (w)->backoff_queue->length == 0 ); \
			g_assert ( (w)->wait_ncf_queue->length == 0 ); \
			g_assert ( (w)->wait_data_queue->length == 0 ); \
			g_assert ( (w)->lost_count == 0 ); \
			g_assert ( (w)->fragment_count == 0 ); \
			g_assert ( (w)->parity_count == 0 ); \
			g_assert ( (w)->committed_count == 0 ); \
			g_assert ( (w)->parity_data_count == 0 ); \
		} \
	}
#else
#define ASSERT_RXW_BASE_INVARIANT(w)    while(0)
#define ASSERT_RXW_POINTER_INVARIANT(w) while(0)
#endif


static void _list_iterator (gpointer, gpointer);
static inline int pgm_rxw_pop_lead (pgm_rxw_t*);
static inline int pgm_rxw_pop_trail (pgm_rxw_t*);
static inline int pgm_rxw_pkt_remove1 (pgm_rxw_t*, pgm_rxw_packet_t*);
static inline int pgm_rxw_pkt_data_free1 (pgm_rxw_t*, gpointer);
static inline int pgm_rxw_data_free1 (pgm_rxw_t*, pgm_rxw_packet_t*);
static inline int pgm_rxw_pkt_free1 (pgm_rxw_t*, pgm_rxw_packet_t*);
static inline gpointer pgm_rxw_alloc_packet (pgm_rxw_t*);
static inline gpointer pgm_rxw_alloc0_packet (pgm_rxw_t*);


/* sub-windows of the receive window
 *                                      r->lead
 *  | Parity-data | Commit |   Incoming   |
 *  |<----------->|<------>|<------------>|
 *  |             |        |              |
 * r->trail    r->commit_trail
 *                       r->commit_lead
 */

static inline guint32
pgm_rxw_incoming_empty (pgm_rxw_t* r)
{
	return r->commit_lead == r->lead + 1;
}

static inline guint32
pgm_rxw_commit_empty (pgm_rxw_t* r)
{
	return r->commit_trail == r->commit_lead;
}

static inline guint32
pgm_rxw_parity_data_empty (pgm_rxw_t* r)
{
	return r->trail == r->commit_trail;
}


pgm_rxw_t*
pgm_rxw_init (
	const void*	identifier,		/* TSI */
	guint16		tpdu_length,
	guint32		preallocate_size,
	guint32		rxw_sqns,		/* transmit window size in sequence numbers */
	guint		rxw_secs,		/* size in seconds */
	guint		rxw_max_rte,		/* max bandwidth */
	GTrashStack**	trash_data,
	GTrashStack**	trash_packet,
	GStaticMutex*	trash_mutex
	)
{
	g_trace ("init (tpdu %i pre-alloc %i rxw_sqns %i rxw_secs %i rxw_max_rte %i).",
		tpdu_length, preallocate_size, rxw_sqns, rxw_secs, rxw_max_rte);

	pgm_rxw_t* r = g_slice_alloc0 (sizeof(pgm_rxw_t));
	r->identifier = identifier;
	r->pdata = g_ptr_array_new ();
	r->max_tpdu = tpdu_length;

	r->trash_data = trash_data;
	r->trash_packet = trash_packet;
	r->trash_mutex = trash_mutex;

	if (preallocate_size)
	{
		g_static_mutex_lock (r->trash_mutex);
		for (guint32 i = 0; i < preallocate_size; i++)
		{
			gpointer data   = g_slice_alloc (r->max_tpdu);
			gpointer packet = g_slice_alloc (sizeof(pgm_rxw_packet_t));
			g_trash_stack_push (r->trash_data, data);
			g_trash_stack_push (r->trash_packet, packet);
		}
		g_static_mutex_unlock (r->trash_mutex);
	}

/* calculate receive window parameters as per transmit window */
	if (rxw_sqns)
	{
	}
	else if (rxw_secs && rxw_max_rte)
	{
		rxw_sqns = (rxw_secs * rxw_max_rte) / r->max_tpdu;
	}

	g_ptr_array_set_size (r->pdata, rxw_sqns);

/* empty state:
 *
 * trail = 0, lead = -1
 * commit_trail = commit_lead = rxw_trail = rxw_trail_init = 0
 */
	r->lead = -1;
	r->trail = r->lead + 1;

/* limit retransmit requests on late session joining */
	r->is_rxw_constrained = TRUE;
	r->is_window_defined = FALSE;

/* empty queue's for nak & ncfs */
	r->backoff_queue = g_queue_new ();
	r->wait_ncf_queue = g_queue_new ();
	r->wait_data_queue = g_queue_new ();

/* statistics */
#if 0
	r->min_fill_time = G_MAXINT;
	r->max_fill_time = G_MININT;
	r->min_nak_transmit_count = G_MAXINT;
	r->max_nak_transmit_count = G_MININT;
#endif

#ifdef RXW_DEBUG
	guint memory = sizeof(pgm_rxw_t) +
/* pointer array */
			sizeof(GPtrArray) + sizeof(guint) +
			*(guint*)( (char*)r->pdata + sizeof(gpointer) + sizeof(guint) ) +
/* pre-allocated data & packets */
			( preallocate_size * (r->max_tpdu + sizeof(pgm_rxw_packet_t)) ) +
/* state queues */
			3 * sizeof(GQueue) +
/* guess at timer */
			4 * sizeof(int);
			
	g_trace ("memory usage: %ub (%uMb)", memory, memory / (1024 * 1024));
#endif

	ASSERT_RXW_BASE_INVARIANT(r);
	ASSERT_RXW_POINTER_INVARIANT(r);
	return r;
}

int
pgm_rxw_shutdown (
	pgm_rxw_t*	r
	)
{
	g_trace ("rxw: shutdown.");

	ASSERT_RXW_BASE_INVARIANT(r);
	ASSERT_RXW_POINTER_INVARIANT(r);

/* pointer array */
	if (r->pdata)
	{
		g_ptr_array_foreach (r->pdata, _list_iterator, r);
		g_ptr_array_free (r->pdata, TRUE);
		r->pdata = NULL;
	}

/* nak/ncf time lists,
 * important: link items are static to each packet struct
 */
	if (r->backoff_queue)
	{
		g_slice_free (GQueue, r->backoff_queue);
		r->backoff_queue = NULL;
	}
	if (r->wait_ncf_queue)
	{
		g_slice_free (GQueue, r->wait_ncf_queue);
		r->wait_ncf_queue = NULL;
	}
	if (r->wait_data_queue)
	{
		g_slice_free (GQueue, r->wait_data_queue);
		r->wait_data_queue = NULL;
	}

/* window */
	g_slice_free1 (sizeof(pgm_rxw_t), r);

	return PGM_RXW_OK;
}

static void
_list_iterator (
	gpointer	data,
	gpointer	user_data
	)
{
	if (data == NULL) return;

	g_assert ( user_data != NULL);

	pgm_rxw_pkt_free1 ((pgm_rxw_t*)user_data, (pgm_rxw_packet_t*)data);
}

static inline gpointer
pgm_rxw_alloc_packet (
	pgm_rxw_t*	r
	)
{
	ASSERT_RXW_BASE_INVARIANT(r);

	gpointer p;

	g_static_mutex_lock (r->trash_mutex);
	if (*r->trash_packet) {
		p = g_trash_stack_pop (r->trash_packet);
	} else {
		p = g_slice_alloc (sizeof(pgm_rxw_packet_t));
	}
	g_static_mutex_unlock (r->trash_mutex);

	return p;
}

static inline gpointer
pgm_rxw_alloc0_packet (
	pgm_rxw_t*	r
	)
{
	ASSERT_RXW_BASE_INVARIANT(r);

	gpointer p;

	g_static_mutex_lock (r->trash_mutex);
	if (*r->trash_packet) {
		p = g_trash_stack_pop (r->trash_packet);
		memset (p, 0, sizeof(pgm_rxw_packet_t));
	} else {
		p = g_slice_alloc0 (sizeof(pgm_rxw_packet_t));
	}
	g_static_mutex_unlock (r->trash_mutex);

	ASSERT_RXW_BASE_INVARIANT(r);

	return p;
}

/* the sequence number is inside the packet as opposed to from internal
 * counters, this means one push on the receive window can actually translate
 * as many: the extra's acting as place holders and NAK containers.
 *
 * ownership of packet is with receive window, therefore must be added to
 * receive window or trash stack.
 *
 * returns:
 *	PGM_RXW_CREATED_PLACEHOLDER
 *	PGM_RXW_FILLED_PLACEHOLDER
 *	PGM_RXW_ADVANCED_WINDOW
 *	PGM_RXW_NOT_IN_TXW if sequence number is very wibbly wobbly
 *	PGM_RXW_DUPLICATE
 *	PGM_RXW_APDU_LOST
 *	PGM_RXW_MALFORMED_APDU
 */

int
pgm_rxw_push_fragment (
	pgm_rxw_t*	r,
	gpointer	packet,
	gsize		length,
	guint32		sequence_number,
	guint32		trail,
	struct pgm_opt_fragment* opt_fragment,
	pgm_time_t	nak_rb_expiry
	)
{
	ASSERT_RXW_BASE_INVARIANT(r);
	ASSERT_RXW_POINTER_INVARIANT(r);

	guint dropped = 0;
	int retval = PGM_RXW_UNKNOWN;

/* convert to more apparent names */
	const guint32 apdu_first_sqn	= opt_fragment ? g_ntohl (opt_fragment->opt_sqn) : 0;
	const guint32 apdu_len		= opt_fragment ? g_ntohl (opt_fragment->opt_frag_len) : 0;

	g_trace ("#%u: data trail #%u: push: window ( rxw_trail %u rxw_trail_init %u trail %u lead %u )",
		sequence_number, trail, 
		r->rxw_trail, r->rxw_trail_init, r->trail, r->lead);

/* trail is the next packet to commit upstream, lead is the leading edge
 * of the receive window with possible gaps inside, rxw_trail is the transmit
 * window trail for retransmit requests.
 */

	if ( !r->is_window_defined )
	{
/* if this packet is a fragment of an apdu, and not the first, we continue on as per spec but careful to
 * advance the trailing edge to discard the remaining fragments.
 */
		g_trace ("#%u: using odata to temporarily define window", sequence_number);

		r->lead = sequence_number - 1;
		r->commit_trail = r->commit_lead = r->rxw_trail = r->rxw_trail_init = r->trail = r->lead + 1;

		r->is_rxw_constrained = TRUE;
		r->is_window_defined = TRUE;
	}
	else
	{
/* check if packet should be discarded or processed further */

		if ( !IN_TXW(r, sequence_number) )
		{
			g_trace ("#%u: not in transmit window, discarding.", sequence_number);
			pgm_rxw_pkt_data_free1 (r, packet);
			retval = PGM_RXW_NOT_IN_TXW;
			goto out;
		}

		pgm_rxw_window_update (r, trail, r->lead, r->tg_size, r->tg_sqn_shift, nak_rb_expiry);
	}

	g_trace ("#%u: window ( rxw_trail %u rxw_trail_init %u trail %u commit_trail %u commit_lead %u lead %u )",
		sequence_number, r->rxw_trail, r->rxw_trail_init, r->trail, r->commit_trail, r->commit_lead, r->lead);
	ASSERT_RXW_BASE_INVARIANT(r);
	ASSERT_RXW_POINTER_INVARIANT(r);

/* already committed */
	if ( pgm_uint32_lt (sequence_number, r->commit_lead) )
	{
		g_trace ("#%u: already committed, discarding.", sequence_number);
		pgm_rxw_pkt_data_free1 (r, packet);
		retval = PGM_RXW_DUPLICATE;
		goto out;
	}

/* check for duplicate */
	if ( pgm_uint32_lte (sequence_number, r->lead) )
	{
		g_trace ("#%u: in rx window, checking for duplicate.", sequence_number);

		pgm_rxw_packet_t* rp = RXW_PACKET(r, sequence_number);

		if (rp)
		{
			if (rp->length)
			{
				g_trace ("#%u: already received, discarding.", sequence_number);
				pgm_rxw_pkt_data_free1 (r, packet);
				retval = PGM_RXW_DUPLICATE;
				goto out;
			}

/* for fragments check that apdu is valid */
			if (	apdu_len && 
				apdu_first_sqn != sequence_number &&
				(
					pgm_rxw_empty (r) ||
				       !ABS_IN_RXW(r, apdu_first_sqn) ||
					RXW_PACKET(r, apdu_first_sqn)->state == PGM_PKT_LOST_DATA_STATE
				)
			   )
			{
				g_trace ("#%u: first fragment #%u not in receive window, apdu is lost.", sequence_number, apdu_first_sqn);
				pgm_rxw_mark_lost (r, sequence_number);
				pgm_rxw_pkt_data_free1 (r, packet);
				retval = PGM_RXW_APDU_LOST;
				goto out_flush;
			}

			if ( apdu_len && pgm_uint32_gt (apdu_first_sqn, sequence_number) )
			{
				g_trace ("#%u: first apdu fragment sequence number: #%u not lowest, ignoring packet.",
					sequence_number, apdu_first_sqn);
				pgm_rxw_pkt_data_free1 (r, packet);
				retval = PGM_RXW_MALFORMED_APDU;
				goto out;
			}

/* destination should not contain a data packet, although it may contain parity */
			g_assert( rp->state == PGM_PKT_BACK_OFF_STATE ||
				  rp->state == PGM_PKT_WAIT_NCF_STATE ||
				  rp->state == PGM_PKT_WAIT_DATA_STATE ||
				  rp->state == PGM_PKT_HAVE_PARITY_STATE ||
				  rp->state == PGM_PKT_LOST_DATA_STATE );
			g_trace ("#%u: filling in a gap.", sequence_number);

			if ( rp->state == PGM_PKT_HAVE_PARITY_STATE )
			{
				g_trace ("#%u: destination contains parity, shuffling to next available entry.", sequence_number);
/* find if any other packets are lost in this transmission group */

				const guint32 tg_sqn_mask = 0xffffffff << r->tg_sqn_shift;
				const guint32 next_tg_sqn = (sequence_number & tg_sqn_mask) + 1;

				if (sequence_number != next_tg_sqn)
				for (guint32 i = sequence_number + 1; i != next_tg_sqn; i++)
				{
					pgm_rxw_packet_t* pp = RXW_PACKET(r, i);
					if ( pp->state == PGM_PKT_BACK_OFF_STATE ||
					     pp->state == PGM_PKT_WAIT_NCF_STATE ||
					     pp->state == PGM_PKT_WAIT_DATA_STATE ||
					     pp->state == PGM_PKT_LOST_DATA_STATE )
					{
						g_assert (pp->data == NULL);

/* move parity to this new sequence number */
						memcpy (&pp->opt_fragment, &rp->opt_fragment, sizeof(struct pgm_opt_fragment));
						pp->data	= rp->data;
						pp->length	= rp->length;
						pp->state	= rp->state;
						rp->data	= NULL;
						rp->length	= 0;
						rp->state	= PGM_PKT_WAIT_DATA_STATE;
						break;
					}
				}

/* no incomplete packet found, therefore parity is no longer required */
				if (rp->state != PGM_PKT_WAIT_DATA_STATE)
				{
					pgm_rxw_data_free1 (r, rp);
					rp->state = PGM_PKT_WAIT_DATA_STATE;
				}
			}
			else if ( rp->state == PGM_PKT_LOST_DATA_STATE )	/* lucky packet */
			{
				r->lost_count--;
			}

/* a non-committed packet */
			r->fragment_count++;

			if (apdu_len)	/* a fragment */
			{
				memcpy (&rp->opt_fragment, opt_fragment, sizeof(struct pgm_opt_fragment));
			}

			g_assert (rp->data == NULL);
			rp->data	= packet;
			rp->length	= length;

			pgm_rxw_pkt_state_unlink (r, rp);
			rp->state	= PGM_PKT_HAVE_DATA_STATE;
			retval		= PGM_RXW_FILLED_PLACEHOLDER;

			const guint32 fill_time = pgm_time_now - rp->t0;
			if (!r->max_fill_time) {
				r->max_fill_time = r->min_fill_time = fill_time;
			}
			else
			{
				if (fill_time > r->max_fill_time)
					r->max_fill_time = fill_time;
				else if (fill_time < r->min_fill_time)
					r->min_fill_time = fill_time;

				if (!r->max_nak_transmit_count) {
					r->max_nak_transmit_count = r->min_nak_transmit_count = rp->nak_transmit_count;
				}
				else
				{
					if (rp->nak_transmit_count > r->max_nak_transmit_count)
						r->max_nak_transmit_count = rp->nak_transmit_count;
					else if (rp->nak_transmit_count < r->min_nak_transmit_count)
						r->min_nak_transmit_count = rp->nak_transmit_count;
				}
			}
		}
		else
		{
			g_debug ("sequence_number %u points to (null) in window (trail %u commit_trail %u commit_lead %u lead %u).",
				sequence_number, r->trail, r->commit_trail, r->commit_lead, r->lead);
			ASSERT_RXW_BASE_INVARIANT(r);
			ASSERT_RXW_POINTER_INVARIANT(r);
			g_assert_not_reached();
		}
	}
	else	/* sequence_number > lead */
	{
/* extends receive window */

/* check bounds of commit window */
		guint32 new_commit_sqns = ( 1 + sequence_number ) - r->commit_trail;
                if ( !pgm_rxw_commit_empty (r) &&
		     (new_commit_sqns >= pgm_rxw_len (r)) )
                {
			pgm_rxw_window_update (r, r->rxw_trail, sequence_number, r->tg_size, r->tg_sqn_shift, nak_rb_expiry);
			pgm_rxw_pkt_data_free1 (r, packet);
			goto out;
                }


		g_trace ("#%u: lead extended.", sequence_number);
		g_assert ( pgm_uint32_gt (sequence_number, r->lead) );

		if ( pgm_rxw_full(r) )
		{
			dropped++;
//			g_trace ("#%u: dropping #%u due to odata filling window.", sequence_number, r->trail);

			pgm_rxw_pop_trail (r);
//			pgm_rxw_flush (r);
		}

		r->lead++;

/* if packet is non-contiguous to current leading edge add place holders */
		if (r->lead != sequence_number)
		{
/* TODO: can be rather inefficient on packet loss looping through dropped sequence numbers
 */
			while (r->lead != sequence_number)
			{
				pgm_rxw_packet_t* ph = pgm_rxw_alloc0_packet(r);
				ph->link_.data		= ph;
				ph->sequence_number     = r->lead;
				ph->nak_rb_expiry	= nak_rb_expiry;
				ph->state		= PGM_PKT_BACK_OFF_STATE;
				ph->t0			= pgm_time_now;

				RXW_SET_PACKET(r, ph->sequence_number, ph);

/* send nak by sending to end of expiry list */
				g_queue_push_head_link (r->backoff_queue, &ph->link_);
				g_trace ("#%" G_GUINT32_FORMAT ": place holder, backoff_queue %" G_GUINT32_FORMAT "/%u lead %" G_GUINT32_FORMAT,
					sequence_number, r->backoff_queue->length, pgm_rxw_sqns(r), r->lead);

				if ( pgm_rxw_full(r) )
				{
					dropped++;
//					g_trace ("dropping #%u due to odata filling window.", r->trail);

					pgm_rxw_pop_trail (r);
//					pgm_rxw_flush (r);
				}

				r->lead++;
			}
			retval = PGM_RXW_CREATED_PLACEHOLDER;
		}
		else
		{
			retval = PGM_RXW_ADVANCED_WINDOW;
		}

		g_assert ( r->lead == sequence_number );

/* sanity check on sequence number distance */
		if ( apdu_len && pgm_uint32_gt (apdu_first_sqn, sequence_number) )
		{
			g_trace ("#%u: first apdu fragment sequence number: #%u not lowest, ignoring packet.",
				sequence_number, apdu_first_sqn);
			pgm_rxw_pkt_data_free1 (r, packet);
			retval = PGM_RXW_MALFORMED_APDU;
			goto out;
		}

		pgm_rxw_packet_t* rp	= pgm_rxw_alloc0_packet(r);
		rp->link_.data		= rp;
		rp->sequence_number     = r->lead;

/* for fragments check that apdu is valid: dupe code to above */
		if (    apdu_len && 
			apdu_first_sqn != sequence_number &&
			(	
				pgm_rxw_empty (r) ||
			       !ABS_IN_RXW(r, apdu_first_sqn) ||
				RXW_PACKET(r, apdu_first_sqn)->state == PGM_PKT_LOST_DATA_STATE
			)
		   )
		{
			g_trace ("#%u: first fragment #%u not in receive window, apdu is lost.", sequence_number, apdu_first_sqn);
			pgm_rxw_pkt_data_free1 (r, packet);
			rp->state = PGM_PKT_LOST_DATA_STATE;
			r->lost_count++;
			RXW_SET_PACKET(r, rp->sequence_number, rp);
			retval = PGM_RXW_APDU_LOST;
			r->is_waiting = TRUE;
			goto out_flush;
		}

/* a non-committed packet */
		r->fragment_count++;

		if (apdu_len)	/* fragment */
		{
			memcpy (&rp->opt_fragment, opt_fragment, sizeof(struct pgm_opt_fragment));
		}
		rp->data		= packet;
		rp->length		= length;
		rp->state		= PGM_PKT_HAVE_DATA_STATE;

		RXW_SET_PACKET(r, rp->sequence_number, rp);
		g_trace ("#%" G_GUINT32_FORMAT ": added packet #%" G_GUINT32_FORMAT ", rxw_sqns %" G_GUINT32_FORMAT,
			sequence_number, rp->sequence_number, pgm_rxw_sqns(r));
	}

	r->is_waiting = TRUE;

out_flush:
	g_trace ("#%u: push complete: window ( rxw_trail %u rxw_trail_init %u trail %u commit_trail %u commit_lead %u lead %u )",
		sequence_number,
		r->rxw_trail, r->rxw_trail_init, r->trail, r->commit_trail, r->commit_lead, r->lead);

out:
	if (dropped) {
		g_trace ("dropped %u messages due to odata filling window.", dropped);
		r->cumulative_losses += dropped;
	}

	ASSERT_RXW_BASE_INVARIANT(r);
	ASSERT_RXW_POINTER_INVARIANT(r);
	return retval;
}

/* flush packets but instead of calling on_data append the contiguous data packets
 * to the provided scatter/gather vector.
 *
 * when transmission groups are enabled, packets remain in the windows tagged committed
 * until the transmission group has been completely committed.  this allows the packet
 * data to be used in parity calculations to recover the missing packets.
 *
 * returns -1 on nothing read, returns 0 on zero bytes read.
 */
gssize
pgm_rxw_readv (
	pgm_rxw_t*		r,
	pgm_msgv_t**		pmsg,		/* message array, updated as messages appended */
	guint			msg_len,	/* number of items in pmsg */
	struct iovec**		piov,		/* underlying iov storage */
	guint			iov_len		/* number of items in piov */
	)
{
	ASSERT_RXW_BASE_INVARIANT(r);

	g_trace ("pgm_rxw_readv");

	guint dropped = 0;
	gssize bytes_read = 0;
	guint msgs_read = 0;
	const pgm_msgv_t* msg_end = *pmsg + msg_len;
	const struct iovec* iov_end = *piov + iov_len;

	while ( !pgm_rxw_incoming_empty (r) )
	{
		pgm_rxw_packet_t* cp = RXW_PACKET(r, r->commit_lead);
		g_assert ( cp != NULL );

		switch (cp->state) {
		case PGM_PKT_LOST_DATA_STATE:
/* if packets are being held drop them all as group is now unrecoverable */
			while (r->commit_lead != r->trail) {
				dropped++;
				pgm_rxw_pop_trail (r);
			}

/* from now on r->commit_lead ≡ r->trail */
			g_assert (r->commit_lead == r->trail);

/* check for lost apdu */
			if ( g_ntohl (cp->of_apdu_len) )
			{
				const guint32 apdu_first_sqn = g_ntohl (cp->of_apdu_first_sqn);

/* drop the first fragment, then others follow through as its no longer in the window */
				if ( r->trail == apdu_first_sqn )
				{
					dropped++;
					pgm_rxw_pop_trail (r);
				}

/* flush others, make sure to check each packet is an apdu packet and not simply a zero match */
				while (!pgm_rxw_empty(r))
				{
					cp = RXW_PACKET(r, r->trail);
					if (g_ntohl (cp->of_apdu_len) && g_ntohl (cp->of_apdu_first_sqn) == apdu_first_sqn)
					{
						dropped++;
						pgm_rxw_pop_trail (r);
					}
					else
					{	/* another apdu or tpdu exists */
						break;
					}
				}
			}
			else
			{	/* plain tpdu */
				g_trace ("skipping lost packet @ #%" G_GUINT32_FORMAT, cp->sequence_number);

				dropped++;
				pgm_rxw_pop_trail (r);
/* one tpdu lost */
			}

			g_assert (r->commit_lead == r->trail);
			goto out;
			continue;
		
		case PGM_PKT_HAVE_DATA_STATE:
			/* not lost */
			g_assert ( cp->data != NULL && cp->length > 0 );

/* check for contiguous apdu */
			if ( g_ntohl (cp->of_apdu_len) )
			{
				if ( g_ntohl (cp->of_apdu_first_sqn) != cp->sequence_number )
				{
					g_trace ("partial apdu at trailing edge, marking lost.");
					pgm_rxw_mark_lost (r, cp->sequence_number);
					break;
				}

				guint32 frag = g_ntohl (cp->of_apdu_first_sqn);
				guint32 apdu_len = 0;
				pgm_rxw_packet_t* ap = NULL;
				while ( ABS_IN_RXW(r, frag) && apdu_len < g_ntohl (cp->of_apdu_len) )
				{
					ap = RXW_PACKET(r, frag);
					g_assert ( ap != NULL );
					if (ap->state != PGM_PKT_HAVE_DATA_STATE)
					{
						break;
					}
					apdu_len += ap->length;
					frag++;
				}

				if (apdu_len == g_ntohl (cp->of_apdu_len))
				{
/* check if sufficient room for apdu */
					const guint32 apdu_len_in_frags = frag - g_ntohl (cp->of_apdu_first_sqn) + 1;
					if (*piov + apdu_len_in_frags > iov_end) {
						break;
					}

					g_trace ("contiguous apdu found @ #%" G_GUINT32_FORMAT " - #%" G_GUINT32_FORMAT 
							", passing upstream.",
						g_ntohl (cp->of_apdu_first_sqn), ap->sequence_number);

/* pass upstream & cleanup */
					(*pmsg)->msgv_identifier = r->identifier;
					(*pmsg)->msgv_iovlen     = 0;
					(*pmsg)->msgv_iov        = *piov;
					for (guint32 i = g_ntohl (cp->of_apdu_first_sqn); i < frag; i++)
					{
						ap = RXW_PACKET(r, i);

						(*piov)->iov_base = ap->data;	/* copy */
						(*piov)->iov_len  = ap->length;
						(*pmsg)->msgv_iovlen++;

						++(*piov);

						bytes_read += ap->length;	/* stats */

						ap->state = PGM_PKT_COMMIT_DATA_STATE;
						r->fragment_count--;		/* accounting */
						r->commit_lead++;
						r->committed_count++;
					}

/* end of commit buffer */
					++(*pmsg);
					msgs_read++;

					if (*pmsg == msg_end) {
						goto out;
					}

					if (*piov == iov_end) {
						goto out;
					}
				}
				else
				{	/* incomplete apdu */
					g_trace ("partial apdu found %u of %u bytes.",
						apdu_len, g_ntohl (cp->of_apdu_len));
					goto out;
				}
			}
			else
			{	/* plain tpdu */
				g_trace ("one packet found @ #%" G_GUINT32_FORMAT ", passing upstream.",
					cp->sequence_number);

/* pass upstream, including data memory ownership */
				(*pmsg)->msgv_identifier = r->identifier;
				(*pmsg)->msgv_iovlen     = 1;
				(*pmsg)->msgv_iov        = *piov;
				(*piov)->iov_base = cp->data;
				(*piov)->iov_len  = cp->length;
				bytes_read += cp->length;
				msgs_read++;

/* move to commit window */
				cp->state = PGM_PKT_COMMIT_DATA_STATE;
				r->fragment_count--;
				r->commit_lead++;
				r->committed_count++;

/* end of commit buffer */
				++(*pmsg);
				++(*piov);

				if (*pmsg == msg_end) {
					goto out;
				}

				if (*piov == iov_end) {
					goto out;
				}
			}

/* one apdu or tpdu processed */
			break;

		default:
			g_trace ("!(have|lost)_data_state, sqn %" G_GUINT32_FORMAT " packet state %s(%i) cp->length %u", r->commit_lead, pgm_rxw_state_string(cp->state), cp->state, cp->length);
			goto out;
		}
	}

out:
	r->cumulative_losses += dropped;
	r->bytes_delivered   += bytes_read;
	r->msgs_delivered    += msgs_read;

	r->pgm_sock_err.lost_count = dropped;

	ASSERT_RXW_BASE_INVARIANT(r);

	return msgs_read ? bytes_read : -1;
}

/* used to indicate application layer has released interest in packets in committed-data state,
 * move to parity-data state until transmission group has completed.
 */
int
pgm_rxw_release_committed (
	pgm_rxw_t*		r
	)
{
	if (r->committed_count == 0)		/* first call to read */
	{
		g_trace ("no commit packets to release");
		return PGM_RXW_OK;
	}

	g_assert( !pgm_rxw_empty(r) );

	pgm_rxw_packet_t* pp = RXW_PACKET(r, r->commit_trail);
	while ( r->committed_count && pp->state == PGM_PKT_COMMIT_DATA_STATE )
	{
		g_trace ("releasing commit sqn %u", pp->sequence_number);
		pp->state = PGM_PKT_PARITY_DATA_STATE;
		r->committed_count--;
		r->parity_data_count++;
		r->commit_trail++;
		pp = RXW_PACKET(r, r->commit_trail);
	}

	g_assert( r->committed_count == 0 );

	return PGM_RXW_OK;
}

/* used to flush completed transmission groups of any parity-data state packets.
 */
int
pgm_rxw_free_committed (
	pgm_rxw_t*		r
	)
{
	if ( r->parity_data_count == 0 ) {
		g_trace ("no parity-data packets free'd");
		return PGM_RXW_OK;
	}

	g_assert( r->commit_trail != r->trail );

/* calculate transmission group at commit trailing edge */
	const guint32 tg_sqn_mask = 0xffffffff << r->tg_sqn_shift;
	const guint32 tg_sqn = r->commit_trail & tg_sqn_mask;
	const guint32 pkt_sqn = r->commit_trail & ~tg_sqn_mask;

	guint32 new_rx_trail = tg_sqn;
	if (pkt_sqn == r->tg_size - 1)	/* end of group */
		new_rx_trail++;

	pgm_rxw_packet_t* pp = RXW_PACKET(r, r->trail);
	while ( new_rx_trail != r->trail )
	{
		g_trace ("free committed sqn %u", pp->sequence_number);
		g_assert( pp->state == PGM_PKT_PARITY_DATA_STATE );
		pgm_rxw_pop_trail (r);
		pp = RXW_PACKET(r, r->trail);
	}

	return PGM_RXW_OK;
}

int
pgm_rxw_pkt_state_unlink (
	pgm_rxw_t*		r,
	pgm_rxw_packet_t*	rp
	)
{
	ASSERT_RXW_BASE_INVARIANT(r);
	g_assert ( rp != NULL );

/* remove from state queues */
	GQueue* queue = NULL;

	switch (rp->state) {
	case PGM_PKT_BACK_OFF_STATE:  queue = r->backoff_queue; break;
	case PGM_PKT_WAIT_NCF_STATE:  queue = r->wait_ncf_queue; break;
	case PGM_PKT_WAIT_DATA_STATE: queue = r->wait_data_queue; break;
	case PGM_PKT_HAVE_DATA_STATE:
	case PGM_PKT_HAVE_PARITY_STATE:
	case PGM_PKT_COMMIT_DATA_STATE:
	case PGM_PKT_PARITY_DATA_STATE:
	case PGM_PKT_LOST_DATA_STATE:
		break;

	default:
		g_critical ("rp->state = %i", rp->state);
		g_assert_not_reached();
		break;
	}

	if (queue)
	{
#ifdef RXW_DEBUG
		guint original_length = queue->length;
#endif
		g_queue_unlink (queue, &rp->link_);
		rp->link_.prev = rp->link_.next = NULL;
#ifdef RXW_DEBUG
		g_assert (queue->length == original_length - 1);
#endif
	}

	rp->state = PGM_PKT_ERROR_STATE;

	ASSERT_RXW_BASE_INVARIANT(r);
	return PGM_RXW_OK;
}

/* similar to rxw_pkt_free1 but ignore the data payload, used to transfer
 * ownership upstream.
 */

static inline int
pgm_rxw_pkt_remove1 (
	pgm_rxw_t*		r,
	pgm_rxw_packet_t*	rp
	)
{
	ASSERT_RXW_BASE_INVARIANT(r);
	g_assert ( rp != NULL );

	if (rp->data)
	{
		rp->data = NULL;
	}

//	g_slice_free1 (sizeof(pgm_rxw_t), rp);
	g_static_mutex_lock (r->trash_mutex);
	g_trash_stack_push (r->trash_packet, rp);
	g_static_mutex_unlock (r->trash_mutex);

	ASSERT_RXW_BASE_INVARIANT(r);
	return PGM_RXW_OK;
}

static inline int
pgm_rxw_pkt_data_free1 (
	pgm_rxw_t*		r,
	gpointer		packet
	)
{
//	g_slice_free1 (rp->length, rp->data);
	g_static_mutex_lock (r->trash_mutex);
	g_trash_stack_push (r->trash_data, packet);
	g_static_mutex_unlock (r->trash_mutex);
	return PGM_RXW_OK;
}

static inline int
pgm_rxw_data_free1 (
	pgm_rxw_t*		r,
	pgm_rxw_packet_t*	rp
	)
{
	pgm_rxw_pkt_data_free1 (r, rp->data);
	rp->data = NULL;

	return PGM_RXW_OK;
}

static inline int
pgm_rxw_pkt_free1 (
	pgm_rxw_t*		r,
	pgm_rxw_packet_t*	rp
	)
{
	ASSERT_RXW_BASE_INVARIANT(r);
	g_assert ( rp != NULL );

	if (rp->data)
	{
		pgm_rxw_data_free1 (r, rp);
	}

//	g_slice_free1 (sizeof(pgm_rxw_t), rp);
	g_static_mutex_lock (r->trash_mutex);
	g_trash_stack_push (r->trash_packet, rp);
	g_static_mutex_unlock (r->trash_mutex);

	ASSERT_RXW_BASE_INVARIANT(r);
	return PGM_RXW_OK;
}

/* peek contents of of window entry, allow writing of data and parity members
 * to store temporary repair data.
 */
int
pgm_rxw_peek (
	pgm_rxw_t*	r,
	guint32		sequence_number,
	struct pgm_opt_fragment**	opt_fragment,
	gpointer*	data,
	guint16*	length,			/* matched to underlying type size */
	gboolean*	is_parity
	)
{
	ASSERT_RXW_BASE_INVARIANT(r);

/* already committed */
	if ( pgm_uint32_lt (sequence_number, r->trail) )
	{
		return PGM_RXW_DUPLICATE;
	}

/* not in window */
	if ( !IN_TXW(r, sequence_number) )
	{
		return PGM_RXW_NOT_IN_TXW;
	}

	if ( !ABS_IN_RXW(r, sequence_number) )
	{
		return PGM_RXW_ADVANCED_WINDOW;
	}

/* check if window is not empty */
	g_assert ( !pgm_rxw_empty (r) );

	pgm_rxw_packet_t* rp = RXW_PACKET(r, sequence_number);

	*opt_fragment	= &rp->opt_fragment;
	*data		= rp->data;
	*length		= rp->length;
	*is_parity	= rp->state == PGM_PKT_HAVE_PARITY_STATE;

	ASSERT_RXW_BASE_INVARIANT(r);
	return PGM_RXW_OK;
}

/* overwrite an existing packet entry with parity repair data.
 */
int
pgm_rxw_push_nth_parity (
	pgm_rxw_t*	r,
	guint32		sequence_number,
	guint32		trail,
	struct pgm_opt_fragment* opt_fragment,			/* in network order */
	gpointer	data,
	guint16		length,
	pgm_time_t	nak_rb_expiry
	)
{
	ASSERT_RXW_BASE_INVARIANT(r);

/* advances window */
	if ( !ABS_IN_RXW(r, sequence_number) )
	{
		pgm_rxw_window_update (r, trail, r->lead, r->tg_size, r->tg_sqn_shift, nak_rb_expiry);
	}

/* check if window is not empty */
	g_assert ( !pgm_rxw_empty (r) );
	g_assert ( ABS_IN_RXW(r, sequence_number) );

	pgm_rxw_packet_t* rp = RXW_PACKET(r, sequence_number);

/* cannot push parity over original data or existing parity */
	g_assert ( rp->state == PGM_PKT_BACK_OFF_STATE ||
		   rp->state == PGM_PKT_WAIT_NCF_STATE ||
		   rp->state == PGM_PKT_WAIT_DATA_STATE ||
		   rp->state == PGM_PKT_LOST_DATA_STATE );

	pgm_rxw_pkt_state_unlink (r, rp);

	if (opt_fragment) {
		memcpy (&rp->opt_fragment, opt_fragment, sizeof(struct pgm_opt_fragment));
	}
	rp->data	= data;
	rp->length	= length;
	rp->state	= PGM_PKT_HAVE_PARITY_STATE;

	r->parity_count++;

	ASSERT_RXW_BASE_INVARIANT(r);
	return PGM_RXW_OK;
}

/* overwrite parity-data with a calculated repair payload
 */
int
pgm_rxw_push_nth_repair (
	pgm_rxw_t*	r,
	guint32		sequence_number,
	guint32		trail,
	struct pgm_opt_fragment* opt_fragment,			/* in network order */
	gpointer	data,
	guint16		length,
	pgm_time_t	nak_rb_expiry
	)
{
	ASSERT_RXW_BASE_INVARIANT(r);

/* advances window */
	if ( !ABS_IN_RXW(r, sequence_number) )
	{
		pgm_rxw_window_update (r, trail, r->lead, r->tg_size, r->tg_sqn_shift, nak_rb_expiry);
	}

/* check if window is not empty */
	g_assert ( !pgm_rxw_empty (r) );
	g_assert ( ABS_IN_RXW(r, sequence_number) );

	pgm_rxw_packet_t* rp = RXW_PACKET(r, sequence_number);

	g_assert ( rp->state == PGM_PKT_HAVE_PARITY_STATE );

/* return parity block */
	pgm_rxw_data_free1 (r, rp);

	r->fragment_count++;

	if (opt_fragment) {
		memcpy (&rp->opt_fragment, opt_fragment, sizeof(struct pgm_opt_fragment));
		g_assert( g_ntohl (rp->of_apdu_len) > 0 );
	}
	rp->data	= data;
	rp->length	= length;
	rp->state	= PGM_PKT_HAVE_DATA_STATE;

	r->parity_count--;

	ASSERT_RXW_BASE_INVARIANT(r);
	return PGM_RXW_OK;
}

/* remove from leading edge of ahead side of receive window */
static int
pgm_rxw_pop_lead (
	pgm_rxw_t*	r
	)
{
/* check if window is not empty */
	ASSERT_RXW_BASE_INVARIANT(r);
	g_assert ( !pgm_rxw_empty (r) );

	pgm_rxw_packet_t* rp = RXW_PACKET(r, r->lead);

/* cleanup state counters */
	switch (rp->state) {
	case PGM_PKT_LOST_DATA_STATE:
		r->lost_count--;
		break;

	case PGM_PKT_HAVE_DATA_STATE:
		r->fragment_count--;
		break;

	case PGM_PKT_HAVE_PARITY_STATE:
		r->parity_count--;
		break;

	case PGM_PKT_COMMIT_DATA_STATE:
		r->committed_count--;
		break;

	case PGM_PKT_PARITY_DATA_STATE:
		r->parity_data_count--;
		break;

	default: break;
	}

	pgm_rxw_pkt_state_unlink (r, rp);
	pgm_rxw_pkt_free1 (r, rp);
	RXW_SET_PACKET(r, r->lead, NULL);

	r->lead--;

	ASSERT_RXW_BASE_INVARIANT(r);
	return PGM_RXW_OK;
}

/* remove from trailing edge of non-contiguous receive window causing data loss */
static inline int
pgm_rxw_pop_trail (
	pgm_rxw_t*	r
	)
{
/* check if window is not empty */
	ASSERT_RXW_BASE_INVARIANT(r);
	g_assert ( !pgm_rxw_empty (r) );

	pgm_rxw_packet_t* rp = RXW_PACKET(r, r->trail);

/* cleanup state counters */
	switch (rp->state) {
	case PGM_PKT_LOST_DATA_STATE:
		r->lost_count--;
		break;

	case PGM_PKT_HAVE_DATA_STATE:
		r->fragment_count--;
		break;

	case PGM_PKT_HAVE_PARITY_STATE:
		r->parity_count--;
		break;

	case PGM_PKT_COMMIT_DATA_STATE:
		r->committed_count--;
		break;

	case PGM_PKT_PARITY_DATA_STATE:
		r->parity_data_count--;
		break;

	default: break;
	}

	pgm_rxw_pkt_state_unlink (r, rp);
	pgm_rxw_pkt_free1 (r, rp);
	RXW_SET_PACKET(r, r->trail, NULL);

/* advance trailing pointers as necessary */
	if (r->trail++ == r->commit_trail)
		if (r->commit_trail++ == r->commit_lead)
			r->commit_lead++;

	ASSERT_RXW_BASE_INVARIANT(r);
	return PGM_RXW_OK;
}

/* update receiving window with new trailing and leading edge parameters of transmit window
 * can generate data loss by excluding outstanding NAK requests.
 *
 * returns number of place holders (NAKs) generated
 */
int
pgm_rxw_window_update (
	pgm_rxw_t*	r,
	guint32		txw_trail,
	guint32		txw_lead,
	guint32		tg_size,		/* transmission group size, 1 = no groups */
	guint		tg_sqn_shift,		/*			    0 = no groups */
	pgm_time_t	nak_rb_expiry
	)
{
	ASSERT_RXW_BASE_INVARIANT(r);
	ASSERT_RXW_POINTER_INVARIANT(r);

	guint naks = 0;
	guint dropped = 0;

/* SPM is first message seen, define new window parameters */
	if (!r->is_window_defined)
	{
		g_trace ("SPM defining receive window");

		r->lead = txw_lead;
		r->commit_trail = r->commit_lead = r->rxw_trail = r->rxw_trail_init = r->trail = r->lead + 1;

		r->tg_size = tg_size;
		r->tg_sqn_shift = tg_sqn_shift;

		r->is_rxw_constrained = TRUE;
		r->is_window_defined = TRUE;

		return 0;
	}

	if ( pgm_uint32_gt (txw_lead, r->lead) )
	{
/* check bounds of commit window */
		guint32 new_commit_sqns = ( 1 + txw_lead ) - r->commit_trail;
		if ( !pgm_rxw_commit_empty (r) &&
		     (new_commit_sqns > pgm_rxw_len (r)) )
		{
			guint32 constrained_lead = r->commit_trail + pgm_rxw_len (r) - 1;
			g_trace ("constraining advertised lead %u to commit window, new lead %u",
				txw_lead, constrained_lead);
			txw_lead = constrained_lead;
		}

		g_trace ("advancing lead to %u", txw_lead);

		if ( r->lead != txw_lead)
		{
/* generate new naks, should rarely if ever occur? */
	
			while ( r->lead != txw_lead )
			{
				if ( pgm_rxw_full(r) )
				{
					dropped++;
//					g_trace ("dropping #%u due to full window.", r->trail);

					pgm_rxw_pop_trail (r);
					r->is_waiting = TRUE;
				}

				r->lead++;

				pgm_rxw_packet_t* ph = pgm_rxw_alloc0_packet(r);
				ph->link_.data		= ph;
				ph->sequence_number     = r->lead;
				ph->nak_rb_expiry	= nak_rb_expiry;
				ph->state		= PGM_PKT_BACK_OFF_STATE;
				ph->t0			= pgm_time_now;

				RXW_SET_PACKET(r, ph->sequence_number, ph);
				g_trace ("adding placeholder #%u", ph->sequence_number);

/* send nak by sending to end of expiry list */
				g_queue_push_head_link (r->backoff_queue, &ph->link_);
				naks++;
			}
		}
	}
	else
	{
		g_trace ("lead not advanced.");

		if (txw_lead != r->lead)
		{
			g_trace ("lead stepped backwards, ignoring: %u -> %u.", r->lead, txw_lead);
		}
	}

	if ( r->is_rxw_constrained && SLIDINGWINDOW_GT(r, txw_trail, r->rxw_trail_init) )
	{
		g_trace ("constraint removed on trail.");
		r->is_rxw_constrained = FALSE;
	}

	if ( !r->is_rxw_constrained && SLIDINGWINDOW_GT(r, txw_trail, r->rxw_trail) )
	{
		g_trace ("advancing rxw_trail to %u", txw_trail);
		r->rxw_trail = txw_trail;

		if (SLIDINGWINDOW_GT(r, r->rxw_trail, r->trail))
		{
			g_trace ("advancing trail to rxw_trail");

/* jump remaining sequence numbers if window is empty */
			if ( pgm_rxw_empty(r) )
			{
				const guint32 distance = ( (gint32)(r->rxw_trail) - (gint32)(r->trail) );

				dropped  += distance;
				r->commit_trail = r->commit_lead = r->trail += distance;
				r->lead  += distance;
			}
			else
			{
/* mark lost all non-received sequence numbers between commit lead and new rxw_trail */
				for (guint32 sequence_number = r->commit_lead;
				     IN_TXW(r, sequence_number) && SLIDINGWINDOW_GT(r, r->rxw_trail, sequence_number);
				     sequence_number++)
				{
					pgm_rxw_packet_t* rp = RXW_PACKET(r, sequence_number);
					if (rp->state == PGM_PKT_BACK_OFF_STATE ||
					    rp->state == PGM_PKT_WAIT_NCF_STATE ||
					    rp->state == PGM_PKT_WAIT_DATA_STATE)
					{
						dropped++;
						pgm_rxw_mark_lost (r, sequence_number);
					}
				}
			}
		} /* trail > commit_lead */
	}
	else
	{
		g_trace ("rxw_trail not advanced.");

		if (!r->is_rxw_constrained)
		{
			if (txw_trail != r->rxw_trail)
			{
				g_trace ("rxw_trail stepped backwards, ignoring.");
			}
		}
	}

	if (dropped)
	{
		g_trace ("dropped %u messages due to full window.", dropped);
		r->cumulative_losses += dropped;
	}

	if (r->tg_size != tg_size) {
		g_trace ("window transmission group size updated %i -> %i.", r->tg_size, tg_size);
		r->tg_size = tg_size;
		r->tg_sqn_shift = tg_sqn_shift;
	}

	g_trace ("window ( rxw_trail %u rxw_trail_init %u trail %u commit_trail %u commit_lead %u lead %u rxw_sqns %u )",
		r->rxw_trail, r->rxw_trail_init, r->trail, r->commit_trail, r->commit_lead, r->lead, pgm_rxw_sqns(r));

	ASSERT_RXW_BASE_INVARIANT(r);
	ASSERT_RXW_POINTER_INVARIANT(r);
	return naks;
}

/* mark a packet lost due to failed recovery, this either advances the trailing edge
 * or creates a hole to later skip.
 */

int
pgm_rxw_mark_lost (
	pgm_rxw_t*	r,
	guint32		sequence_number
	)
{
	ASSERT_RXW_BASE_INVARIANT(r);
	ASSERT_RXW_POINTER_INVARIANT(r);

	pgm_rxw_packet_t* rp = RXW_PACKET(r, sequence_number);

/* invalid if we already have data or parity */
	g_assert( rp->state == PGM_PKT_BACK_OFF_STATE ||
		  rp->state == PGM_PKT_WAIT_NCF_STATE ||
		  rp->state == PGM_PKT_WAIT_DATA_STATE );

/* remove current state */
	pgm_rxw_pkt_state_unlink (r, rp);

	rp->state = PGM_PKT_LOST_DATA_STATE;
	r->lost_count++;
	r->cumulative_losses++;
	r->is_waiting = TRUE;

	ASSERT_RXW_BASE_INVARIANT(r);
	ASSERT_RXW_POINTER_INVARIANT(r);
	return PGM_RXW_OK;
}

/* received a uni/multicast ncf, search for a matching nak & tag or extend window if
 * beyond lead
 *
 * returns:
 *	PGM_RXW_WINDOW_UNDEFINED	- still waiting for SPM 
 *	PGM_RXW_DUPLICATE
 *	PGM_RXW_CREATED_PLACEHOLDER
 */

int
pgm_rxw_ncf (
	pgm_rxw_t*	r,
	guint32		sequence_number,
	pgm_time_t	nak_rdata_expiry,
	pgm_time_t	nak_rb_expiry
	)
{
	int retval = PGM_RXW_UNKNOWN;

	ASSERT_RXW_BASE_INVARIANT(r);
	ASSERT_RXW_POINTER_INVARIANT(r);

	g_trace ("pgm_rxw_ncf(#%u)", sequence_number);

	if (!r->is_window_defined) {
		retval = PGM_RXW_WINDOW_UNDEFINED;
		goto out;
	}

/* already committed */
	if ( pgm_uint32_lt (sequence_number, r->commit_lead) )
	{
		g_trace ("ncf #%u: already committed, discarding.", sequence_number);
		retval = PGM_RXW_DUPLICATE;
		goto out;
	}

	pgm_rxw_packet_t* rp = RXW_PACKET(r, sequence_number);

	if (rp)
	{
		switch (rp->state) {
/* already received ncf */
		case PGM_PKT_WAIT_DATA_STATE:
		{
			ASSERT_RXW_BASE_INVARIANT(r);
			ASSERT_RXW_POINTER_INVARIANT(r);
			g_trace ("ncf ignored as sequence number already in wait_data_state.");
			retval = PGM_RXW_DUPLICATE;
			goto out;
		}

		case PGM_PKT_BACK_OFF_STATE:
		case PGM_PKT_WAIT_NCF_STATE:
			rp->nak_rdata_expiry = nak_rdata_expiry;
			g_trace ("nak_rdata_expiry in %f seconds.", pgm_to_secsf( rp->nak_rdata_expiry - pgm_time_now ));
			break;

/* ignore what we have or have not */
		case PGM_PKT_HAVE_DATA_STATE:
		case PGM_PKT_HAVE_PARITY_STATE:
		case PGM_PKT_COMMIT_DATA_STATE:
		case PGM_PKT_PARITY_DATA_STATE:
		case PGM_PKT_LOST_DATA_STATE:
			g_trace ("ncf ignored as sequence number already closed.");
			retval = PGM_RXW_DUPLICATE;
			goto out;

		default:
			g_assert_not_reached();
		}

		pgm_rxw_pkt_state_unlink (r, rp);
		rp->state = PGM_PKT_WAIT_DATA_STATE;
		g_queue_push_head_link (r->wait_data_queue, &rp->link_);

		retval = PGM_RXW_CREATED_PLACEHOLDER;
		goto out;
	}

/* not an expected ncf, extend receive window to pre-empt loss detection */
	if ( !IN_TXW(r, sequence_number) )
	{
		g_trace ("ncf #%u not in tx window, discarding.", sequence_number);
		retval = PGM_RXW_NOT_IN_TXW;
		goto out;
	}

	g_trace ("ncf extends lead #%u to #%u", r->lead, sequence_number);

/* mark all sequence numbers to ncf # in BACK-OFF_STATE */

	guint dropped = 0;
	
/* check bounds of commit window */
	guint32 new_commit_sqns = ( 1 + sequence_number ) - r->commit_trail;
	if ( !pgm_rxw_commit_empty (r) &&
		(new_commit_sqns > pgm_rxw_len (r)) )
	{
		pgm_rxw_window_update (r, r->rxw_trail, sequence_number, r->tg_size, r->tg_sqn_shift, nak_rb_expiry);
		retval = PGM_RXW_CREATED_PLACEHOLDER;
		goto out;
	}

	r->lead++;

	while (r->lead != sequence_number)
	{
		if ( pgm_rxw_full(r) )
		{
			dropped++;
//			g_trace ("dropping #%u due to full window.", r->trail);

			pgm_rxw_pop_trail (r);
			r->is_waiting = TRUE;
		}

		pgm_rxw_packet_t* ph = pgm_rxw_alloc0_packet(r);
		ph->link_.data		= ph;
		ph->sequence_number     = r->lead;
		ph->nak_rb_expiry	= nak_rb_expiry;
		ph->state		= PGM_PKT_BACK_OFF_STATE;
		ph->t0			= pgm_time_now;

		RXW_SET_PACKET(r, ph->sequence_number, ph);
		g_trace ("ncf: adding placeholder #%u", ph->sequence_number);

/* send nak by sending to end of expiry list */
		g_queue_push_head_link (r->backoff_queue, &ph->link_);

		r->lead++;
	}

/* create WAIT_DATA state placeholder for ncf # */

	g_assert ( r->lead == sequence_number );

	if ( pgm_rxw_full(r) )
	{
		dropped++;
//		g_trace ("dropping #%u due to full window.", r->trail);

		pgm_rxw_pop_trail (r);
		r->is_waiting = TRUE;
	}

	pgm_rxw_packet_t* ph = pgm_rxw_alloc0_packet(r);
	ph->link_.data		= ph;
	ph->sequence_number     = r->lead;
	ph->nak_rdata_expiry	= nak_rdata_expiry;
	ph->state		= PGM_PKT_WAIT_DATA_STATE;
	ph->t0			= pgm_time_now;
		
	RXW_SET_PACKET(r, ph->sequence_number, ph);
	g_trace ("ncf: adding placeholder #%u", ph->sequence_number);

/* do not send nak, simply add to ncf list */
	g_queue_push_head_link (r->wait_data_queue, &ph->link_);

	r->is_waiting = TRUE;

	if (dropped) {
		g_trace ("ncf: dropped %u messages due to full window.", dropped);
		r->cumulative_losses += dropped;
	}

	retval = PGM_RXW_CREATED_PLACEHOLDER;

out:
	ASSERT_RXW_BASE_INVARIANT(r);
	ASSERT_RXW_POINTER_INVARIANT(r);
	return retval;
}

/* state string helper
 */

const char*
pgm_rxw_state_string (
	pgm_pkt_state_e		state
	)
{
	const char* c;

	switch (state) {
	case PGM_PKT_BACK_OFF_STATE:	c = "PGM_PKT_BACK_OFF_STATE"; break;
	case PGM_PKT_WAIT_NCF_STATE:	c = "PGM_PKT_WAIT_NCF_STATE"; break;
	case PGM_PKT_WAIT_DATA_STATE:	c = "PGM_PKT_WAIT_DATA_STATE"; break;
	case PGM_PKT_HAVE_DATA_STATE:	c = "PGM_PKT_HAVE_DATA_STATE"; break;
	case PGM_PKT_HAVE_PARITY_STATE:	c = "PGM_PKT_HAVE_PARITY_STATE"; break;
	case PGM_PKT_COMMIT_DATA_STATE: c = "PGM_PKT_COMMIT_DATA_STATE"; break;
	case PGM_PKT_PARITY_DATA_STATE:	c = "PGM_PKT_PARITY_DATA_STATE"; break;
	case PGM_PKT_LOST_DATA_STATE:	c = "PGM_PKT_LOST_DATA_STATE"; break;
	case PGM_PKT_ERROR_STATE:	c = "PGM_PKT_ERROR_STATE"; break;
	default: c = "(unknown)"; break;
	}

	return c;
}

const char*
pgm_rxw_returns_string (
	pgm_rxw_returns_e	retval
	)
{
	const char* c;

	switch (retval) {
	case PGM_RXW_OK:			c = "PGM_RXW_OK"; break;
	case PGM_RXW_CREATED_PLACEHOLDER:	c = "PGM_RXW_CREATED_PLACEHOLDER"; break;
	case PGM_RXW_FILLED_PLACEHOLDER:	c = "PGM_RXW_FILLED_PLACEHOLDER"; break;
	case PGM_RXW_ADVANCED_WINDOW:		c = "PGM_RXW_ADVANCED_WINDOW"; break;
	case PGM_RXW_NOT_IN_TXW:		c = "PGM_RXW_NOT_IN_TXW"; break;
	case PGM_RXW_WINDOW_UNDEFINED:		c = "PGM_RXW_WINDOW_UNDEFINED"; break;
	case PGM_RXW_DUPLICATE:			c = "PGM_RXW_DUPLICATE"; break;
	case PGM_RXW_APDU_LOST:			c = "PGM_RXW_APDU_LOST"; break;
	case PGM_RXW_MALFORMED_APDU:		c = "PGM_RXW_MALFORMED_APDU"; break;
	case PGM_RXW_UNKNOWN:			c = "PGM_RXW_UNKNOWN"; break;
	default: c = "(unknown)"; break;
	}

	return c;
}

/* eof */
