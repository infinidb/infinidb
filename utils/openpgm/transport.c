/* vim:ts=8:sts=8:sw=4:noai:noexpandtab
 *
 * PGM transport: manage incoming & outgoing sockets with ambient SPMs, 
 * transmit & receive windows.
 *
 * Copyright (c) 2006-2008 Miru Limited.
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
#include <getopt.h>
#include <limits.h>
#include <netdb.h>
#include <poll.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <net/if.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <netinet/udp.h>
#ifdef CONFIG_HAVE_EPOLL
#	include <sys/epoll.h>
#endif
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <arpa/inet.h>

#include <glib.h>

#include "pgm/transport.h"
#include "pgm/if.h"
#include "pgm/packet.h"
#include "pgm/txwi.h"
#include "pgm/rxwi.h"
#include "pgm/rate_control.h"
#include "pgm/sn.h"
#include "pgm/timer.h"
#include "pgm/checksum.h"
#include "pgm/reed_solomon.h"
#include "pgm/err.h"

//#define TRANSPORT_DEBUG
//#define TRANSPORT_SPM_DEBUG

#ifndef TRANSPORT_DEBUG
#	define g_trace(m,...)		while (0)
#else
#include <ctype.h>
#	ifdef TRANSPORT_SPM_DEBUG
#		define g_trace(m,...)		g_debug(__VA_ARGS__)
#	else
#		define g_trace(m,...)		do { if (strcmp((m),"SPM")) { g_debug(__VA_ARGS__); } } while (0)
#	endif
#endif


/* internal: Glib event loop GSource of spm & rx state timers */
struct pgm_timer_t {
	GSource		source;
	pgm_time_t	expiration;
	pgm_transport_t* transport;
};

typedef struct pgm_timer_t pgm_timer_t;


/* callback for pgm timer events */
typedef int (*pgm_timer_callback)(pgm_transport_t*);


/* global locals */

static int ipproto_pgm = IPPROTO_PGM;

GStaticRWLock pgm_transport_list_lock = G_STATIC_RW_LOCK_INIT;		/* list of all transports for admin interfaces */
GSList* pgm_transport_list = NULL;

/* helpers for pgm_peer_t */
#define next_nak_rb_expiry(r)       ( ((pgm_rxw_packet_t*)(r)->backoff_queue->tail->data)->nak_rb_expiry )
#define next_nak_rpt_expiry(r)      ( ((pgm_rxw_packet_t*)(r)->wait_ncf_queue->tail->data)->nak_rpt_expiry )
#define next_nak_rdata_expiry(r)    ( ((pgm_rxw_packet_t*)(r)->wait_data_queue->tail->data)->nak_rdata_expiry )


static GSource* pgm_create_timer (pgm_transport_t*);
static int pgm_add_timer_full (pgm_transport_t*, gint);
static int pgm_add_timer (pgm_transport_t*);

static gboolean pgm_timer_prepare (GSource*, gint*);
static gboolean pgm_timer_check (GSource*);
static gboolean pgm_timer_dispatch (GSource*, GSourceFunc, gpointer);

static GSourceFuncs g_pgm_timer_funcs = {
	.prepare		= pgm_timer_prepare,
	.check			= pgm_timer_check,
	.dispatch		= pgm_timer_dispatch,
	.finalize		= NULL,
	.closure_callback	= NULL
};

static int send_spm_unlocked (pgm_transport_t*);
static inline int send_spm (pgm_transport_t*);
static int send_spmr (pgm_peer_t*);
static int send_nak (pgm_peer_t*, guint32);
static int send_parity_nak (pgm_peer_t*, guint, guint);
static int send_nak_list (pgm_peer_t*, pgm_sqn_list_t*);
static int send_ncf (pgm_transport_t*, struct sockaddr*, struct sockaddr*, guint32, gboolean);
static int send_ncf_list (pgm_transport_t*, struct sockaddr*, struct sockaddr*, pgm_sqn_list_t*, gboolean);

static void nak_rb_state (pgm_peer_t*);
static void nak_rpt_state (pgm_peer_t*);
static void nak_rdata_state (pgm_peer_t*);
static void check_peer_nak_state (pgm_transport_t*);
static pgm_time_t min_nak_expiry (pgm_time_t, pgm_transport_t*);

static int send_rdata (pgm_transport_t*, guint32, gpointer, gsize, gboolean);

static inline pgm_peer_t* pgm_peer_ref (pgm_peer_t*);
static inline void pgm_peer_unref (pgm_peer_t*);

static gboolean on_nak_notify (GIOChannel*, GIOCondition, gpointer);
static gboolean on_timer_notify (GIOChannel*, GIOCondition, gpointer);
static gboolean on_timer_shutdown (GIOChannel*, GIOCondition, gpointer);

static int on_spm (pgm_peer_t*, struct pgm_header*, gpointer, gsize);
static int on_spmr (pgm_transport_t*, pgm_peer_t*, struct pgm_header*, gpointer, gsize);
static int on_nak (pgm_transport_t*, struct pgm_header*, gpointer, gsize);
static int on_peer_nak (pgm_peer_t*, struct pgm_header*, gpointer, gsize);
static int on_ncf (pgm_peer_t*, struct pgm_header*, gpointer, gsize);
static int on_nnak (pgm_transport_t*, struct pgm_header*, gpointer, gsize);
static int on_odata (pgm_peer_t*, struct pgm_header*, gpointer, gsize);
static int on_rdata (pgm_peer_t*, struct pgm_header*, gpointer, gsize);

static gssize pgm_transport_send_one (pgm_transport_t*, gpointer, gsize, int);
static gssize pgm_transport_send_one_copy (pgm_transport_t*, gconstpointer, gsize, int);

static gboolean g_source_remove_context (GMainContext*, guint);
static int get_opt_fragment (struct pgm_opt_header*, struct pgm_opt_fragment**);


/* re-entrant form of pgm_print_tsi()
 */
int
pgm_print_tsi_r (
	const pgm_tsi_t*	tsi,
	char*			buf,
	gsize			bufsize
	)
{
	g_return_val_if_fail (tsi != NULL, -EINVAL);
	g_return_val_if_fail (buf != NULL, -EINVAL);

	const guint8* gsi = (const guint8*)tsi;
	guint16 source_port = tsi->sport;
	snprintf(buf, bufsize, "%i.%i.%i.%i.%i.%i.%i",
		gsi[0], gsi[1], gsi[2], gsi[3], gsi[4], gsi[5], g_ntohs (source_port));
	return 0;
}

/* transform TSI to ASCII string form.
 *
 * on success, returns pointer to ASCII string.  on error, returns NULL.
 */
gchar*
pgm_print_tsi (
	const pgm_tsi_t*	tsi
	)
{
	g_return_val_if_fail (tsi != NULL, NULL);

	static char buf[PGM_TSISTRLEN];
	pgm_print_tsi_r (tsi, buf, sizeof(buf));
	return buf;
}

/* create hash value of TSI for use with GLib hash tables.
 *
 * on success, returns a hash value corresponding to the TSI.  on error, fails
 * on assert.
 */
guint
pgm_tsi_hash (
	gconstpointer v
        )
{
	g_assert( v != NULL );
	const pgm_tsi_t* tsi = v;
	char buf[PGM_TSISTRLEN];
	int valid = pgm_print_tsi_r(tsi, buf, sizeof(buf));
	g_assert( valid == 0 );
	return g_str_hash( buf );
}

/* compare two transport session identifier TSI values.
 *
 * returns TRUE if they are equal, FALSE if they are not.
 */
gboolean
pgm_tsi_equal (
	gconstpointer   v,
	gconstpointer   v2
        )
{
	return memcmp (v, v2, sizeof(struct pgm_tsi_t)) == 0;
}

gsize
pgm_transport_pkt_offset (
	gboolean		can_fragment
	)
{
	return can_fragment ? ( sizeof(struct pgm_header)
			      + sizeof(struct pgm_data)
			      + sizeof(struct pgm_opt_length)
	                      + sizeof(struct pgm_opt_header)
			      + sizeof(struct pgm_opt_fragment) )
			    : ( sizeof(struct pgm_header) + sizeof(struct pgm_data) );
}

/* memory allocation for zero-copy packetv api
 * returned packets are offset to the payload location
 */
gpointer
pgm_packetv_alloc (
	pgm_transport_t*	transport,
	gboolean		can_fragment
	)
{
	g_assert (transport);
	return (guint8*)pgm_txw_alloc (transport->txw) + pgm_transport_pkt_offset (can_fragment);
}

/* return memory to the slab
 */
void
pgm_packetv_free1 (
	pgm_transport_t*	transport,
	gpointer		tsdu,
	gboolean		can_fragment
	)
{
	g_assert (transport);
	gpointer pkt = (guint8*)tsdu - pgm_transport_pkt_offset (can_fragment);
	g_static_rw_lock_writer_lock (&transport->txw_lock);
	g_trash_stack_push (&((pgm_txw_t*)transport->txw)->trash_data, pkt);
	g_static_rw_lock_writer_unlock (&transport->txw_lock);
}

/* fast log base 2 of power of 2
 */

guint
pgm_power2_log2 (
	guint		v
	)
{
	static const unsigned int b[] = {0xAAAAAAAA, 0xCCCCCCCC, 0xF0F0F0F0, 0xFF00FF00, 0xFFFF0000};
	unsigned int r = (v & b[0]) != 0;
	for (int i = 4; i > 0; i--) {
		r |= ((v & b[i]) != 0) << i;
	}
	return r;
}

/* calculate NAK_RB_IVL as random time interval 1 - NAK_BO_IVL.
 */
static inline guint32
nak_rb_ivl (
	pgm_transport_t* transport
	)
{
	return g_rand_int_range (transport->rand_, 1 /* us */, transport->nak_bo_ivl);
}

/* locked and rate regulated sendto
 *
 * on success, returns number of bytes sent.  on error, -1 is returned, and
 * errno set appropriately.
 */
static inline gssize
pgm_sendto (
	pgm_transport_t*	transport,
	gboolean		use_rate_limit,
	gboolean		use_router_alert,
	const void*		buf,
	gsize			len,
	int			flags,
	const struct sockaddr*	to,
	gsize			tolen
	)
{
	g_assert( transport );
	g_assert( buf );
	g_assert( len > 0 );
	g_assert( to );
	g_assert( tolen > 0 );

	GStaticMutex* mutex = use_router_alert ? &transport->send_with_router_alert_mutex : &transport->send_mutex;
	int sock = use_router_alert ? transport->send_with_router_alert_sock : transport->send_sock;

	if (use_rate_limit)
	{
		int check = pgm_rate_check (transport->rate_control, len, flags);
		if (check < 0 && errno == EAGAIN)
		{
			return (gssize)check;
		}
	}

	g_static_mutex_lock (mutex);

	ssize_t sent = sendto (sock, buf, len, flags, to, (socklen_t)tolen);
	if (	sent < 0 &&
		errno != ENETUNREACH &&		/* Network is unreachable */
		errno != EHOSTUNREACH &&	/* No route to host */
		!( errno == EAGAIN && flags & MSG_DONTWAIT )	/* would block on non-blocking send */
	   )
	{
/* poll for cleared socket */
		struct pollfd p = {
			.fd		= transport->send_sock,
			.events		= POLLOUT,
			.revents	= 0
				  };
		int ready = poll (&p, 1, 500 /* ms */);
		if (ready > 0)
		{
			sent = sendto (sock, buf, len, flags, to, (socklen_t)tolen);
			if ( sent < 0 )
			{
				g_warning ("sendto %s failed: %i/%s",
						inet_ntoa( ((const struct sockaddr_in*)to)->sin_addr ),
						errno,
						strerror (errno));
			}
		}
		else if (ready == 0)
		{
			g_warning ("sendto %s socket pollout timeout.",
					 inet_ntoa( ((const struct sockaddr_in*)to)->sin_addr ));
		}
		else
		{
			g_warning ("poll on blocked sendto %s socket failed: %i %s",
					inet_ntoa( ((const struct sockaddr_in*)to)->sin_addr ),
					errno,
					strerror (errno));
		}
	}

	g_static_mutex_unlock (mutex);

	return sent;
}

/* socket helper, for setting pipe ends non-blocking
 *
 * on success, returns 0.  on error, returns -1, and sets errno appropriately.
 */
int
pgm_set_nonblocking (
	int		filedes[2]
	)
{
	int retval = 0;

/* set write end non-blocking */
	int fd_flags = fcntl (filedes[1], F_GETFL);
	if (fd_flags < 0) {
		retval = fd_flags;
		goto out;
	}
	retval = fcntl (filedes[1], F_SETFL, fd_flags | O_NONBLOCK);
	if (retval < 0) {
		retval = fd_flags;
		goto out;
	}
/* set read end non-blocking */
	fcntl (filedes[0], F_GETFL);
	if (fd_flags < 0) {
		retval = fd_flags;
		goto out;
	}
	retval = fcntl (filedes[0], F_SETFL, fd_flags | O_NONBLOCK);
	if (retval < 0) {
		retval = fd_flags;
		goto out;
	}

out:
	return retval;
}		

/* startup PGM engine, mainly finding PGM protocol definition, if any from NSS
 *
 * on success, returns 0.
 */
int
pgm_init (void)
{
	int retval = 0;

/* ensure threading enabled */
	if (!g_thread_supported ()) g_thread_init (NULL);

/* ensure timer enabled */
	if (!pgm_time_supported ()) pgm_time_init();


/* find PGM protocol id */

// TODO: fix valgrind errors
#ifdef CONFIG_HAVE_GETPROTOBYNAME_R
	char b[1024];
	struct protoent protobuf, *proto;
	int e = getprotobyname_r("pgm", &protobuf, b, sizeof(b), &proto);
	if (e != -1 && proto != NULL) {
		if (proto->p_proto != ipproto_pgm) {
			g_trace("INFO","Setting PGM protocol number to %i from /etc/protocols.", proto->p_proto);
			ipproto_pgm = proto->p_proto;
		}
	}
#else
	struct protoent *proto = getprotobyname("pgm");
	if (proto != NULL) {
		if (proto->p_proto != ipproto_pgm) {
			g_trace("INFO","Setting PGM protocol number to %i from /etc/protocols.", proto->p_proto);
			ipproto_pgm = proto->p_proto;
		}
	}
#endif

	return retval;
}

/* destroy a pgm_transport object and contents, if last transport also destroy
 * associated event loop
 *
 * TODO: clear up locks on destruction: 1: flushing, 2: destroying:, 3: destroyed.
 *
 * If application calls a function on the transport after destroy() it is a
 * programmer error: segv likely to occur on unlock.
 *
 * on success, returns 0.  if transport is invalid, or previously destroyed,
 * returns -EINVAL.
 */

int
pgm_transport_destroy (
	pgm_transport_t*	transport,
	gboolean		flush
	)
{
	g_return_val_if_fail (transport != NULL, -EINVAL);

	g_static_rw_lock_writer_lock (&pgm_transport_list_lock);
	pgm_transport_list = g_slist_remove (pgm_transport_list, transport);
	g_static_rw_lock_writer_unlock (&pgm_transport_list_lock);

	transport->is_open = FALSE;

/* rollback any pkt_dontwait APDU */
	if (transport->is_apdu_eagain)
	{
		((pgm_txw_t*)transport->txw)->lead = transport->pkt_dontwait_state.first_sqn - 1;
		g_static_rw_lock_writer_unlock (&transport->txw_lock);
		transport->is_apdu_eagain = FALSE;
	}

/* cleanup rdata-transmit channel in timer thread */
	if (transport->rdata_id > 0) {
		g_source_remove_context (transport->timer_context, transport->rdata_id);
	}
	if (transport->rdata_channel) {
		g_io_channel_unref (transport->rdata_channel);
	}

/* terminate & join internal thread */
	if (transport->notify_id > 0) {
		g_source_remove_context (transport->timer_context, transport->notify_id);
	}
	if (transport->notify_channel) {
		g_io_channel_unref (transport->notify_channel);
	}

	if (transport->timer_id > 0) {
		g_source_remove_context (transport->timer_context, transport->timer_id);
	}

#ifndef PGM_SINGLE_THREAD
	if (transport->timer_thread) {
		pgm_notify_send (&transport->timer_shutdown);
		g_thread_join (transport->timer_thread);
		transport->timer_thread = NULL;
	}
#endif /* !PGM_SINGLE_THREAD */

/* cleanup shutdown comms */
	if (transport->shutdown_channel) {
		g_io_channel_unref (transport->shutdown_channel);
	}

	g_static_mutex_lock (&transport->mutex);

/* assume lock from create() if not bound */
	if (transport->is_bound) {
		g_static_mutex_lock (&transport->send_mutex);
		g_static_mutex_lock (&transport->send_with_router_alert_mutex);
	}

/* flush data by sending heartbeat SPMs & processing NAKs until ambient */
	if (flush) {
	}

	if (transport->peers_hashtable) {
		g_hash_table_destroy (transport->peers_hashtable);
		transport->peers_hashtable = NULL;
	}
	if (transport->peers_list) {
		g_trace ("INFO","destroying peer data.");

		do {
			GList* next = transport->peers_list->next;
			pgm_peer_unref ((pgm_peer_t*)transport->peers_list->data);

			transport->peers_list = next;
		} while (transport->peers_list);
	}

/* clean up receiver trash stacks */
	if (transport->rx_data) {
		gpointer* p = NULL;
		while ( (p = g_trash_stack_pop (&transport->rx_data)) )
		{
			g_slice_free1 (transport->max_tpdu - transport->iphdr_len, p);
		}

		g_assert (transport->rx_data == NULL);
	}

	if (transport->rx_packet) {
		gpointer* p = NULL;
		while ( (p = g_trash_stack_pop (&transport->rx_packet)) )
		{
			g_slice_free1 (sizeof(pgm_rxw_packet_t), p);
		}

		g_assert (transport->rx_packet == NULL);
	}

	if (transport->txw) {
		g_trace ("INFO","destroying transmit window.");
		pgm_txw_shutdown (transport->txw);
		transport->txw = NULL;
	}

	if (transport->rate_control) {
		g_trace ("INFO","destroying rate control.");
		pgm_rate_destroy (transport->rate_control);
		transport->rate_control = NULL;
	}
	if (transport->recv_sock) {
		g_trace ("INFO","closing receive socket.");
		close(transport->recv_sock);
		transport->recv_sock = 0;
	}

	if (transport->send_sock) {
		g_trace ("INFO","closing send socket.");
		close(transport->send_sock);
		transport->send_sock = 0;
	}
	if (transport->send_with_router_alert_sock) {
		g_trace ("INFO","closing send with router alert socket.");
		close(transport->send_with_router_alert_sock);
		transport->send_with_router_alert_sock = 0;
	}

	if (transport->spm_heartbeat_interval) {
		g_free (transport->spm_heartbeat_interval);
		transport->spm_heartbeat_interval = NULL;
	}

	if (transport->rand_) {
		g_rand_free (transport->rand_);
		transport->rand_ = NULL;
	}

	pgm_notify_destroy (&transport->timer_notify);
	pgm_notify_destroy (&transport->timer_shutdown);
	pgm_notify_destroy (&transport->rdata_notify);
	pgm_notify_destroy (&transport->waiting_notify);

	g_static_rw_lock_free (&transport->peers_lock);

	g_static_rw_lock_free (&transport->txw_lock);

	if (transport->is_bound) {
		g_static_mutex_unlock (&transport->send_mutex);
		g_static_mutex_unlock (&transport->send_with_router_alert_mutex);
	}
	g_static_mutex_free (&transport->send_mutex);
	g_static_mutex_free (&transport->send_with_router_alert_mutex);

	g_static_mutex_unlock (&transport->mutex);
	g_static_mutex_free (&transport->mutex);

	if (transport->parity_buffer) {
		g_free (transport->parity_buffer);
		transport->parity_buffer = NULL;
	}
	if (transport->rs) {
		pgm_rs_destroy (transport->rs);
		transport->rs = NULL;
	}

	if (transport->rx_buffer) {
		g_free (transport->rx_buffer);
		transport->rx_buffer = NULL;
	}

	if (transport->piov) {
		g_free (transport->piov);
		transport->piov = NULL;
	}

	g_free (transport);

	g_trace ("INFO","finished.");
	return 0;
}

/* increase reference count for peer object
 *
 * on success, returns peer object.
 */
static inline pgm_peer_t*
pgm_peer_ref (
	pgm_peer_t*	peer
	)
{
	g_return_val_if_fail (peer != NULL, NULL);

	g_atomic_int_inc (&peer->ref_count);

	return peer;
}

/* decrease reference count of peer object, destroying on last reference.
 */
static inline void
pgm_peer_unref (
	pgm_peer_t*	peer
	)
{
	g_return_if_fail (peer != NULL);

	gboolean is_zero = g_atomic_int_dec_and_test (&peer->ref_count);

	if (G_UNLIKELY (is_zero))
	{
/* peer lock */
		g_static_mutex_free (&peer->mutex);

/* receive window */
		pgm_rxw_shutdown (peer->rxw);
		peer->rxw = NULL;

/* reed solomon state */
		if (peer->rs) {
			pgm_rs_destroy (peer->rs);
			peer->rs = NULL;
		}

/* object */
		g_free (peer);
	}
}

/* timer thread execution function.
 *
 * when thread loop is terminated, returns NULL, to be returned by
 * g_thread_join()
 */
static gpointer
pgm_timer_thread (
	gpointer		data
	)
{
	pgm_transport_t* transport = (pgm_transport_t*)data;

	transport->timer_context = g_main_context_new ();
	g_mutex_lock (transport->thread_mutex);
	transport->timer_loop = g_main_loop_new (transport->timer_context, FALSE);
	g_cond_signal (transport->thread_cond);
	g_mutex_unlock (transport->thread_mutex);

	g_trace ("INFO", "pgm_timer_thread entering event loop.");
	g_main_loop_run (transport->timer_loop);
	g_trace ("INFO", "pgm_timer_thread leaving event loop.");

/* cleanup */
	g_main_loop_unref (transport->timer_loop);
	g_main_context_unref (transport->timer_context);

	return NULL;
}

/* create a pgm_transport object.  create sockets that require superuser priviledges, if this is
 * the first instance also create a real-time priority receiving thread.  if interface ports
 * are specified then UDP encapsulation will be used instead of raw protocol.
 *
 * if send == recv only two sockets need to be created iff ip headers are not required (IPv6).
 *
 * all receiver addresses must be the same family.
 * interface and multiaddr must be the same family.
 * family cannot be AF_UNSPEC!
 *
 * returns 0 on success, or -1 on error and sets errno appropriately.
 */

#if ( AF_INET != PF_INET ) || ( AF_INET6 != PF_INET6 )
#error AF_INET and PF_INET are different values, the bananas are jumping in their pyjamas!
#endif

int
pgm_transport_create (
	pgm_transport_t**		transport_,
	pgm_gsi_t*			gsi,
	guint16				sport,		/* set to 0 to randomly select */
	guint16				dport,
	struct group_source_req*	recv_gsr,	/* receive port, multicast group & interface address */
	gsize				recv_len,
	struct group_source_req*	send_gsr	/* send ... */
	)
{
	g_return_val_if_fail (transport_ != NULL, -EINVAL);
	g_return_val_if_fail (gsi != NULL, -EINVAL);
	if (sport) g_return_val_if_fail (sport != dport, -EINVAL);
	g_return_val_if_fail (recv_gsr != NULL, -EINVAL);
	g_return_val_if_fail (recv_len > 0, -EINVAL);
	g_return_val_if_fail (recv_len <= IP_MAX_MEMBERSHIPS, -EINVAL);
	g_return_val_if_fail (send_gsr != NULL, -EINVAL);
	for (unsigned i = 0; i < recv_len; i++)
	{
		g_return_val_if_fail (pgm_sockaddr_family(&recv_gsr[i].gsr_group) == pgm_sockaddr_family(&recv_gsr[0].gsr_group), -EINVAL);
		g_return_val_if_fail (pgm_sockaddr_family(&recv_gsr[i].gsr_group) == pgm_sockaddr_family(&recv_gsr[i].gsr_source), -EINVAL);
	}
	g_return_val_if_fail (pgm_sockaddr_family(&send_gsr->gsr_group) == pgm_sockaddr_family(&send_gsr->gsr_source), -EINVAL);

	int retval = 0;
	pgm_transport_t* transport;

/* create transport object */
	transport = g_malloc0 (sizeof(pgm_transport_t));

/* transport defaults */
	transport->can_send_data = TRUE;
	transport->can_send_nak  = TRUE;
	transport->can_recv = TRUE;

/* regular send lock */
	g_static_mutex_init (&transport->send_mutex);

/* IP router alert send lock */
	g_static_mutex_init (&transport->send_with_router_alert_mutex);

/* timer lock */
	g_static_mutex_init (&transport->mutex);

/* transmit window read/write lock */
	g_static_rw_lock_init (&transport->txw_lock);

/* peer hash map & list lock */
	g_static_rw_lock_init (&transport->peers_lock);

/* lock tx until bound */
	g_static_mutex_lock (&transport->send_mutex);

	memcpy (&transport->tsi.gsi, gsi, 6);
	transport->dport = g_htons (dport);
	if (sport) {
		transport->tsi.sport = g_htons (sport);
	} else {
		do {
			transport->tsi.sport = g_htons (g_random_int_range (0, UINT16_MAX));
		} while (transport->tsi.sport == transport->dport);
	}

/* network data ports */
	transport->udp_encap_port = ((struct sockaddr_in*)&send_gsr->gsr_group)->sin_port;

/* copy network parameters */
	memcpy (&transport->send_gsr, send_gsr, sizeof(struct group_source_req));
	for (unsigned i = 0; i < recv_len; i++)
	{
		memcpy (&transport->recv_gsr[i], &recv_gsr[i], sizeof(struct group_source_req));
	}
	transport->recv_gsr_len = recv_len;

/* open sockets to implement PGM */
	int socket_type, protocol;
	if (transport->udp_encap_port) {
		g_trace ("INFO", "opening UDP encapsulated sockets.");
		socket_type = SOCK_DGRAM;
		protocol = IPPROTO_UDP;
	} else {
		g_trace ("INFO", "opening raw sockets.");
		socket_type = SOCK_RAW;
		protocol = ipproto_pgm;
	}

	if ((transport->recv_sock = socket(pgm_sockaddr_family(&recv_gsr[0].gsr_group),
						socket_type,
						protocol)) < 0)
	{
		retval = transport->recv_sock;
		if (retval == EPERM && 0 != getuid()) {
			g_critical ("PGM protocol requires this program to run as superuser.");
		}
		goto err_destroy;
	}

	if ((transport->send_sock = socket(pgm_sockaddr_family(&send_gsr->gsr_group),
						socket_type,
						protocol)) < 0)
	{
		retval = transport->send_sock;
		goto err_destroy;
	}

	if ((transport->send_with_router_alert_sock = socket(pgm_sockaddr_family(&send_gsr->gsr_group),
						socket_type,
						protocol)) < 0)
	{
		retval = transport->send_with_router_alert_sock;
		goto err_destroy;
	}

/* create timer thread */
#ifndef PGM_SINGLE_THREAD
	GError* err;
	GThread* thread;

/* set up condition for thread context & loop being ready */
	transport->thread_mutex = g_mutex_new ();
	transport->thread_cond = g_cond_new ();

	thread = g_thread_create_full (pgm_timer_thread,
					transport,
					0,
					TRUE,
					TRUE,
					G_THREAD_PRIORITY_HIGH,
					&err);
	if (thread) {
		transport->timer_thread = thread;
	} else {
		g_error ("thread failed: %i %s", err->code, err->message);
		goto err_destroy;
	}

	g_mutex_lock (transport->thread_mutex);
	while (!transport->timer_loop)
		g_cond_wait (transport->thread_cond, transport->thread_mutex);
	g_mutex_unlock (transport->thread_mutex);

	g_mutex_free (transport->thread_mutex);
	transport->thread_mutex = NULL;
	g_cond_free (transport->thread_cond);
	transport->thread_cond = NULL;

#endif /* !PGM_SINGLE_THREAD */

	*transport_ = transport;

	g_static_rw_lock_writer_lock (&pgm_transport_list_lock);
	pgm_transport_list = g_slist_append (pgm_transport_list, transport);
	g_static_rw_lock_writer_unlock (&pgm_transport_list_lock);

	return retval;

err_destroy:
	if (transport->thread_mutex) {
		g_mutex_free (transport->thread_mutex);
		transport->thread_mutex = NULL;
	}
	if (transport->thread_cond) {
		g_cond_free (transport->thread_cond);
		transport->thread_cond = NULL;
	}
	if (transport->timer_thread) {
	}
		
	if (transport->recv_sock) {
		close(transport->recv_sock);
		transport->recv_sock = 0;
	}
	if (transport->send_sock) {
		close(transport->send_sock);
		transport->send_sock = 0;
	}
	if (transport->send_with_router_alert_sock) {
		close(transport->send_with_router_alert_sock);
		transport->send_with_router_alert_sock = 0;
	}

	g_static_mutex_free (&transport->mutex);
	g_free (transport);
	transport = NULL;

	return retval;
}

/* helper to drop out of setuid 0 after creating PGM sockets
 */
void
pgm_drop_superuser (void)
{
	if (0 == getuid()) {
		setuid((gid_t)65534);
		setgid((uid_t)65534);
	}
}

/* 0 < tpdu < 65536 by data type (guint16)
 *
 * IPv4:   68 <= tpdu < 65536		(RFC 2765)
 * IPv6: 1280 <= tpdu < 65536		(RFC 2460)
 *
 * on success, returns 0.  on invalid setting, returns -EINVAL.
 */

int
pgm_transport_set_max_tpdu (
	pgm_transport_t*	transport,
	guint16			max_tpdu
	)
{
	g_return_val_if_fail (transport != NULL, -EINVAL);
	g_return_val_if_fail (!transport->is_bound, -EINVAL);
#ifdef __USE_BSD
	g_return_val_if_fail (max_tpdu >= (sizeof(struct ip) + sizeof(struct pgm_header)), -EINVAL);
#else
	g_return_val_if_fail (max_tpdu >= (sizeof(struct iphdr) + sizeof(struct pgm_header)), -EINVAL);
#endif

	g_static_mutex_lock (&transport->mutex);
	transport->max_tpdu = max_tpdu;
	g_static_mutex_unlock (&transport->mutex);

	return 0;
}

/* TRUE = enable multicast loopback,
 * FALSE = default, to disable.
 *
 * on success, returns 0.  on invalid setting, returns -EINVAL.
 */

int
pgm_transport_set_multicast_loop (
	pgm_transport_t*	transport,
	gboolean		use_multicast_loop
	)
{
	g_return_val_if_fail (transport != NULL, -EINVAL);
	g_return_val_if_fail (!transport->is_bound, -EINVAL);

	g_static_mutex_lock (&transport->mutex);
	transport->use_multicast_loop = use_multicast_loop;
	g_static_mutex_unlock (&transport->mutex);

	return 0;
}

/* 0 < hops < 256, hops == -1 use kernel default (ignored).
 *
 * on success, returns 0.  on invalid setting, returns -EINVAL.
 */

int
pgm_transport_set_hops (
	pgm_transport_t*	transport,
	gint			hops
	)
{
	g_return_val_if_fail (transport != NULL, -EINVAL);
	g_return_val_if_fail (!transport->is_bound, -EINVAL);
	g_return_val_if_fail (hops > 0, -EINVAL);
	g_return_val_if_fail (hops < 256, -EINVAL);

	g_static_mutex_lock (&transport->mutex);
	transport->hops = hops;
	g_static_mutex_unlock (&transport->mutex);

	return 0;
}

/* Linux 2.6 limited to millisecond resolution with conventional timers, however RDTSC
 * and future high-resolution timers allow nanosecond resolution.  Current ethernet technology
 * is limited to microseconds at best so we'll sit there for a bit.
 *
 * on success, returns 0.  on invalid setting, returns -EINVAL.
 */

int
pgm_transport_set_ambient_spm (
	pgm_transport_t*	transport,
	guint			spm_ambient_interval	/* in microseconds */
	)
{
	g_return_val_if_fail (transport != NULL, -EINVAL);
	g_return_val_if_fail (!transport->is_bound, -EINVAL);
	g_return_val_if_fail (spm_ambient_interval > 0, -EINVAL);

	g_static_mutex_lock (&transport->mutex);
	transport->spm_ambient_interval = spm_ambient_interval;
	g_static_mutex_unlock (&transport->mutex);

	return 0;
}

/* an array of intervals appropriately tuned till ambient period is reached.
 *
 * array is zero leaded for ambient state, and zero terminated for easy detection.
 *
 * on success, returns 0.  on invalid setting, returns -EINVAL.
 */

int
pgm_transport_set_heartbeat_spm (
	pgm_transport_t*	transport,
	const guint*		spm_heartbeat_interval,
	int			len
	)
{
	g_return_val_if_fail (transport != NULL, -EINVAL);
	g_return_val_if_fail (!transport->is_bound, -EINVAL);
	g_return_val_if_fail (len > 0, -EINVAL);
	for (int i = 0; i < len; i++) {
		g_return_val_if_fail (spm_heartbeat_interval[i] > 0, -EINVAL);
	}

	g_static_mutex_lock (&transport->mutex);
	if (transport->spm_heartbeat_interval)
		g_free (transport->spm_heartbeat_interval);
	transport->spm_heartbeat_interval = g_malloc (sizeof(guint) * (len+2));
	memcpy (&transport->spm_heartbeat_interval[1], spm_heartbeat_interval, sizeof(guint) * len);
	transport->spm_heartbeat_interval[0] = transport->spm_heartbeat_interval[len] = 0;
	g_static_mutex_unlock (&transport->mutex);

	return 0;
}

/* set interval timer & expiration timeout for peer expiration, very lax checking.
 *
 * 0 < 2 * spm_ambient_interval <= peer_expiry
 *
 * on success, returns 0.  on invalid setting, returns -EINVAL.
 */

int
pgm_transport_set_peer_expiry (
	pgm_transport_t*	transport,
	guint			peer_expiry	/* in microseconds */
	)
{
	g_return_val_if_fail (transport != NULL, -EINVAL);
	g_return_val_if_fail (!transport->is_bound, -EINVAL);
	g_return_val_if_fail (peer_expiry > 0, -EINVAL);
	g_return_val_if_fail (peer_expiry >= 2 * transport->spm_ambient_interval, -EINVAL);

	g_static_mutex_lock (&transport->mutex);
	transport->peer_expiry = peer_expiry;
	g_static_mutex_unlock (&transport->mutex);

	return 0;
}

/* set maximum back off range for listening for multicast SPMR
 *
 * 0 < spmr_expiry < spm_ambient_interval
 *
 * on success, returns 0.  on invalid setting, returns -EINVAL.
 */

int
pgm_transport_set_spmr_expiry (
	pgm_transport_t*	transport,
	guint			spmr_expiry	/* in microseconds */
	)
{
	g_return_val_if_fail (transport != NULL, -EINVAL);
	g_return_val_if_fail (!transport->is_bound, -EINVAL);
	g_return_val_if_fail (spmr_expiry > 0, -EINVAL);
	if (transport->can_send_data) {
		g_return_val_if_fail (transport->spm_ambient_interval > spmr_expiry, -EINVAL);
	}

	g_static_mutex_lock (&transport->mutex);
	transport->spmr_expiry = spmr_expiry;
	g_static_mutex_unlock (&transport->mutex);

	return 0;
}

/* 0 < txw_preallocate <= txw_sqns 
 *
 * can only be enforced at bind.
 *
 * on success, returns 0.  on invalid setting, returns -EINVAL.
 */

int
pgm_transport_set_txw_preallocate (
	pgm_transport_t*	transport,
	guint			sqns
	)
{
	g_return_val_if_fail (transport != NULL, -EINVAL);
	g_return_val_if_fail (!transport->is_bound, -EINVAL);
	g_return_val_if_fail (sqns > 0, -EINVAL);

	g_static_mutex_lock (&transport->mutex);
	transport->txw_preallocate = sqns;
	g_static_mutex_unlock (&transport->mutex);

	return 0;
}

/* 0 < txw_sqns < one less than half sequence space
 *
 * on success, returns 0.  on invalid setting, returns -EINVAL.
 */

int
pgm_transport_set_txw_sqns (
	pgm_transport_t*	transport,
	guint			sqns
	)
{
	g_return_val_if_fail (transport != NULL, -EINVAL);
	g_return_val_if_fail (!transport->is_bound, -EINVAL);
	g_return_val_if_fail (sqns < ((UINT32_MAX/2)-1), -EINVAL);
	g_return_val_if_fail (sqns > 0, -EINVAL);

	g_static_mutex_lock (&transport->mutex);
	transport->txw_sqns = sqns;
	g_static_mutex_unlock (&transport->mutex);

	return 0;
}

/* 0 < secs < ( txw_sqns / txw_max_rte )
 *
 * can only be enforced upon bind.
 *
 * on success, returns 0.  on invalid setting, returns -EINVAL.
 */

int
pgm_transport_set_txw_secs (
	pgm_transport_t*	transport,
	guint			secs
	)
{
	g_return_val_if_fail (transport != NULL, -EINVAL);
	g_return_val_if_fail (!transport->is_bound, -EINVAL);
	g_return_val_if_fail (secs > 0, -EINVAL);

	g_static_mutex_lock (&transport->mutex);
	transport->txw_secs = secs;
	g_static_mutex_unlock (&transport->mutex);

	return 0;
}

/* 0 < txw_max_rte < interface capacity
 *
 *  10mb :   1250000
 * 100mb :  12500000
 *   1gb : 125000000
 *
 * no practical way to determine upper limit and enforce.
 *
 * on success, returns 0.  on invalid setting, returns -EINVAL.
 */

int
pgm_transport_set_txw_max_rte (
	pgm_transport_t*	transport,
	guint			max_rte
	)
{
	g_return_val_if_fail (transport != NULL, -EINVAL);
	g_return_val_if_fail (!transport->is_bound, -EINVAL);
	g_return_val_if_fail (max_rte > 0, -EINVAL);

	g_static_mutex_lock (&transport->mutex);
	transport->txw_max_rte = max_rte;
	g_static_mutex_unlock (&transport->mutex);

	return 0;
}

/* 0 < rxw_preallocate <= rxw_sqns 
 *
 * can only be enforced at bind.
 *
 * on success, returns 0.  on invalid setting, returns -EINVAL.
 */

int
pgm_transport_set_rxw_preallocate (
	pgm_transport_t*	transport,
	guint			sqns
	)
{
	g_return_val_if_fail (transport != NULL, -EINVAL);
	g_return_val_if_fail (!transport->is_bound, -EINVAL);
	g_return_val_if_fail (sqns > 0, -EINVAL);

	g_static_mutex_lock (&transport->mutex);
	transport->rxw_preallocate = sqns;
	g_static_mutex_unlock (&transport->mutex);

	return 0;
}

/* 0 < rxw_sqns < one less than half sequence space
 *
 * on success, returns 0.  on invalid setting, returns -EINVAL.
 */

int
pgm_transport_set_rxw_sqns (
	pgm_transport_t*	transport,
	guint			sqns
	)
{
	g_return_val_if_fail (transport != NULL, -EINVAL);
	g_return_val_if_fail (!transport->is_bound, -EINVAL);
	g_return_val_if_fail (sqns < ((UINT32_MAX/2)-1), -EINVAL);
	g_return_val_if_fail (sqns > 0, -EINVAL);

	g_static_mutex_lock (&transport->mutex);
	transport->rxw_sqns = sqns;
	g_static_mutex_unlock (&transport->mutex);

	return 0;
}

/* 0 < secs < ( rxw_sqns / rxw_max_rte )
 *
 * can only be enforced upon bind.
 *
 * on success, returns 0.  on invalid setting, returns -EINVAL.
 */

int
pgm_transport_set_rxw_secs (
	pgm_transport_t*	transport,
	guint			secs
	)
{
	g_return_val_if_fail (transport != NULL, -EINVAL);
	g_return_val_if_fail (!transport->is_bound, -EINVAL);
	g_return_val_if_fail (secs > 0, -EINVAL);

	g_static_mutex_lock (&transport->mutex);
	transport->rxw_secs = secs;
	g_static_mutex_unlock (&transport->mutex);

	return 0;
}

/* 0 < rxw_max_rte < interface capacity
 *
 *  10mb :   1250000
 * 100mb :  12500000
 *   1gb : 125000000
 *
 * no practical way to determine upper limit and enforce.
 *
 * on success, returns 0.  on invalid setting, returns -EINVAL.
 */

int
pgm_transport_set_rxw_max_rte (
	pgm_transport_t*	transport,
	guint			max_rte
	)
{
	g_return_val_if_fail (transport != NULL, -EINVAL);
	g_return_val_if_fail (!transport->is_bound, -EINVAL);
	g_return_val_if_fail (max_rte > 0, -EINVAL);

	g_static_mutex_lock (&transport->mutex);
	transport->rxw_max_rte = max_rte;
	g_static_mutex_unlock (&transport->mutex);

	return 0;
}


/* 0 < wmem < wmem_max (user)
 *
 * operating system and sysctl dependent maximum, minimum on Linux 256 (doubled).
 *
 * on success, returns 0.  on invalid setting, returns -EINVAL.
 */

int
pgm_transport_set_sndbuf (
	pgm_transport_t*	transport,
	int			size		/* not gsize/gssize as we propogate to setsockopt() */
	)
{
	g_return_val_if_fail (transport != NULL, -EINVAL);
	g_return_val_if_fail (!transport->is_bound, -EINVAL);
	g_return_val_if_fail (size > 0, -EINVAL);

	int wmem_max;
	FILE *fp = fopen ("/proc/sys/net/core/wmem_max", "r");
	if (fp) {
		fscanf (fp, "%d", &wmem_max);
		fclose (fp);
		g_return_val_if_fail (size <= wmem_max, -EINVAL);
	} else {
		g_warning ("cannot open /proc/sys/net/core/wmem_max");
	}

	g_static_mutex_lock (&transport->mutex);
	transport->sndbuf = size;
	g_static_mutex_unlock (&transport->mutex);

	return 0;
}

/* 0 < rmem < rmem_max (user)
 *
 * minimum on Linux is 2048 (doubled).
 *
 * on success, returns 0.  on invalid setting, returns -EINVAL.
 */

int
pgm_transport_set_rcvbuf (
	pgm_transport_t*	transport,
	int			size		/* not gsize/gssize */
	)
{
	g_return_val_if_fail (transport != NULL, -EINVAL);
	g_return_val_if_fail (!transport->is_bound, -EINVAL);
	g_return_val_if_fail (size > 0, -EINVAL);

	int rmem_max;
	FILE *fp = fopen ("/proc/sys/net/core/rmem_max", "r");
	if (fp) {
		fscanf (fp, "%d", &rmem_max);
		fclose (fp);
		g_return_val_if_fail (size <= rmem_max, -EINVAL);
	} else {
		g_warning ("cannot open /proc/sys/net/core/rmem_max");
	}

	g_static_mutex_lock (&transport->mutex);
	transport->rcvbuf = size;
	g_static_mutex_unlock (&transport->mutex);

	return 0;
}

/* Actual NAK back-off, NAK_RB_IVL, is random time interval 1 < NAK_BO_IVL,
 * randomized to reduce storms.
 *
 * on success, returns 0.  on invalid setting, returns -EINVAL.
 */

int
pgm_transport_set_nak_bo_ivl (
	pgm_transport_t*	transport,
	guint			usec		/* microseconds */
	)
{
	g_return_val_if_fail (transport != NULL, -EINVAL);
	g_return_val_if_fail (!transport->is_bound, -EINVAL);

	g_static_mutex_lock (&transport->mutex);
	transport->nak_bo_ivl = usec;
	g_static_mutex_unlock (&transport->mutex);

	return 0;
}

/* Set NAK_RPT_IVL, the repeat interval before re-sending a NAK.
 *
 * on success, returns 0.  on invalid setting, returns -EINVAL.
 */
int
pgm_transport_set_nak_rpt_ivl (
	pgm_transport_t*	transport,
	guint			usec		/* microseconds */
	)
{
	g_return_val_if_fail (transport != NULL, -EINVAL);
	g_return_val_if_fail (!transport->is_bound, -EINVAL);

	g_static_mutex_lock (&transport->mutex);
	transport->nak_rpt_ivl = usec;
	g_static_mutex_unlock (&transport->mutex);

	return 0;
}

/* Set NAK_RDATA_IVL, the interval waiting for data.
 *
 * on success, returns 0.  on invalid setting, returns -EINVAL.
 */
int
pgm_transport_set_nak_rdata_ivl (
	pgm_transport_t*	transport,
	guint			usec		/* microseconds */
	)
{
	g_return_val_if_fail (transport != NULL, -EINVAL);
	g_return_val_if_fail (!transport->is_bound, -EINVAL);

	g_static_mutex_lock (&transport->mutex);
	transport->nak_rdata_ivl = usec;
	g_static_mutex_unlock (&transport->mutex);

	return 0;
}

/* statistics are limited to guint8, i.e. 255 retries
 *
 * on success, returns 0.  on invalid setting, returns -EINVAL.
 */
int
pgm_transport_set_nak_data_retries (
	pgm_transport_t*	transport,
	guint			cnt
	)
{
	g_return_val_if_fail (transport != NULL, -EINVAL);
	g_return_val_if_fail (!transport->is_bound, -EINVAL);

	g_static_mutex_lock (&transport->mutex);
	transport->nak_data_retries = cnt;
	g_static_mutex_unlock (&transport->mutex);

	return 0;
}

/* statistics are limited to guint8, i.e. 255 retries
 *
 * on success, returns 0.  on invalid setting, returns -EINVAL.
 */
int
pgm_transport_set_nak_ncf_retries (
	pgm_transport_t*	transport,
	guint			cnt
	)
{
	g_return_val_if_fail (transport != NULL, -EINVAL);
	g_return_val_if_fail (!transport->is_bound, -EINVAL);

	g_static_mutex_lock (&transport->mutex);
	transport->nak_ncf_retries = cnt;
	g_static_mutex_unlock (&transport->mutex);

	return 0;
}

/* context aware g_io helpers
 *
 * on success, returns id of GSource.
 */
static guint
g_io_add_watch_context_full (
	GIOChannel*		channel,
	GMainContext*		context,
	gint			priority,
	GIOCondition		condition,
	GIOFunc			function,
	gpointer		user_data,
	GDestroyNotify		notify
	)
{
	GSource *source;
	guint id;
  
	g_return_val_if_fail (channel != NULL, 0);

	source = g_io_create_watch (channel, condition);

	if (priority != G_PRIORITY_DEFAULT)
		g_source_set_priority (source, priority);
	g_source_set_callback (source, (GSourceFunc)function, user_data, notify);

	id = g_source_attach (source, context);
	g_source_unref (source);

	return id;
}

static gboolean
g_source_remove_context (
	GMainContext*		context,
	guint			tag
	)
{
	GSource* source;

	g_return_val_if_fail (tag > 0, FALSE);

	source = g_main_context_find_source_by_id (context, tag);
	if (source)
		g_source_destroy (source);

	return source != NULL;
}

/* bind the sockets to the link layer to start receiving data.
 *
 * returns 0 on success, or -1 on error and sets errno appropriately,
 *			 or -2 on NS lookup error and sets h_errno appropriately.
 */

int
pgm_transport_bind (
	pgm_transport_t*	transport
	)
{
	g_return_val_if_fail (transport != NULL, -EINVAL);
	g_return_val_if_fail (!transport->is_bound, -EINVAL);

	int retval = 0;

	g_static_mutex_lock (&transport->mutex);

	g_trace ("INFO","creating new random number generator.");
	transport->rand_ = g_rand_new();

	if (transport->can_send_data)
	{
		g_trace ("INFO","create rx to nak processor notify channel.");
		retval = pgm_notify_init (&transport->rdata_notify);
		if (retval < 0) {
			g_static_mutex_unlock (&transport->mutex);
			goto out;
		}
	}

	g_trace ("INFO","create any to timer notify channel.");
	retval = pgm_notify_init (&transport->timer_notify);
	if (retval < 0) {
		g_static_mutex_unlock (&transport->mutex);
		goto out;
	}

	g_trace ("INFO","create timer shutdown channel.");
	retval = pgm_notify_init (&transport->timer_shutdown);
	if (retval < 0) {
		g_static_mutex_unlock (&transport->mutex);
		goto out;
	}

	if (transport->can_recv)
	{
		g_trace ("INFO","create waiting notify channel.");
		retval = pgm_notify_init (&transport->waiting_notify);
		if (retval < 0) {
			g_static_mutex_unlock (&transport->mutex);
			goto out;
		}
	}

/* determine IP header size for rate regulation engine & stats */
	switch (pgm_sockaddr_family(&transport->send_gsr.gsr_group)) {
	case AF_INET:
#ifdef __USE_BSD
		transport->iphdr_len = sizeof(struct ip);
#else
		transport->iphdr_len = sizeof(struct iphdr);
#endif
		break;

	case AF_INET6:
		transport->iphdr_len = 40;	/* sizeof(struct ipv6hdr) */
		break;
	}
	g_trace ("INFO","assuming IP header size of %" G_GSIZE_FORMAT " bytes", transport->iphdr_len);

	if (transport->udp_encap_port)
	{
		guint udphdr_len = sizeof( struct udphdr );
		g_trace ("INFO","assuming UDP header size of %i bytes", udphdr_len);
		transport->iphdr_len += udphdr_len;
	}

	transport->max_tsdu = transport->max_tpdu - transport->iphdr_len - pgm_transport_pkt_offset (FALSE);
	transport->max_tsdu_fragment = transport->max_tpdu - transport->iphdr_len - pgm_transport_pkt_offset (TRUE);

	if (transport->can_send_data)
	{
		g_trace ("INFO","construct transmit window.");
		transport->txw = pgm_txw_init (transport->max_tpdu - transport->iphdr_len,
						transport->txw_preallocate,
						transport->txw_sqns,
						transport->txw_secs,
						transport->txw_max_rte);
	}

/* create peer list */
	if (transport->can_recv)
	{
		transport->peers_hashtable = g_hash_table_new (pgm_tsi_hash, pgm_tsi_equal);
	}

	if (transport->udp_encap_port)
	{
/* set socket sharing if loopback enabled, needs to be performed pre-bind */
		g_trace ("INFO","set socket sharing.");
		gboolean v = TRUE;
		if (0 != setsockopt (transport->recv_sock, SOL_SOCKET, SO_REUSEADDR, (const char*)&v, sizeof(v)) ||
		    0 != setsockopt (transport->send_sock, SOL_SOCKET, SO_REUSEADDR, (const char*)&v, sizeof(v)) ||
		    0 != setsockopt (transport->send_with_router_alert_sock, SOL_SOCKET, SO_REUSEADDR, (const char*)&v, sizeof(v)))
		{
			g_static_mutex_unlock (&transport->mutex);
			goto out;
		}

/* request extra packet information to determine destination address on each packet */
		g_trace ("INFO","request socket packet-info.");
		int recv_family = pgm_sockaddr_family(&transport->recv_gsr[0].gsr_group);
		retval = pgm_sockaddr_pktinfo (transport->recv_sock, recv_family, TRUE);
		if (retval < 0) {
			g_static_mutex_unlock (&transport->mutex);
			goto out;
		}
	}
	else
	{
		int recv_family = pgm_sockaddr_family(&transport->recv_gsr[0].gsr_group);
		if (AF_INET == recv_family)
		{
/* include IP header only for incoming data, only works for IPv4 */
			g_trace ("INFO","request IP headers.");
			retval = pgm_sockaddr_hdrincl (transport->recv_sock, recv_family, TRUE);
			if (retval < 0) {
				g_static_mutex_unlock (&transport->mutex);
				goto out;
			}
		}
		else
		{
			g_assert (AF_INET6 == recv_family);
			g_trace ("INFO","request socket packet-info.");
			retval = pgm_sockaddr_pktinfo (transport->recv_sock, recv_family, TRUE);
			if (retval < 0) {
				g_static_mutex_unlock (&transport->mutex);
				goto out;
			}
		}
	}

/* buffers, set size first then re-read to confirm actual value */
	if (transport->rcvbuf)
	{
		g_trace ("INFO","set receive socket buffer size.");
		retval = setsockopt(transport->recv_sock, SOL_SOCKET, SO_RCVBUF, (char*)&transport->rcvbuf, sizeof(transport->rcvbuf));
		if (retval < 0) {
			g_static_mutex_unlock (&transport->mutex);
			goto out;
		}
	}
	if (transport->sndbuf)
	{
		g_trace ("INFO","set send socket buffer size.");
		retval = setsockopt(transport->send_sock, SOL_SOCKET, SO_SNDBUF, (char*)&transport->sndbuf, sizeof(transport->sndbuf));
		if (retval < 0) {
			g_static_mutex_unlock (&transport->mutex);
			goto out;
		}
		retval = setsockopt(transport->send_with_router_alert_sock, SOL_SOCKET, SO_SNDBUF, (char*)&transport->sndbuf, sizeof(transport->sndbuf));
		if (retval < 0) {
			g_static_mutex_unlock (&transport->mutex);
			goto out;
		}
	}

/* Most socket-level options utilize an int parameter for optval. */
	int buffer_size;
	socklen_t len = sizeof(buffer_size);
	retval = getsockopt(transport->recv_sock, SOL_SOCKET, SO_RCVBUF, &buffer_size, &len);
	if (retval < 0) {
		g_static_mutex_unlock (&transport->mutex);
		goto out;
	}
	g_trace ("INFO","receive buffer set at %i bytes.", buffer_size);

	retval = getsockopt(transport->send_sock, SOL_SOCKET, SO_SNDBUF, &buffer_size, &len);
	if (retval < 0) {
		g_static_mutex_unlock (&transport->mutex);
		goto out;
	}
	retval = getsockopt(transport->send_with_router_alert_sock, SOL_SOCKET, SO_SNDBUF, &buffer_size, &len);
	if (retval < 0) {
		g_static_mutex_unlock (&transport->mutex);
		goto out;
	}
	g_trace ("INFO","send buffer set at %i bytes.", buffer_size);

/* bind udp unicast sockets to interfaces, note multicast on a bound interface is
 * fruity on some platforms so callee should specify any interface.
 *
 * after binding default interfaces (0.0.0.0) are resolved
 */
/* TODO: different ports requires a new bound socket */

	struct sockaddr_storage recv_addr;
	memset (&recv_addr, 0, sizeof(recv_addr));

#ifdef CONFIG_BIND_INADDR_ANY

/* force default interface for bind-only, source address is still valid for multicast membership */
	((struct sockaddr*)&recv_addr)->sa_family = pgm_sockaddr_family(&transport->recv_gsr[0].gsr_group);
	switch (pgm_sockaddr_family(&recv_addr)) {
	case AF_INET:
		((struct sockaddr_in*)&recv_addr)->sin_addr.s_addr = INADDR_ANY;
		break;

	case AF_INET6:
		((struct sockaddr_in6*)&recv_addr)->sin6_addr = in6addr_any;
		break;
	}
#else
	retval = pgm_if_indextosockaddr (transport->recv_gsr[0].gsr_interface, pgm_sockaddr_family(&transport->recv_gsr[0].gsr_group), &recv_addr);
	if (retval < 0) {
#ifdef TRANSPORT_DEBUG
		int errno_ = errno;
		g_trace ("INFO","pgm_if_indextosockaddr failed on recv_gsr[0] interface %i, family %s, %s",
				transport->recv_gsr[0].gsr_interface,
				AF_INET == pgm_sockaddr_family(&transport->send_gsr.gsr_group) ? "AF_INET" :
					( AF_INET6 == pgm_sockaddr_family(&transport->send_gsr.gsr_group) ? "AF_INET6" :
					  "AF_UNKNOWN" ),
				strerror(errno_));
		errno = errno_;
#endif
		g_static_mutex_unlock (&transport->mutex);
		goto out;
	}
#ifdef TRANSPORT_DEBUG
	else
	{
		g_trace ("INFO","binding receive socket to interface index %i", transport->recv_gsr[0].gsr_interface);
	}
#endif

#endif /* CONFIG_BIND_INADDR_ANY */

	((struct sockaddr_in*)&recv_addr)->sin_port = ((struct sockaddr_in*)&transport->recv_gsr[0].gsr_group)->sin_port;

	retval = bind (transport->recv_sock,
			(struct sockaddr*)&recv_addr,
			pgm_sockaddr_len(&recv_addr));
	if (retval < 0) {
#ifdef TRANSPORT_DEBUG
		int errno_ = errno;
		char s[INET6_ADDRSTRLEN];
		pgm_sockaddr_ntop (&recv_addr, s, sizeof(s));
		g_trace ("INFO","bind failed on recv_gsr[0] interface \"%s\" %s", s, strerror(errno_));
		errno = errno_;
#endif
		g_static_mutex_unlock (&transport->mutex);
		goto out;
	}

#ifdef TRANSPORT_DEBUG
	{
		char s[INET6_ADDRSTRLEN];
		pgm_sockaddr_ntop (&recv_addr, s, sizeof(s));
		g_trace ("INFO","bind succeeded on recv_gsr[0] interface %s", s);
	}
#endif

/* keep a copy of the original address source to re-use for router alert bind */
	struct sockaddr_storage send_addr, send_with_router_alert_addr;
	memset (&send_addr, 0, sizeof(send_addr));

	retval = pgm_if_indextosockaddr (transport->send_gsr.gsr_interface, pgm_sockaddr_family(&transport->send_gsr.gsr_group), (struct sockaddr*)&send_addr);
	if (retval < 0) {
#ifdef TRANSPORT_DEBUG
		int errno_ = errno;
		g_trace ("INFO","pgm_if_indextosockaddr failed on send_gsr interface %i, family %s, %s",
				transport->send_gsr.gsr_interface,
				AF_INET == pgm_sockaddr_family(&transport->send_gsr.gsr_group) ? "AF_INET" :
					( AF_INET6 == pgm_sockaddr_family(&transport->send_gsr.gsr_group) ? "AF_INET6" :
					  "AF_UNKNOWN" ),
				strerror(errno_));
		errno = errno_;
#endif
		g_static_mutex_unlock (&transport->mutex);
		goto out;
	}
#ifdef TRANSPORT_DEBUG
	else
	{
		g_trace ("INFO","binding send socket to interface index %i", transport->send_gsr.gsr_interface);
	}
#endif

	memcpy (&send_with_router_alert_addr, &send_addr, pgm_sockaddr_len(&send_addr));

	retval = bind (transport->send_sock,
			(struct sockaddr*)&send_addr,
			pgm_sockaddr_len(&send_addr));
	if (retval < 0) {
#ifdef TRANSPORT_DEBUG
		int errno_ = errno;
		char s[INET6_ADDRSTRLEN];
		pgm_sockaddr_ntop (&send_addr, s, sizeof(s));
		g_trace ("INFO","bind failed on send_gsr interface %s: %s", s, strerror(errno_));
		errno = errno_;
#endif
		g_static_mutex_unlock (&transport->mutex);
		goto out;
	}

/* resolve bound address if 0.0.0.0 */
	switch (pgm_sockaddr_family(&send_addr)) {
	case AF_INET:
		if (((struct sockaddr_in*)&send_addr)->sin_addr.s_addr == INADDR_ANY)
		{
			retval = pgm_if_getnodeaddr(AF_INET, (struct sockaddr*)&send_addr, sizeof(send_addr));
			if (retval < 0) {
				g_static_mutex_unlock (&transport->mutex);
				goto out;
			}
		}
		break;

	case AF_INET6:
		if (memcmp (&in6addr_any, &((struct sockaddr_in6*)&send_addr)->sin6_addr, sizeof(in6addr_any)) == 0)
		{
			retval = pgm_if_getnodeaddr(AF_INET6, (struct sockaddr*)&send_addr, sizeof(send_addr));
			if (retval < 0) {
				g_static_mutex_unlock (&transport->mutex);
				goto out;
			}
		}
		break;
	}

#ifdef TRANSPORT_DEBUG
	{
		char s[INET6_ADDRSTRLEN];
		pgm_sockaddr_ntop (&send_addr, s, sizeof(s));
		g_trace ("INFO","bind succeeded on send_gsr interface %s", s);
	}
#endif

	retval = bind (transport->send_with_router_alert_sock,
			(struct sockaddr*)&send_with_router_alert_addr,
			pgm_sockaddr_len(&send_with_router_alert_addr));
	if (retval < 0) {
#ifdef TRANSPORT_DEBUG
		int errno_ = errno;
		char s[INET6_ADDRSTRLEN];
		pgm_sockaddr_ntop (&send_with_router_alert_addr, s, sizeof(s));
		g_trace ("INFO","bind (router alert) failed on send_gsr interface %s: %s", s, strerror(errno_));
		errno = errno_;
#endif
		g_static_mutex_unlock (&transport->mutex);
		goto out;
	}

#ifdef TRANSPORT_DEBUG
	{
		char s[INET6_ADDRSTRLEN];
		pgm_sockaddr_ntop (&send_with_router_alert_addr, s, sizeof(s));
		g_trace ("INFO","bind (router alert) succeeded on send_gsr interface %s", s);
	}
#endif

/* save send side address for broadcasting as source nla */
	memcpy (&transport->send_addr, &send_addr, pgm_sockaddr_len(&send_addr));
	

/* receiving groups (multiple) */
	for (unsigned i = 0; i < transport->recv_gsr_len; i++)
	{
		struct group_source_req* p = &transport->recv_gsr[i];
		int recv_level = ( (AF_INET == pgm_sockaddr_family((&p->gsr_group))) ? SOL_IP : SOL_IPV6 );
		int optname = (pgm_sockaddr_cmp ((struct sockaddr*)&p->gsr_group, (struct sockaddr*)&p->gsr_source) == 0)
				? MCAST_JOIN_GROUP : MCAST_JOIN_SOURCE_GROUP;
		socklen_t plen = MCAST_JOIN_GROUP == optname ? sizeof(struct group_req) : sizeof(struct group_source_req);
		retval = setsockopt(transport->recv_sock, recv_level, optname, p, plen);
		if (retval < 0) {
#ifdef TRANSPORT_DEBUG
			int errno_ = errno;
			char s1[INET6_ADDRSTRLEN], s2[INET6_ADDRSTRLEN];
			pgm_sockaddr_ntop (&p->gsr_group, s1, sizeof(s1));
			pgm_sockaddr_ntop (&p->gsr_source, s2, sizeof(s2));
			if (optname == MCAST_JOIN_GROUP)
				g_trace ("INFO","MCAST_JOIN_GROUP failed on recv_gsr[%i] interface %i group %s: %s",
					i, p->gsr_interface, s1, strerror(errno_));
			else
				g_trace ("INFO","MCAST_JOIN_SOURCE_GROUP failed on recv_gsr[%i] interface %i group %s source %s: %s",
					i, p->gsr_interface, s1, s2, strerror(errno_));
			errno = errno_;
#endif
			g_static_mutex_unlock (&transport->mutex);
			goto out;
		}
#ifdef TRANSPORT_DEBUG
		else
		{
			char s1[INET6_ADDRSTRLEN], s2[INET6_ADDRSTRLEN];
			pgm_sockaddr_ntop (&p->gsr_group, s1, sizeof(s1));
			pgm_sockaddr_ntop (&p->gsr_source, s2, sizeof(s2));
			if (optname == MCAST_JOIN_GROUP)
				g_trace ("INFO","MCAST_JOIN_GROUP succeeded on recv_gsr[%i] interface %i group %s",
					i, p->gsr_interface, s1);
			else
				g_trace ("INFO","MCAST_JOIN_SOURCE_GROUP succeeded on recv_gsr[%i] interface %i group %s source %s",
					i, p->gsr_interface, s1, s2);
		}
#endif
	}

/* send group (singular) */
	retval = pgm_sockaddr_multicast_if (transport->send_sock, (struct sockaddr*)&transport->send_addr, transport->send_gsr.gsr_interface);
	if (retval < 0) {
#ifdef TRANSPORT_DEBUG
		int errno_ = errno;
		char s[INET6_ADDRSTRLEN];
		pgm_sockaddr_ntop (&transport->send_addr, s, sizeof(s));
		g_trace ("INFO","pgm_sockaddr_multicast_if failed on send_gsr address %s interface %i: %s",
					s, transport->send_gsr.gsr_interface, strerror(errno_));
		errno = errno_;
#endif
		g_static_mutex_unlock (&transport->mutex);
		goto out;
	}
#ifdef TRANSPORT_DEBUG
	else
	{
		char s[INET6_ADDRSTRLEN];
		pgm_sockaddr_ntop (&transport->send_addr, s, sizeof(s));
		g_trace ("INFO","pgm_sockaddr_multicast_if succeeded on send_gsr address %s interface %i",
					s, transport->send_gsr.gsr_interface);
	}
#endif
	retval = pgm_sockaddr_multicast_if (transport->send_with_router_alert_sock, (struct sockaddr*)&transport->send_addr, transport->send_gsr.gsr_interface);
	if (retval < 0) {
#ifdef TRANSPORT_DEBUG
		int errno_ = errno;
		char s[INET6_ADDRSTRLEN];
		pgm_sockaddr_ntop (&transport->send_addr, s, sizeof(s));
		g_trace ("INFO","pgm_sockaddr_multicast_if (router alert) failed on send_gsr address %s interface %i: %s",
					s, transport->send_gsr.gsr_interface, strerror(errno_));
		errno = errno_;
#endif
		g_static_mutex_unlock (&transport->mutex);
		goto out;
	}
#ifdef TRANSPORT_DEBUG
	else
	{
		char s[INET6_ADDRSTRLEN];
		pgm_sockaddr_ntop (&transport->send_addr, s, sizeof(s));
		g_trace ("INFO","pgm_sockaddr_multicast_if (router alert) succeeded on send_gsr address %s interface %i",
					s, transport->send_gsr.gsr_interface);
	}
#endif

/* multicast loopback */
	if (!transport->use_multicast_loop)
	{
		g_trace ("INFO","set multicast loop.");
		retval = pgm_sockaddr_multicast_loop (transport->recv_sock, pgm_sockaddr_family(&transport->recv_gsr[0].gsr_group), transport->use_multicast_loop);
		if (retval < 0) {
			g_static_mutex_unlock (&transport->mutex);
			goto out;
		}
		retval = pgm_sockaddr_multicast_loop (transport->send_sock, pgm_sockaddr_family(&transport->send_gsr.gsr_group), transport->use_multicast_loop);
		if (retval < 0) {
			g_static_mutex_unlock (&transport->mutex);
			goto out;
		}
		retval = pgm_sockaddr_multicast_loop (transport->send_with_router_alert_sock, pgm_sockaddr_family(&transport->send_gsr.gsr_group), transport->use_multicast_loop);
		if (retval < 0) {
			g_static_mutex_unlock (&transport->mutex);
			goto out;
		}
	}

/* multicast ttl: many crappy network devices go CPU ape with TTL=1, 16 is a popular alternative */
	g_trace ("INFO","set multicast hop limit.");
	retval = pgm_sockaddr_multicast_hops (transport->recv_sock, pgm_sockaddr_family(&transport->recv_gsr[0].gsr_group), transport->hops);
	if (retval < 0) {
		g_static_mutex_unlock (&transport->mutex);
		goto out;
	}
	retval = pgm_sockaddr_multicast_hops (transport->send_sock, pgm_sockaddr_family(&transport->send_gsr.gsr_group), transport->hops);
	if (retval < 0) {
		g_static_mutex_unlock (&transport->mutex);
		goto out;
	}
	retval = pgm_sockaddr_multicast_hops (transport->send_with_router_alert_sock, pgm_sockaddr_family(&transport->send_gsr.gsr_group), transport->hops);
	if (retval < 0) {
		g_static_mutex_unlock (&transport->mutex);
		goto out;
	}

/* set Expedited Forwarding PHB for network elements, no ECN.
 * 
 * codepoint 101110 (RFC 3246)
 */
	g_trace ("INFO","set packet differentiated services field to expedited forwarding.");
	int dscp = 0x2e << 2;
	retval = pgm_sockaddr_tos (transport->send_sock, pgm_sockaddr_family(&transport->send_gsr.gsr_group), dscp);
	if (retval < 0) {
		g_trace ("INFO","DSCP setting requires CAP_NET_ADMIN or ADMIN capability.");
		retval = 0;
		goto no_cap_net_admin;
	}
	retval = pgm_sockaddr_tos (transport->send_with_router_alert_sock, pgm_sockaddr_family(&transport->send_gsr.gsr_group), dscp);
	if (retval < 0) {
		g_static_mutex_unlock (&transport->mutex);
		goto out;
	}

no_cap_net_admin:

/* any to timer notify channel */
	transport->notify_channel = g_io_channel_unix_new (pgm_notify_get_fd (&transport->timer_notify));
	transport->notify_id = g_io_add_watch_context_full (transport->notify_channel, transport->timer_context, G_PRIORITY_HIGH, G_IO_IN, on_timer_notify, transport, NULL);

/* timer shutdown channel */
	transport->shutdown_channel = g_io_channel_unix_new (pgm_notify_get_fd (&transport->timer_shutdown));
	transport->shutdown_id = g_io_add_watch_context_full (transport->shutdown_channel, transport->timer_context, G_PRIORITY_HIGH, G_IO_IN, on_timer_shutdown, transport, NULL);

/* rx to nak processor notify channel */
	if (transport->can_send_data)
	{
		transport->rdata_channel = g_io_channel_unix_new (pgm_notify_get_fd (&transport->rdata_notify));
		transport->rdata_id = g_io_add_watch_context_full (transport->rdata_channel, transport->timer_context, G_PRIORITY_HIGH, G_IO_IN, on_nak_notify, transport, NULL);

/* create recyclable SPM packet */
		switch (pgm_sockaddr_family(&transport->send_gsr.gsr_group)) {
		case AF_INET:
			transport->spm_len = sizeof(struct pgm_header) + sizeof(struct pgm_spm);
			break;

		case AF_INET6:
			transport->spm_len = sizeof(struct pgm_header) + sizeof(struct pgm_spm6);
			break;
		}

		if (transport->use_proactive_parity || transport->use_ondemand_parity)
		{
			transport->spm_len += sizeof(struct pgm_opt_length) +
					      sizeof(struct pgm_opt_header) +
					      sizeof(struct pgm_opt_parity_prm);
		}

		transport->spm_packet = g_slice_alloc0 (transport->spm_len);

		struct pgm_header* header = (struct pgm_header*)transport->spm_packet;
		struct pgm_spm* spm = (struct pgm_spm*)( header + 1 );
		memcpy (header->pgm_gsi, &transport->tsi.gsi, sizeof(pgm_gsi_t));
		header->pgm_sport	= transport->tsi.sport;
		header->pgm_dport	= transport->dport;
		header->pgm_type	= PGM_SPM;

		pgm_sockaddr_to_nla ((struct sockaddr*)&transport->send_addr, (char*)&spm->spm_nla_afi);

/* OPT_PARITY_PRM */
		if (transport->use_proactive_parity || transport->use_ondemand_parity)
		{
			header->pgm_options     = PGM_OPT_PRESENT | PGM_OPT_NETWORK;

			struct pgm_opt_length* opt_len = (struct pgm_opt_length*)(spm + 1);
			opt_len->opt_type	= PGM_OPT_LENGTH;
			opt_len->opt_length	= sizeof(struct pgm_opt_length);
			opt_len->opt_total_length = g_htons (	sizeof(struct pgm_opt_length) +
								sizeof(struct pgm_opt_header) +
								sizeof(struct pgm_opt_parity_prm) );
			struct pgm_opt_header* opt_header = (struct pgm_opt_header*)(opt_len + 1);
			opt_header->opt_type	= PGM_OPT_PARITY_PRM | PGM_OPT_END;
			opt_header->opt_length	= sizeof(struct pgm_opt_header) + sizeof(struct pgm_opt_parity_prm);
			struct pgm_opt_parity_prm* opt_parity_prm = (struct pgm_opt_parity_prm*)(opt_header + 1);
			opt_parity_prm->opt_reserved = (transport->use_proactive_parity ? PGM_PARITY_PRM_PRO : 0) |
						       (transport->use_ondemand_parity ? PGM_PARITY_PRM_OND : 0);
			opt_parity_prm->parity_prm_tgs = g_htonl (transport->rs_k);
		}

/* setup rate control */
		if (transport->txw_max_rte)
		{
			g_trace ("INFO","Setting rate regulation to %i bytes per second.",
					transport->txw_max_rte);
	
			retval = pgm_rate_create (&transport->rate_control, transport->txw_max_rte, transport->iphdr_len);
			if (retval < 0) {
				g_static_mutex_unlock (&transport->mutex);
				goto out;
			}
		}

/* announce new transport by sending out SPMs */
		send_spm_unlocked (transport);
		send_spm_unlocked (transport);
		send_spm_unlocked (transport);

/* parity buffer for odata/rdata transmission */
		if (transport->use_proactive_parity || transport->use_ondemand_parity)
		{
			g_trace ("INFO","Enabling Reed-Solomon forward error correction, RS(%i,%i).",
					transport->rs_n, transport->rs_k);
			transport->parity_buffer = g_malloc ( transport->max_tpdu );
			pgm_rs_create (&transport->rs, transport->rs_n, transport->rs_k);
		}

		transport->next_poll = transport->next_ambient_spm = pgm_time_update_now() + transport->spm_ambient_interval;
	}
	else
	{
		g_assert (transport->can_recv);
		transport->next_poll = pgm_time_update_now() + pgm_secs( 30 );
	}

	g_trace ("INFO","adding dynamic timer");
	transport->timer_id = pgm_add_timer (transport);

/* allocate first incoming packet buffer */
	transport->rx_buffer = g_malloc ( transport->max_tpdu );

/* scatter/gather vector for contiguous reading from the window */
	transport->piov_len = IOV_MAX;
	transport->piov = g_malloc ( transport->piov_len * sizeof( struct iovec ) );

/* receiver trash */
	g_static_mutex_init (&transport->rx_mutex);

/* cleanup */
	transport->is_bound = TRUE;
	transport->is_open = TRUE;
	g_static_mutex_unlock (&transport->send_mutex);
	g_static_mutex_unlock (&transport->mutex);

	g_trace ("INFO","transport successfully created.");
out:
	return retval;
}

/* a peer in the context of the transport is another party on the network sending PGM
 * packets.  for each peer we need a receive window and network layer address (nla) to
 * which nak requests can be forwarded to.
 *
 * on success, returns new peer object.
 */

static pgm_peer_t*
new_peer (
	pgm_transport_t*	transport,
	pgm_tsi_t*		tsi,
	struct sockaddr*	src_addr,
	gsize			src_addr_len,
	struct sockaddr*	dst_addr,
	gsize			dst_addr_len
	)
{
	pgm_peer_t* peer;

#ifdef TRANSPORT_DEBUG
	char localnla[INET6_ADDRSTRLEN];
	char groupnla[INET6_ADDRSTRLEN];
	inet_ntop (	pgm_sockaddr_family( src_addr ),
			pgm_sockaddr_addr( src_addr ),
			localnla,
			sizeof(localnla) );
	inet_ntop (	pgm_sockaddr_family( dst_addr ),
			pgm_sockaddr_addr( dst_addr ),
			groupnla,
			sizeof(groupnla) );
	g_trace ("INFO","new peer, tsi %s, group nla %s, local nla %s", pgm_print_tsi (tsi), groupnla, localnla);
#endif

	peer = g_malloc0 (sizeof(pgm_peer_t));
	peer->last_packet = pgm_time_update_now();
	peer->expiry = peer->last_packet + transport->peer_expiry;
	g_static_mutex_init (&peer->mutex);
	peer->transport = transport;
	memcpy (&peer->tsi, tsi, sizeof(pgm_tsi_t));
	((struct sockaddr_in*)&peer->nla)->sin_port = transport->udp_encap_port;
	((struct sockaddr_in*)&peer->nla)->sin_addr.s_addr = INADDR_ANY;
	memcpy (&peer->group_nla, dst_addr, dst_addr_len);
	memcpy (&peer->local_nla, src_addr, src_addr_len);

/* lock on rx window */
	peer->rxw = pgm_rxw_init (
				&peer->tsi,
				transport->max_tpdu - transport->iphdr_len,
				transport->rxw_preallocate,
				transport->rxw_sqns,
				transport->rxw_secs,
				transport->rxw_max_rte,
				&transport->rx_data,
				&transport->rx_packet,
				&transport->rx_mutex);

	memcpy (&((pgm_rxw_t*)peer->rxw)->pgm_sock_err.tsi, &peer->tsi, sizeof(pgm_tsi_t));
	peer->spmr_expiry = peer->last_packet + transport->spmr_expiry;

/* add peer to hash table and linked list */
	g_static_rw_lock_writer_lock (&transport->peers_lock);
	gpointer entry = pgm_peer_ref(peer);
	g_hash_table_insert (transport->peers_hashtable, &peer->tsi, entry);
/* there is no g_list_prepend_link(): */
	peer->link_.next = transport->peers_list;
	peer->link_.data = peer;
/* update next entries previous link */
	if (transport->peers_list)
		transport->peers_list->prev = &peer->link_;
/* update head */
	transport->peers_list = &peer->link_;
	g_static_rw_lock_writer_unlock (&transport->peers_lock);

/* prod timer thread if sleeping */
	g_static_mutex_lock (&transport->mutex);
	if (pgm_time_after( transport->next_poll, peer->spmr_expiry ))
	{
		transport->next_poll = peer->spmr_expiry;
		g_trace ("INFO","new_peer: prod timer thread");
		if (!pgm_notify_send (&transport->timer_notify)) {
			g_critical ("notify to timer channel failed :(");
			/* retval = -EINVAL; */
		}
	}
	g_static_mutex_unlock (&transport->mutex);

	return peer;
}

/* data incoming on receive sockets, can be from a sender or receiver, or simply bogus.
 * for IPv4 we receive the IP header to handle fragmentation, for IPv6 we cannot, but the
 * underlying stack handles this for us.
 *
 * recvmsgv reads a vector of apdus each contained in a IO scatter/gather array.
 *
 * can be called due to event from incoming socket(s) or timer induced data loss.
 *
 * on success, returns bytes read, on error returns -1.
 */

gssize
pgm_transport_recvmsgv (
	pgm_transport_t*	transport,
	pgm_msgv_t*		msg_start,
	gsize			msg_len,
	int			flags		/* MSG_DONTWAIT for non-blocking */
	)
{
	g_trace ("INFO", "pgm_transport_recvmsgv");
	g_assert( msg_len > 0 );

/* reject on closed transport */
	if (!transport->is_open) {
		errno = ENOTCONN;
		return -1;
	}
	if (transport->has_lost_data) {
		pgm_rxw_t* lost_rxw = transport->peers_waiting->data;
		msg_start[0].msgv_iov = &transport->piov[0];
		transport->piov[0].iov_base = &lost_rxw->pgm_sock_err;
		if (transport->will_close_on_failure) {
			transport->is_open = FALSE;
		} else {
			transport->has_lost_data = !transport->has_lost_data;
		}
		errno = ECONNRESET;
		return -1;
	}

	gsize bytes_read = 0;
	guint data_read = 0;
	pgm_msgv_t* pmsg = msg_start;
	const pgm_msgv_t* msg_end = msg_start + msg_len;
	struct iovec* piov = transport->piov;
	const struct iovec* iov_end = piov + transport->piov_len;

/* lock waiting so extra events are not generated during call */
	g_static_mutex_lock (&transport->waiting_mutex);

/* second, flush any remaining contiguous messages from previous call(s) */
	if (transport->peers_waiting || transport->peers_committed)
	{
		while (transport->peers_committed)
		{
			pgm_rxw_t* committed_rxw = transport->peers_committed->data;

/* move any previous blocks to parity */
			pgm_rxw_release_committed (committed_rxw);

			transport->peers_committed->data = NULL;
			transport->peers_committed->next = NULL;
			transport->peers_committed = transport->peers_committed->next;
		}

		while (transport->peers_waiting)
		{
			pgm_rxw_t* waiting_rxw = transport->peers_waiting->data;
			const gssize peer_bytes_read = pgm_rxw_readv (waiting_rxw, &pmsg, msg_end - pmsg, &piov, iov_end - piov);

/* clean up completed transmission groups */
			pgm_rxw_free_committed (waiting_rxw);

			if (waiting_rxw->ack_cumulative_losses != waiting_rxw->cumulative_losses)
			{
				transport->has_lost_data = TRUE;
				waiting_rxw->pgm_sock_err.lost_count = waiting_rxw->cumulative_losses - waiting_rxw->ack_cumulative_losses;
				waiting_rxw->ack_cumulative_losses = waiting_rxw->cumulative_losses;
			}
	
			if (peer_bytes_read >= 0)
			{
/* add to release list */
				waiting_rxw->commit_link.data = waiting_rxw;
				waiting_rxw->commit_link.next = transport->peers_committed;
				transport->peers_committed = &waiting_rxw->commit_link;

				bytes_read += peer_bytes_read;
				data_read++;

				if (pgm_rxw_full(waiting_rxw)) 		/* window full */
				{
					goto out;
				}
	
				if (pmsg == msg_end || piov == iov_end)	/* commit full */
				{
					goto out;
				}
			}
			if (transport->has_lost_data)
			{
				goto out;
			}

/* next */
			transport->peers_waiting->data = NULL;
			transport->peers_waiting->next = NULL;
			transport->peers_waiting = transport->peers_waiting->next;
		}
	}

/* read the data:
 *
 * Buffer is always max_tpdu in length.  Ideally we have zero copy but the recv includes the ip & pgm headers and
 * the pgm options.  Over thousands of messages the gains by using less receive window memory are more conducive (maybe).
 *
 * We cannot actually block here as packets pushed by the timers need to be addressed too.
 */
	struct sockaddr_storage src_addr;
	ssize_t len;
	gsize bytes_received = 0;
	struct iovec iov = {
		.iov_base	= transport->rx_buffer,
		.iov_len	= transport->max_tpdu
	};
	size_t aux[1024 / sizeof(size_t)];
	struct msghdr msg = {
		.msg_name	= &src_addr,
		.msg_namelen	= sizeof(src_addr),
		.msg_iov	= &iov,
		.msg_iovlen	= 1,
		.msg_control	= aux,
		.msg_controllen = sizeof(aux),
		.msg_flags	= 0
	};

recv_again:

/* reset msghdr */
	msg.msg_namelen		= sizeof(src_addr);
	msg.msg_iov[0].iov_len	= transport->max_tpdu;
	msg.msg_iovlen		= 1;
	msg.msg_controllen	= sizeof(aux);

	len = recvmsg (transport->recv_sock, &msg, MSG_DONTWAIT);
	if (len < 0) {
		if (bytes_received) {
			goto flush_waiting;
		} else {
			goto check_for_repeat;
		}
	} else if (len == 0) {
		goto out;
	}

	bytes_received += len;

	struct sockaddr_storage dst_addr;
	socklen_t dst_addr_len;
	struct pgm_header *pgm_header;
	gpointer packet;
	gsize packet_len;
	int e;

/* successfully read packet */

	if (!transport->udp_encap_port &&
	    AF_INET == pgm_sockaddr_family(&src_addr))
	{
/* IPv4 PGM includes IP packet header which we can easily parse to grab destination multicast group
 */
		e = pgm_parse_raw(transport->rx_buffer, len, (struct sockaddr*)&dst_addr, &dst_addr_len, &pgm_header, &packet, &packet_len);
	}
	else
	{
/* UDP and IPv6 PGM requires use of IP control messages to get destination address
 */
		struct cmsghdr* cmsg;
		gboolean found_dstaddr = FALSE;

		for (cmsg = CMSG_FIRSTHDR(&msg); cmsg != NULL; cmsg = CMSG_NXTHDR(&msg, cmsg))
		{
			g_trace ("INFO", "cmsg: level=%d type=%d", cmsg->cmsg_level, cmsg->cmsg_type);
			if (IPPROTO_IP == cmsg->cmsg_level && 
			    IP_PKTINFO == cmsg->cmsg_type)
			{
				const struct in_pktinfo *in = (struct in_pktinfo*) CMSG_DATA(cmsg);
				dst_addr_len = sizeof(struct sockaddr_in);
				((struct sockaddr_in*)&dst_addr)->sin_family		= AF_INET;
				((struct sockaddr_in*)&dst_addr)->sin_addr.s_addr	= in->ipi_addr.s_addr;
				found_dstaddr = TRUE;
				break;
			}

			if (IPPROTO_IPV6 == cmsg->cmsg_level && 
			    IPV6_PKTINFO == cmsg->cmsg_type)
			{
				const struct in6_pktinfo *in6 = (struct in6_pktinfo*) CMSG_DATA(cmsg);
				dst_addr_len = sizeof(struct sockaddr_in6);
				((struct sockaddr_in6*)&dst_addr)->sin6_family		= AF_INET6;
				memcpy (&((struct sockaddr_in6*)&dst_addr)->sin6_addr, &in6->ipi6_addr, dst_addr_len);
				found_dstaddr = TRUE;
				break;
			}
		}

/* set any empty address if no headers found */
		if (!found_dstaddr)
		{
			g_trace("INFO","no destination address found in header");
			((struct sockaddr_in*)&dst_addr)->sin_family		= AF_INET;
			((struct sockaddr_in*)&dst_addr)->sin_addr.s_addr	= INADDR_ANY;
			dst_addr_len = sizeof(struct sockaddr_in);
		}

		e = pgm_parse_udp_encap(transport->rx_buffer, len, (struct sockaddr*)&dst_addr, &dst_addr_len, &pgm_header, &packet, &packet_len);
	}

	if (e < 0)
	{
/* TODO: difference between PGM_PC_SOURCE_CKSUM_ERRORS & PGM_PC_RECEIVER_CKSUM_ERRORS */
		if (e == -2)
			transport->cumulative_stats[PGM_PC_SOURCE_CKSUM_ERRORS]++;
		transport->cumulative_stats[PGM_PC_SOURCE_PACKETS_DISCARDED]++;
		goto check_for_repeat;
	}

/* calculate senders TSI */
	pgm_tsi_t tsi;
	memcpy (&tsi.gsi, pgm_header->pgm_gsi, sizeof(pgm_gsi_t));
	tsi.sport = pgm_header->pgm_sport;

//	g_trace ("INFO","tsi %s", pgm_print_tsi (&tsi));
	pgm_peer_t* source = NULL;

	if (pgm_is_upstream (pgm_header->pgm_type) || pgm_is_peer (pgm_header->pgm_type))
	{

/* upstream = receiver to source, peer-to-peer = receive to receiver
 *
 * NB: SPMRs can be upstream or peer-to-peer, if the packet is multicast then its
 *     a peer-to-peer message, if its unicast its an upstream message.
 */

		if (pgm_header->pgm_sport != transport->dport)
		{

/* its upstream/peer-to-peer for another session */

			transport->cumulative_stats[PGM_PC_SOURCE_PACKETS_DISCARDED]++;
			goto check_for_repeat;
		}

		if ( pgm_is_peer (pgm_header->pgm_type)
			&& pgm_sockaddr_is_addr_multicast ((struct sockaddr*)&dst_addr) )
		{

/* its a multicast peer-to-peer message */

			if ( pgm_header->pgm_dport == transport->tsi.sport )
			{

/* we are the source, propagate null as the source */

				source = NULL;

				if (!transport->can_send_data)
				{
					transport->cumulative_stats[PGM_PC_SOURCE_PACKETS_DISCARDED]++;
					goto check_for_repeat;
				}
			}
			else
			{
/* we are not the source */

				if (!transport->can_recv)
				{
					transport->cumulative_stats[PGM_PC_SOURCE_PACKETS_DISCARDED]++;
					goto check_for_repeat;
				}

/* check to see the source this peer-to-peer message is about is in our peer list */

				pgm_tsi_t source_tsi;
				memcpy (&source_tsi.gsi, &tsi.gsi, sizeof(pgm_gsi_t));
				source_tsi.sport = pgm_header->pgm_dport;

				g_static_rw_lock_reader_lock (&transport->peers_lock);
				source = g_hash_table_lookup (transport->peers_hashtable, &source_tsi);
				g_static_rw_lock_reader_unlock (&transport->peers_lock);
				if (source == NULL)
				{

/* this source is unknown, we don't care about messages about it */

					transport->cumulative_stats[PGM_PC_SOURCE_PACKETS_DISCARDED]++;
					goto check_for_repeat;
				}
			}
		}
		else if ( pgm_is_upstream (pgm_header->pgm_type)
			&& !pgm_sockaddr_is_addr_multicast ((struct sockaddr*)&dst_addr)
			&& ( pgm_header->pgm_dport == transport->tsi.sport )
			&& pgm_gsi_equal (&tsi.gsi, &transport->tsi.gsi) )
		{

/* unicast upstream message, note that dport & sport are reversed */

			source = NULL;

			if (!transport->can_send_data)
			{
				transport->cumulative_stats[PGM_PC_SOURCE_PACKETS_DISCARDED]++;
				goto check_for_repeat;
			}
		}
		else
		{

/* it is a mystery! */

			transport->cumulative_stats[PGM_PC_SOURCE_PACKETS_DISCARDED]++;
			goto check_for_repeat;
		}

		gpointer pgm_data = pgm_header + 1;
		gsize pgm_len = packet_len - sizeof(pgm_header);

		switch (pgm_header->pgm_type) {
		case PGM_NAK:
			if (source) {
				on_peer_nak (source, pgm_header, pgm_data, pgm_len);
			} else if (!pgm_sockaddr_is_addr_multicast ((struct sockaddr*)&dst_addr)) {
				on_nak (transport, pgm_header, pgm_data, pgm_len);
				goto check_for_repeat;
			} else {
/* ignore multicast NAKs as the source */
				transport->cumulative_stats[PGM_PC_SOURCE_PACKETS_DISCARDED]++;
				goto check_for_repeat;
			}
			break;

		case PGM_NNAK:	on_nnak (transport, pgm_header, pgm_data, pgm_len); break;
		case PGM_SPMR:	on_spmr (transport, source, pgm_header, pgm_data, pgm_len); break;
		case PGM_POLR:
		default:
			transport->cumulative_stats[PGM_PC_SOURCE_PACKETS_DISCARDED]++;
			goto check_for_repeat;
		}
	}
	else
	{

/* downstream = source to receivers */

		if (!pgm_is_downstream (pgm_header->pgm_type))
		{
			transport->cumulative_stats[PGM_PC_SOURCE_PACKETS_DISCARDED]++;
			goto check_for_repeat;
		}

/* pgm packet DPORT contains our transport DPORT */
		if (pgm_header->pgm_dport != transport->dport)
		{
			transport->cumulative_stats[PGM_PC_SOURCE_PACKETS_DISCARDED]++;
			goto check_for_repeat;
		}

		if (!transport->can_recv)
		{
			transport->cumulative_stats[PGM_PC_SOURCE_PACKETS_DISCARDED]++;
			goto check_for_repeat;
		}

/* search for TSI peer context or create a new one */
		g_static_rw_lock_reader_lock (&transport->peers_lock);
		source = g_hash_table_lookup (transport->peers_hashtable, &tsi);
		g_static_rw_lock_reader_unlock (&transport->peers_lock);
		if (source == NULL)
		{
			source = new_peer (transport, &tsi, (struct sockaddr*)&src_addr, pgm_sockaddr_len(&src_addr), (struct sockaddr*)&dst_addr, dst_addr_len);
		}
		else
		{
			source->last_packet = pgm_time_now;
		}

		source->cumulative_stats[PGM_PC_RECEIVER_BYTES_RECEIVED] += len;
		source->last_packet = pgm_time_now;

		const gpointer pgm_data = pgm_header + 1;
		const gsize pgm_len = packet_len - sizeof(pgm_header);

/* handle PGM packet type */
		switch (pgm_header->pgm_type) {
		case PGM_ODATA:	on_odata (source, pgm_header, pgm_data, pgm_len); break;
		case PGM_NCF:	on_ncf (source, pgm_header, pgm_data, pgm_len); break;
		case PGM_RDATA: on_rdata (source, pgm_header, pgm_data, pgm_len); break;
		case PGM_SPM:
			on_spm (source, pgm_header, pgm_data, pgm_len);

/* update group NLA if appropriate */
			if (pgm_sockaddr_is_addr_multicast ((struct sockaddr*)&dst_addr)) {
				memcpy (&source->group_nla, &dst_addr, dst_addr_len);
			}
			break;
		default:
			transport->cumulative_stats[PGM_PC_SOURCE_PACKETS_DISCARDED]++;
			goto check_for_repeat;
		}

	} /* downstream message */

/* check whether source has waiting data */
	if (source && ((pgm_rxw_t*)source->rxw)->is_waiting && !((pgm_rxw_t*)source->rxw)->waiting_link.data)
	{
		((pgm_rxw_t*)source->rxw)->waiting_link.data = source->rxw;
		((pgm_rxw_t*)source->rxw)->waiting_link.next = transport->peers_waiting;
		transport->peers_waiting = &((pgm_rxw_t*)source->rxw)->waiting_link;
		goto flush_waiting;	/* :D */
	}

	if (transport->peers_waiting)
	{
flush_waiting:
/* flush any congtiguous packets generated by the receipt of this packet */
		while (transport->peers_waiting)
		{
			pgm_rxw_t* waiting_rxw = transport->peers_waiting->data;
			const gssize peer_bytes_read = pgm_rxw_readv (waiting_rxw, &pmsg, msg_end - pmsg, &piov, iov_end - piov);

/* clean up completed transmission groups */
			pgm_rxw_free_committed (waiting_rxw);

			if (waiting_rxw->ack_cumulative_losses != waiting_rxw->cumulative_losses)
			{
				transport->has_lost_data = TRUE;
				waiting_rxw->pgm_sock_err.lost_count = waiting_rxw->cumulative_losses - waiting_rxw->ack_cumulative_losses;
				waiting_rxw->ack_cumulative_losses = waiting_rxw->cumulative_losses;
			}

			if (peer_bytes_read >= 0)
			{
/* add to release list */
				waiting_rxw->commit_link.data = waiting_rxw;
				waiting_rxw->commit_link.next = transport->peers_committed;
				transport->peers_committed = &waiting_rxw->commit_link;

				bytes_read += peer_bytes_read;
				data_read++;

				if (pgm_rxw_full(waiting_rxw)) 		/* window full */
				{
					goto out;
				}

				if (pmsg == msg_end || piov == iov_end) /* commit full */
				{
					goto out;
				}
			}
			if (transport->has_lost_data)
			{
				goto out;
			}
 
/* next */
			transport->peers_waiting->data = NULL;
			transport->peers_waiting->next = NULL;
			transport->peers_waiting = transport->peers_waiting->next;
		}
	}

check_for_repeat:
/* repeat if non-blocking and not full */
	if (flags & MSG_DONTWAIT)
	{
		if (len > 0 && pmsg < msg_end &&
			( ( data_read == 0 && msg_len == 1 ) ||		/* leave early with one apdu */
			( msg_len > 1 ) )				/* or wait for vector to fill up */
		)
		{
			g_trace ("SPM","recv again on not-full");
			goto recv_again;		/* \:D/ */
		}
	}
	else
	{
/* repeat if blocking and empty, i.e. received non data packet.
 */
		if (0 == data_read)
		{
			int n_fds = 2;
			struct pollfd fds[ n_fds ];
			memset (fds, 0, sizeof(fds));
			if (-1 == pgm_transport_poll_info (transport, fds, &n_fds, POLLIN)) {
				g_trace ("SPM", "poll_info returned errno=%i",errno);
				return -1;
			}

/* flush any waiting notifications */
			if (transport->is_waiting_read) {
				pgm_notify_clear (&transport->waiting_notify);
				transport->is_waiting_read = FALSE;
			}

/* spin the locks to allow other thread to set waiting state,
 * first run should trigger waiting pipe event which will flush and loop.
 */
			g_static_mutex_unlock (&transport->waiting_mutex);
			int events = poll (fds, n_fds, -1 /* timeout=∞ */);

			if (-1 == events) {
				g_trace ("SPM","poll returned errno=%i",errno);
				return events;
			}
			g_static_mutex_lock (&transport->waiting_mutex);

			if (fds[0].revents) {
				g_trace ("SPM","recv again on empty");
				goto recv_again;
			} else {
				g_trace ("SPM","state generated event");
				goto flush_waiting;
			}
		}
	}

out:
	if (0 == data_read)
	{
		if (transport->is_waiting_read) {
			pgm_notify_clear (&transport->waiting_notify);
			transport->is_waiting_read = FALSE;
		}

		g_static_mutex_unlock (&transport->waiting_mutex);

		if (transport->has_lost_data) {
			pgm_rxw_t* lost_rxw = transport->peers_waiting->data;
			msg_start[0].msgv_iov = &transport->piov[0];
			transport->piov[0].iov_base = &lost_rxw->pgm_sock_err;
			if (transport->will_close_on_failure) {
				transport->is_open = FALSE;
			} else {
				transport->has_lost_data = !transport->has_lost_data;
			}
			errno = ECONNRESET;
		} else {
			errno = EAGAIN;
		}

/* return reset on zero bytes instead of waiting for next call */
		return -1;
	}
	else if (transport->peers_waiting)
	{
		if (transport->is_waiting_read && transport->is_edge_triggered_recv)
		{
/* empty waiting-pipe */
			pgm_notify_clear (&transport->waiting_notify);
			transport->is_waiting_read = FALSE;
		}
		else if (!transport->is_waiting_read && !transport->is_edge_triggered_recv)
		{
/* fill waiting-pipe */
			if (!pgm_notify_send (&transport->waiting_notify)) {
				g_critical ("send to waiting notify channel failed :(");
			}
			transport->is_waiting_read = TRUE;
		}
	}

	g_static_mutex_unlock (&transport->waiting_mutex);
	return bytes_read;
}

/* read one contiguous apdu and return as a IO scatter/gather array.  msgv is owned by
 * the caller, tpdu contents are owned by the receive window.
 *
 * on success, returns the number of bytes read.  on error, -1 is returned, and
 * errno is set appropriately.
 */

gssize
pgm_transport_recvmsg (
	pgm_transport_t*	transport,
	pgm_msgv_t*		msgv,
	int			flags		/* MSG_DONTWAIT for non-blocking */
	)
{
	return pgm_transport_recvmsgv (transport, msgv, 1, flags);
}

/* vanilla read function.  copies from the receive window to the provided buffer
 * location.  the caller must provide an adequately sized buffer to store the largest
 * expected apdu or else it will be truncated.
 *
 * on success, returns the number of bytes read.  on error, -1 is returned, and
 * errno is set appropriately.
 */

gssize
pgm_transport_recvfrom (
	pgm_transport_t*	transport,
	gpointer		data,
	gsize			len,
	int			flags,		/* MSG_DONTWAIT for non-blocking */
	pgm_tsi_t*		from
	)
{
	pgm_msgv_t msgv;

	gssize bytes_read = pgm_transport_recvmsg (transport, &msgv, flags);

/* merge apdu packets together */
	if (bytes_read > 0)
	{
		gssize bytes_copied = 0;
		struct iovec* p = msgv.msgv_iov;

/* copy sender TSI to application buffer */
		if (from) {
			memcpy (from, msgv.msgv_tsi, sizeof(pgm_tsi_t));
		}

		do {
			size_t src_bytes = p->iov_len;
			g_assert (src_bytes > 0);

			if (bytes_copied + src_bytes > len) {
				g_error ("APDU truncated as provided buffer too small %" G_GSIZE_FORMAT " > %" G_GSIZE_FORMAT, bytes_read, len);
				src_bytes = len - bytes_copied;
				bytes_read = bytes_copied + src_bytes;
			}

			memcpy (data, p->iov_base, src_bytes);

			data = (char*)data + src_bytes;
			bytes_copied += src_bytes;
			p++;

		} while (bytes_copied < bytes_read);
	}
	else if (errno == ECONNRESET)
	{
		memcpy (data, msgv.msgv_iov->iov_base, MIN(sizeof(pgm_sock_err_t), len));
	}

	return bytes_read;
}

gssize
pgm_transport_recv (
	pgm_transport_t*	transport,
	gpointer		data,
	gsize			len,
	int			flags		/* MSG_DONTWAIT for non-blocking */
	)
{
	return pgm_transport_recvfrom (transport, data, len, flags, NULL);
}

/* add select parameters for the transports receive socket(s)
 *
 * returns highest file descriptor used plus one.
 */

int
pgm_transport_select_info (
	pgm_transport_t*	transport,
	fd_set*			readfds,
	fd_set*			writefds,
	int*			n_fds		/* in: max fds, out: max (in:fds, transport:fds) */
	)
{
	g_assert (transport);
	g_assert (n_fds);

	if (!transport->is_open) {
		errno = EBADF;
		return -1;
	}

	int fds = 0;

	if (readfds)
	{
		FD_SET(transport->recv_sock, readfds);
		fds = transport->recv_sock + 1;

		if (transport->can_recv) {
			int waiting_fd = pgm_notify_get_fd (&transport->waiting_notify);
			FD_SET(waiting_fd, readfds);
			fds = MAX(fds, waiting_fd + 1);
		}
	}

	if (transport->can_send_data && writefds)
	{
		FD_SET(transport->send_sock, writefds);

		fds = MAX(transport->send_sock + 1, fds);
	}

	return *n_fds = MAX(fds, *n_fds);
}

/* add poll parameters for this transports receive socket(s)
 *
 * returns number of pollfd structures filled.
 */

int
pgm_transport_poll_info (
	pgm_transport_t*	transport,
	struct pollfd*		fds,
	int*			n_fds,		/* in: #fds, out: used #fds */
	int			events		/* POLLIN, POLLOUT */
	)
{
	g_assert (transport);
	g_assert (fds);
	g_assert (n_fds);

	if (!transport->is_open) {
		errno = EBADF;
		return -1;
	}

	int moo = 0;

/* we currently only support one incoming socket */
	if (events & POLLIN)
	{
		g_assert ( (1 + moo) <= *n_fds );
		fds[moo].fd = transport->recv_sock;
		fds[moo].events = POLLIN;
		moo++;
		if (transport->can_recv)
		{
			g_assert ( (1 + moo) <= *n_fds );
			fds[moo].fd = pgm_notify_get_fd (&transport->waiting_notify);
			fds[moo].events = POLLIN;
			moo++;
		}
	}

/* ODATA only published on regular socket, no need to poll router-alert sock */
	if (transport->can_send_data && events & POLLOUT)
	{
		g_assert ( (1 + moo) <= *n_fds );
		fds[moo].fd = transport->send_sock;
		fds[moo].events = POLLOUT;
		moo++;
	}

	return *n_fds = moo;
}

/* add epoll parameters for this transports recieve socket(s), events should
 * be set to EPOLLIN to wait for incoming events (data), and EPOLLOUT to wait
 * for non-blocking write.
 *
 * returns 0 on success, -1 on failure and sets errno appropriately.
 */
#ifdef CONFIG_HAVE_EPOLL
int
pgm_transport_epoll_ctl (
	pgm_transport_t*	transport,
	int			epfd,
	int			op,		/* EPOLL_CTL_ADD, ... */
	int			events		/* EPOLLIN, EPOLLOUT */
	)
{
	if (op != EPOLL_CTL_ADD)	/* only addition currently supported */
	{
		errno = EINVAL;
		return -1;
	}
	else if (!transport->is_open)
	{
		errno = EBADF;
		return -1;
	}

	struct epoll_event event;
	int retval = 0;

	if (events & EPOLLIN)
	{
		event.events = events & (EPOLLIN | EPOLLET);
		event.data.ptr = transport;
		retval = epoll_ctl (epfd, op, transport->recv_sock, &event);
		if (retval) {
			goto out;
		}

		if (transport->can_recv)
		{
			retval = epoll_ctl (epfd, op, pgm_notify_get_fd (&transport->waiting_notify), &event);
			if (retval) {
				goto out;
			}
		}

		if (events & EPOLLET) {
			transport->is_edge_triggered_recv = TRUE;
		}
	}

	if (transport->can_send_data && events & EPOLLOUT)
	{
		event.events = events & (EPOLLOUT | EPOLLET);
		event.data.ptr = transport;
		retval = epoll_ctl (epfd, op, transport->send_sock, &event);
	}
out:
	return retval;
}
#endif

/* prototype of function to send pro-active parity NAKs.
 */
static inline int
pgm_schedule_proactive_nak (
	pgm_transport_t*	transport,
	guint32			nak_tg_sqn	/* transmission group (shifted) */
	)
{
	int retval = 0;

	pgm_txw_retransmit_push (transport->txw,
				 nak_tg_sqn | transport->rs_proactive_h,
				 TRUE /* is_parity */,
				 transport->tg_sqn_shift);
	if (!pgm_notify_send (&transport->rdata_notify)) {
		g_critical ("send to rdata notify channel failed :(");
		retval = -EINVAL;
	}
	return retval;
}

/* a deferred request for RDATA, now processing in the timer thread, we check the transmit
 * window to see if the packet exists and forward on, maintaining a lock until the queue is
 * empty.
 *
 * returns TRUE to keep monitoring the event source.
 */

static gboolean
on_nak_notify (
	G_GNUC_UNUSED GIOChannel*	source,
	G_GNUC_UNUSED GIOCondition	condition,
	gpointer			data
	)
{
	pgm_transport_t* transport = data;

/* remove one event from notify channel */
	pgm_notify_read (&transport->rdata_notify);

/* We can flush queue and block all odata, or process one set, or process each
 * sequence number individually.
 */
	guint32		r_sqn;
	gpointer	r_packet = NULL;
	guint16		r_length = 0;
	gboolean	is_parity = FALSE;
	guint		rs_h = 0;
	const guint	rs_2t = transport->rs_n - transport->rs_k;

/* parity packets are re-numbered across the transmission group with index h, sharing the space
 * with the original packets.  beyond the transmission group size (k), the PGM option OPT_PARITY_GRP
 * provides the extra offset value.
 */

/* peek from the retransmit queue so we can eliminate duplicate NAKs up until the repair packet
 * has been retransmitted.
 */
	g_static_rw_lock_reader_lock (&transport->txw_lock);
	if (!pgm_txw_retransmit_try_peek (transport->txw, &r_sqn, &r_packet, &r_length, &is_parity, &rs_h))
	{
		gboolean is_var_pktlen = FALSE;
		gboolean has_saved_partial_csum = TRUE;

/* calculate parity packet */
		if (is_parity)
		{
			const guint32 tg_sqn_mask = 0xffffffff << transport->tg_sqn_shift;
			const guint32 tg_sqn = r_sqn & tg_sqn_mask;

			gboolean is_op_encoded = FALSE;

			guint16 parity_length = 0;
			const guint8* src[ transport->rs_k ];
			for (unsigned i = 0; i < transport->rs_k; i++)
			{
				gpointer	o_packet;
				guint16		o_length;

				pgm_txw_peek (transport->txw, tg_sqn + i, &o_packet, &o_length);

				const struct pgm_header*	o_header = o_packet;
				guint16				o_tsdu_length = g_ntohs (o_header->pgm_tsdu_length);

				if (!parity_length)
				{
					parity_length = o_tsdu_length;
				}
				else if (o_tsdu_length != parity_length)
				{
					is_var_pktlen = TRUE;

					if (o_tsdu_length > parity_length)
						parity_length = o_tsdu_length;
				}

				const struct pgm_data* odata = (const struct pgm_data*)(o_header + 1);

				if (o_header->pgm_options & PGM_OPT_PRESENT)
				{
					const guint16 opt_total_length = g_ntohs(*(const guint16*)( (const char*)( odata + 1 ) + sizeof(guint16)));
					src[i] = (const guint8*)(odata + 1) + opt_total_length;
					is_op_encoded = TRUE;
				}
				else
				{
					src[i] = (const guint8*)(odata + 1);
				}
			}

/* construct basic PGM header to be completed by send_rdata() */
			struct pgm_header*	r_header = (struct pgm_header*)transport->parity_buffer;
			struct pgm_data*	rdata  = (struct pgm_data*)(r_header + 1);
			memcpy (r_header->pgm_gsi, &transport->tsi.gsi, sizeof(pgm_gsi_t));
			r_header->pgm_options = PGM_OPT_PARITY;

/* append actual TSDU length if variable length packets, zero pad as necessary.
 */
			if (is_var_pktlen)
			{
				r_header->pgm_options |= PGM_OPT_VAR_PKTLEN;

				for (unsigned i = 0; i < transport->rs_k; i++)
				{
					gpointer	o_packet;
					guint16		o_length;

					pgm_txw_peek (transport->txw, tg_sqn + i, &o_packet, &o_length);

					const struct pgm_header*	o_header = o_packet;
					const guint16			o_tsdu_length = g_ntohs (o_header->pgm_tsdu_length);

					pgm_txw_zero_pad (transport->txw, o_packet, o_tsdu_length, parity_length);
					*(guint16*)((guint8*)o_packet + parity_length) = o_tsdu_length;
				}
				parity_length += 2;
			}

			r_header->pgm_tsdu_length = g_htons (parity_length);
			rdata->data_sqn		= g_htonl ( tg_sqn | rs_h );

			gpointer data_bytes	= rdata + 1;

			r_packet	= r_header;
			r_length	= sizeof(struct pgm_header) + sizeof(struct pgm_data) + parity_length;

/* encode every option separately, currently only one applies: opt_fragment
 */
			if (is_op_encoded)
			{
				r_header->pgm_options |= PGM_OPT_PRESENT;

				struct pgm_opt_fragment null_opt_fragment;
				guint8* opt_src[ transport->rs_k ];
				memset (&null_opt_fragment, 0, sizeof(null_opt_fragment));
				*(guint8*)&null_opt_fragment |= PGM_OP_ENCODED_NULL;
				for (unsigned i = 0; i < transport->rs_k; i++)
				{
					gpointer	o_packet;
					guint16		o_length;

					pgm_txw_peek (transport->txw, tg_sqn + i, &o_packet, &o_length);

					struct pgm_header*	o_header = o_packet;
					struct pgm_data*	odata = (struct pgm_data*)(o_header + 1);

					struct pgm_opt_fragment* opt_fragment;
					if ((o_header->pgm_options & PGM_OPT_PRESENT) && get_opt_fragment((gpointer)(odata + 1), &opt_fragment))
					{
/* skip three bytes of header */
						opt_src[i] = (guint8*)opt_fragment + sizeof (struct pgm_opt_header);
					}
					else
					{
						opt_src[i] = (guint8*)&null_opt_fragment;
					}
				}

/* add options to this rdata packet */
				struct pgm_opt_length* opt_len = (struct pgm_opt_length*)(rdata + 1);
				opt_len->opt_type	= PGM_OPT_LENGTH;
				opt_len->opt_length	= sizeof(struct pgm_opt_length);
				opt_len->opt_total_length = g_htons (	sizeof(struct pgm_opt_length) +
									sizeof(struct pgm_opt_header) +
									sizeof(struct pgm_opt_fragment) );
				struct pgm_opt_header* opt_header = (struct pgm_opt_header*)(opt_len + 1);
				opt_header->opt_type	= PGM_OPT_FRAGMENT | PGM_OPT_END;
				opt_header->opt_length	= sizeof(struct pgm_opt_header) + sizeof(struct pgm_opt_fragment);
				opt_header->opt_reserved = PGM_OP_ENCODED;
				struct pgm_opt_fragment* opt_fragment = (struct pgm_opt_fragment*)(opt_header + 1);

/* The cast below is the correct way to handle the problem. 
 * The (void *) cast is to avoid a GCC warning like: 
 *
 *   "warning: dereferencing type-punned pointer will break strict-aliasing rules"
 */
				pgm_rs_encode (transport->rs, (const void**)(void*)opt_src, transport->rs_k + rs_h, opt_fragment + sizeof(struct pgm_opt_header), sizeof(struct pgm_opt_fragment) - sizeof(struct pgm_opt_header));

				data_bytes = opt_fragment + 1;

				r_length += sizeof(struct pgm_opt_length) + sizeof(struct pgm_opt_header) + sizeof(struct pgm_opt_fragment);
			}

/* encode payload */
			pgm_rs_encode (transport->rs, (const void**)(void*)src, transport->rs_k + rs_h, data_bytes, parity_length);
			has_saved_partial_csum = FALSE;
		}

		send_rdata (transport, r_sqn, r_packet, r_length, has_saved_partial_csum);

/* now remove sequence number from retransmit queue, re-enabling NAK requests for this packet */
		pgm_txw_retransmit_pop (transport->txw, rs_2t);
	}
	g_static_rw_lock_reader_unlock (&transport->txw_lock);

	return TRUE;
}

/* prod to wakeup timer thread
 *
 * returns TRUE to keep monitoring the event source.
 */

static gboolean
on_timer_notify (
	G_GNUC_UNUSED GIOChannel*	source,
	G_GNUC_UNUSED GIOCondition	condition,
	gpointer			data
	)
{
	pgm_transport_t* transport = data;

/* empty notify channel */
	pgm_notify_clear (&transport->timer_notify);

	return TRUE;
}

static gboolean
on_timer_shutdown (
	G_GNUC_UNUSED GIOChannel*	source,
	G_GNUC_UNUSED GIOCondition	condition,
	gpointer			data
	)
{
	pgm_transport_t* transport = data;
	g_main_loop_quit (transport->timer_loop);
	return FALSE;
}
	

/* SPM indicate start of a session, continued presence of a session, or flushing final packets
 * of a session.
 *
 * returns -EINVAL on invalid packet or duplicate SPM sequence number.
 */

static int
on_spm (
	pgm_peer_t*		sender,
	struct pgm_header*	header,
	gpointer		data,		/* data will be changed to host order on demand */
	gsize			len
	)
{
	int retval;

	if ((retval = pgm_verify_spm (header, data, len)) != 0)
	{
		sender->cumulative_stats[PGM_PC_RECEIVER_MALFORMED_SPMS]++;
		sender->cumulative_stats[PGM_PC_RECEIVER_PACKETS_DISCARDED]++;
		goto out;
	}

	struct pgm_transport_t* transport = sender->transport;
	struct pgm_spm* spm = (struct pgm_spm*)data;
	struct pgm_spm6* spm6 = (struct pgm_spm6*)data;
	const pgm_time_t now = pgm_time_update_now ();

	spm->spm_sqn = g_ntohl (spm->spm_sqn);

/* check for advancing sequence number, or first SPM */
	g_static_mutex_lock (&transport->mutex);
	g_static_mutex_lock (&sender->mutex);
	if ( pgm_uint32_gte (spm->spm_sqn, sender->spm_sqn)
		|| ( ((struct sockaddr*)&sender->nla)->sa_family == 0 ) )
	{
/* copy NLA for replies */
		pgm_nla_to_sockaddr (&spm->spm_nla_afi, (struct sockaddr*)&sender->nla);

/* save sequence number */
		sender->spm_sqn = spm->spm_sqn;

/* update receive window */
		pgm_time_t nak_rb_expiry = now + nak_rb_ivl(transport);
		guint naks = pgm_rxw_window_update (sender->rxw,
							g_ntohl (spm->spm_trail),
							g_ntohl (spm->spm_lead),
							transport->rs_k,
							transport->tg_sqn_shift,
							nak_rb_expiry);
		if (naks && pgm_time_after(transport->next_poll, nak_rb_expiry))
		{
			transport->next_poll = nak_rb_expiry;
			g_trace ("INFO","on_spm: prod timer thread");
			if (!pgm_notify_send (&transport->timer_notify)) {
				g_critical ("send to timer notify channel failed :(");
				retval = -EINVAL;
			}
		}

/* mark receiver window for flushing on next recv() */
		pgm_rxw_t* sender_rxw = (pgm_rxw_t*)sender->rxw;
		if (sender_rxw->cumulative_losses != sender_rxw->ack_cumulative_losses &&
		    !sender_rxw->waiting_link.data)
		{
			transport->has_lost_data = TRUE;
			sender_rxw->pgm_sock_err.lost_count = sender_rxw->cumulative_losses - sender_rxw->ack_cumulative_losses;
			sender_rxw->ack_cumulative_losses = sender_rxw->cumulative_losses;

			sender_rxw->waiting_link.data = sender_rxw;
			sender_rxw->waiting_link.next = transport->peers_waiting;
			transport->peers_waiting = &sender_rxw->waiting_link;
		}
	}
	else
	{	/* does not advance SPM sequence number */
		sender->cumulative_stats[PGM_PC_RECEIVER_DUP_SPMS]++;
		sender->cumulative_stats[PGM_PC_RECEIVER_PACKETS_DISCARDED]++;
		retval = -EINVAL;
	}

/* check whether peer can generate parity packets */
	if (header->pgm_options & PGM_OPT_PRESENT)
	{
		struct pgm_opt_length* opt_len = (spm->spm_nla_afi == AFI_IP6) ?
							(struct pgm_opt_length*)(spm6 + 1) :
							(struct pgm_opt_length*)(spm + 1);
		if (opt_len->opt_type != PGM_OPT_LENGTH)
		{
			retval = -EINVAL;
			sender->cumulative_stats[PGM_PC_RECEIVER_MALFORMED_SPMS]++;
			sender->cumulative_stats[PGM_PC_RECEIVER_PACKETS_DISCARDED]++;
			goto out_unlock;
		}
		if (opt_len->opt_length != sizeof(struct pgm_opt_length))
		{
			retval = -EINVAL;
			sender->cumulative_stats[PGM_PC_RECEIVER_MALFORMED_SPMS]++;
			sender->cumulative_stats[PGM_PC_RECEIVER_PACKETS_DISCARDED]++;
			goto out_unlock;
		}
/* TODO: check for > 16 options & past packet end */
		struct pgm_opt_header* opt_header = (struct pgm_opt_header*)opt_len;
		do {
			opt_header = (struct pgm_opt_header*)((char*)opt_header + opt_header->opt_length);

			if ((opt_header->opt_type & PGM_OPT_MASK) == PGM_OPT_PARITY_PRM)
			{
				struct pgm_opt_parity_prm* opt_parity_prm = (struct pgm_opt_parity_prm*)(opt_header + 1);

				if ((opt_parity_prm->opt_reserved & PGM_PARITY_PRM_MASK) == 0)
				{
					retval = -EINVAL;
					sender->cumulative_stats[PGM_PC_RECEIVER_MALFORMED_SPMS]++;
					sender->cumulative_stats[PGM_PC_RECEIVER_PACKETS_DISCARDED]++;
					goto out_unlock;
				}

				guint32 parity_prm_tgs = g_ntohl (opt_parity_prm->parity_prm_tgs);
				if (parity_prm_tgs < 2 || parity_prm_tgs > 128)
				{
					retval = -EINVAL;
					sender->cumulative_stats[PGM_PC_RECEIVER_MALFORMED_SPMS]++;
					sender->cumulative_stats[PGM_PC_RECEIVER_PACKETS_DISCARDED]++;
					goto out_unlock;
				}
			
				sender->use_proactive_parity = opt_parity_prm->opt_reserved & PGM_PARITY_PRM_PRO;
				sender->use_ondemand_parity = opt_parity_prm->opt_reserved & PGM_PARITY_PRM_OND;
				if (sender->rs_k != parity_prm_tgs)
				{
					sender->rs_n = PGM_RS_DEFAULT_N;
					sender->rs_k = parity_prm_tgs;
					sender->tg_sqn_shift = pgm_power2_log2 (sender->rs_k);
					if (sender->rs) {
						g_trace ("INFO", "Destroying existing Reed-Solomon state for peer.");
						pgm_rs_destroy (sender->rs);
					}
					g_trace ("INFO", "Enabling Reed-Solomon forward error correction for peer, RS(%i,%i)",
						sender->rs_n, sender->rs_k);
					pgm_rs_create (&sender->rs, sender->rs_n, sender->rs_k);
				}
				break;
			}
		} while (!(opt_header->opt_type & PGM_OPT_END));
	}

/* either way bump expiration timer */
	sender->expiry = now + transport->peer_expiry;
	sender->spmr_expiry = 0;
out_unlock:
	g_static_mutex_unlock (&sender->mutex);
	g_static_mutex_unlock (&transport->mutex);

out:
	return retval;
}

/* SPMR indicates if multicast to cancel own SPMR, or unicast to send SPM.
 *
 * rate limited to 1/IHB_MIN per TSI (13.4).
 *
 * if SPMR was valid, returns 0.
 */

static int
on_spmr (
	pgm_transport_t*	transport,
	pgm_peer_t*		peer,
	struct pgm_header*	header,
	gpointer		data,
	gsize			len
	)
{
	g_trace ("INFO","on_spmr()");

	int retval;

	if ((retval = pgm_verify_spmr (header, data, len)) == 0)
	{

/* we are the source */
		if (peer == NULL)
		{
			send_spm (transport);
		}
		else
		{
/* we are a peer */
			g_trace ("INFO", "suppressing SPMR due to peer multicast SPMR.");
			g_static_mutex_lock (&peer->mutex);
			peer->spmr_expiry = 0;
			g_static_mutex_unlock (&peer->mutex);
		}
	}
	else
	{
		transport->cumulative_stats[PGM_PC_SOURCE_PACKETS_DISCARDED]++;
	}

	return retval;
}

/* NAK requesting RDATA transmission for a sending transport, only valid if
 * sequence number(s) still in transmission window.
 *
 * we can potentially have different IP versions for the NAK packet to the send group.
 *
 * TODO: fix IPv6 AFIs
 *
 * take in a NAK and pass off to an asynchronous queue for another thread to process
 *
 * if NAK is valid, returns 0.  on error, -EINVAL is returned.
 */

static int
on_nak (
	pgm_transport_t*	transport,
	struct pgm_header*	header,
	gpointer		data,
	gsize			len
	)
{
	g_trace ("INFO","on_nak()");

	const gboolean is_parity = header->pgm_options & PGM_OPT_PARITY;

	if (is_parity) {
		transport->cumulative_stats[PGM_PC_SOURCE_PARITY_NAKS_RECEIVED]++;

		if (!transport->use_ondemand_parity) {
			transport->cumulative_stats[PGM_PC_SOURCE_MALFORMED_NAKS]++;
			transport->cumulative_stats[PGM_PC_SOURCE_PACKETS_DISCARDED]++;
			goto out;
		}
	} else {
		transport->cumulative_stats[PGM_PC_SOURCE_SELECTIVE_NAKS_RECEIVED]++;
	}

	int retval;
	if ((retval = pgm_verify_nak (header, data, len)) != 0)
	{
		transport->cumulative_stats[PGM_PC_SOURCE_MALFORMED_NAKS]++;
		transport->cumulative_stats[PGM_PC_SOURCE_PACKETS_DISCARDED]++;
		goto out;
	}

	const struct pgm_nak* nak = (struct pgm_nak*)data;
	const struct pgm_nak6* nak6 = (struct pgm_nak6*)data;
		
/* NAK_SRC_NLA contains our transport unicast NLA */
	struct sockaddr_storage nak_src_nla;
	pgm_nla_to_sockaddr (&nak->nak_src_nla_afi, (struct sockaddr*)&nak_src_nla);

	if (pgm_sockaddr_cmp ((struct sockaddr*)&nak_src_nla, (struct sockaddr*)&transport->send_addr) != 0) {
		retval = -EINVAL;
		transport->cumulative_stats[PGM_PC_SOURCE_MALFORMED_NAKS]++;
		transport->cumulative_stats[PGM_PC_SOURCE_PACKETS_DISCARDED]++;
		goto out;
	}

/* NAK_GRP_NLA containers our transport multicast group */ 
	struct sockaddr_storage nak_grp_nla;
	pgm_nla_to_sockaddr ((nak->nak_src_nla_afi == AFI_IP6) ? &nak6->nak6_grp_nla_afi : &nak->nak_grp_nla_afi,
				(struct sockaddr*)&nak_grp_nla);

	if (pgm_sockaddr_cmp ((struct sockaddr*)&nak_grp_nla, (struct sockaddr*)&transport->send_gsr.gsr_group) != 0) {
		retval = -EINVAL;
		transport->cumulative_stats[PGM_PC_SOURCE_MALFORMED_NAKS]++;
		transport->cumulative_stats[PGM_PC_SOURCE_PACKETS_DISCARDED]++;
		goto out;
	}

/* create queue object */
	pgm_sqn_list_t sqn_list;
	sqn_list.sqn[0] = g_ntohl (nak->nak_sqn);
	sqn_list.len = 1;

	g_trace ("INFO", "nak_sqn %" G_GUINT32_FORMAT, sqn_list.sqn[0]);

/* check NAK list */
	const guint32* nak_list = NULL;
	guint nak_list_len = 0;
	if (header->pgm_options & PGM_OPT_PRESENT)
	{
		const struct pgm_opt_length* opt_len = (nak->nak_src_nla_afi == AFI_IP6) ?
							(const struct pgm_opt_length*)(nak6 + 1) :
							(const struct pgm_opt_length*)(nak + 1);
		if (opt_len->opt_type != PGM_OPT_LENGTH)
		{
			retval = -EINVAL;
			transport->cumulative_stats[PGM_PC_SOURCE_MALFORMED_NAKS]++;
			transport->cumulative_stats[PGM_PC_SOURCE_PACKETS_DISCARDED]++;
			goto out;
		}
		if (opt_len->opt_length != sizeof(struct pgm_opt_length))
		{
			retval = -EINVAL;
			transport->cumulative_stats[PGM_PC_SOURCE_MALFORMED_NAKS]++;
			transport->cumulative_stats[PGM_PC_SOURCE_PACKETS_DISCARDED]++;
			goto out;
		}
/* TODO: check for > 16 options & past packet end */
		const struct pgm_opt_header* opt_header = (const struct pgm_opt_header*)opt_len;
		do {
			opt_header = (const struct pgm_opt_header*)((const char*)opt_header + opt_header->opt_length);

			if ((opt_header->opt_type & PGM_OPT_MASK) == PGM_OPT_NAK_LIST)
			{
				nak_list = ((const struct pgm_opt_nak_list*)(opt_header + 1))->opt_sqn;
				nak_list_len = ( opt_header->opt_length - sizeof(struct pgm_opt_header) - sizeof(guint8) ) / sizeof(guint32);
				break;
			}
		} while (!(opt_header->opt_type & PGM_OPT_END));
	}

/* nak list numbers */
#ifdef TRANSPORT_DEBUG
	if (nak_list)
	{
		char nak_sz[1024] = "";
		const guint32 *nakp = nak_list, *nake = nak_list + nak_list_len;
		while (nakp < nake) {
			char tmp[1024];
			sprintf (tmp, "%" G_GUINT32_FORMAT " ", g_ntohl(*nakp));
			strcat (nak_sz, tmp);
			nakp++;
		}
	g_trace ("INFO", "nak list %s", nak_sz);
	}
#endif
	for (unsigned i = 0; i < nak_list_len; i++)
	{
		sqn_list.sqn[sqn_list.len++] = g_ntohl (*nak_list);
		nak_list++;
	}

/* send NAK confirm packet immediately, then defer to timer thread for a.s.a.p
 * delivery of the actual RDATA packets.
 */
	if (nak_list_len) {
		send_ncf_list (transport, (struct sockaddr*)&nak_src_nla, (struct sockaddr*)&nak_grp_nla, &sqn_list, is_parity);
	} else {
		send_ncf (transport, (struct sockaddr*)&nak_src_nla, (struct sockaddr*)&nak_grp_nla, sqn_list.sqn[0], is_parity);
	}

/* queue retransmit requests */
	for (unsigned i = 0; i < sqn_list.len; i++)
	{
		int cnt = pgm_txw_retransmit_push (transport->txw, sqn_list.sqn[i], is_parity, transport->tg_sqn_shift);
		if (cnt > 0)
		{
			if (!pgm_notify_send (&transport->rdata_notify)) {
				g_critical ("send to rdata notify channel failed :(");
				retval = -EINVAL;
			}
		}
	}

out:
	return retval;
}

/* Multicast peer-to-peer NAK handling, pretty much the same as a NCF but different direction
 *
 * if NAK is valid, returns 0.  on error, -EINVAL is returned.
 */

static int
on_peer_nak (
	pgm_peer_t*		peer,
	struct pgm_header*	header,
	gpointer		data,
	gsize			len
	)
{
	g_trace ("INFO","on_peer_nak()");

	int retval;
	pgm_transport_t* transport = peer->transport;

	if ((retval = pgm_verify_nak (header, data, len)) != 0)
	{
		g_trace ("INFO", "Invalid NAK, ignoring.");
		peer->cumulative_stats[PGM_PC_RECEIVER_NAK_ERRORS]++;
		peer->cumulative_stats[PGM_PC_RECEIVER_PACKETS_DISCARDED]++;
		goto out;
	}

	const struct pgm_nak* nak = (struct pgm_nak*)data;
	const struct pgm_nak6* nak6 = (struct pgm_nak6*)data;
		
/* NAK_SRC_NLA must not contain our transport unicast NLA */
	struct sockaddr_storage nak_src_nla;
	pgm_nla_to_sockaddr (&nak->nak_src_nla_afi, (struct sockaddr*)&nak_src_nla);

	if (pgm_sockaddr_cmp ((struct sockaddr*)&nak_src_nla, (struct sockaddr*)&transport->send_addr) == 0) {
		retval = -EINVAL;
		peer->cumulative_stats[PGM_PC_RECEIVER_PACKETS_DISCARDED]++;
		goto out;
	}

/* NAK_GRP_NLA contains one of our transport receive multicast groups: the sources send multicast group */ 
	struct sockaddr_storage nak_grp_nla;
	pgm_nla_to_sockaddr ((nak->nak_src_nla_afi == AFI_IP6) ? &nak6->nak6_grp_nla_afi : &nak->nak_grp_nla_afi,
				(struct sockaddr*)&nak_grp_nla);

	gboolean found = FALSE;
	for (unsigned i = 0; i < transport->recv_gsr_len; i++)
	{
		if (pgm_sockaddr_cmp ((struct sockaddr*)&nak_grp_nla, (struct sockaddr*)&transport->recv_gsr[i].gsr_group) == 0)
		{
			found = TRUE;
		}
	}

	if (!found) {
		g_trace ("INFO", "NAK not destined for this multicast group.");
		retval = -EINVAL;
		peer->cumulative_stats[PGM_PC_RECEIVER_PACKETS_DISCARDED]++;
		goto out;
	}

	g_static_mutex_lock (&transport->mutex);
	g_static_mutex_lock (&peer->mutex);

/* handle as NCF */
	pgm_time_update_now();
	pgm_rxw_ncf (peer->rxw, g_ntohl (nak->nak_sqn), pgm_time_now + transport->nak_rdata_ivl, pgm_time_now + nak_rb_ivl(transport));

/* check NAK list */
	const guint32* nak_list = NULL;
	guint nak_list_len = 0;
	if (header->pgm_options & PGM_OPT_PRESENT)
	{
		const struct pgm_opt_length* opt_len = (nak->nak_src_nla_afi == AFI_IP6) ?
							(const struct pgm_opt_length*)(nak6 + 1) :
							(const struct pgm_opt_length*)(nak + 1);
		if (opt_len->opt_type != PGM_OPT_LENGTH)
		{
			g_trace ("INFO", "First PGM Option in NAK incorrect, ignoring.");
			retval = -EINVAL;
			peer->cumulative_stats[PGM_PC_RECEIVER_MALFORMED_NCFS]++;
			peer->cumulative_stats[PGM_PC_RECEIVER_PACKETS_DISCARDED]++;
			goto out_unlock;
		}
		if (opt_len->opt_length != sizeof(struct pgm_opt_length))
		{
			g_trace ("INFO", "PGM Length Option has incorrect length, ignoring.");
			retval = -EINVAL;
			peer->cumulative_stats[PGM_PC_RECEIVER_MALFORMED_NCFS]++;
			peer->cumulative_stats[PGM_PC_RECEIVER_PACKETS_DISCARDED]++;
			goto out_unlock;
		}
/* TODO: check for > 16 options & past packet end */
		const struct pgm_opt_header* opt_header = (const struct pgm_opt_header*)opt_len;
		do {
			opt_header = (const struct pgm_opt_header*)((const char*)opt_header + opt_header->opt_length);

			if ((opt_header->opt_type & PGM_OPT_MASK) == PGM_OPT_NAK_LIST)
			{
				nak_list = ((const struct pgm_opt_nak_list*)(opt_header + 1))->opt_sqn;
				nak_list_len = ( opt_header->opt_length - sizeof(struct pgm_opt_header) - sizeof(guint8) ) / sizeof(guint32);
				break;
			}
		} while (!(opt_header->opt_type & PGM_OPT_END));
	}

	g_trace ("INFO", "NAK contains 1+%i sequence numbers.", nak_list_len);
	while (nak_list_len)
	{
		pgm_rxw_ncf (peer->rxw, g_ntohl (*nak_list), pgm_time_now + transport->nak_rdata_ivl, pgm_time_now + nak_rb_ivl(transport));
		nak_list++;
		nak_list_len--;
	}

/* mark receiver window for flushing on next recv() */
	pgm_rxw_t* peer_rxw = (pgm_rxw_t*)peer->rxw;
	if (peer_rxw->cumulative_losses != peer_rxw->ack_cumulative_losses &&
	    !peer_rxw->waiting_link.data)
	{
		transport->has_lost_data = TRUE;
		peer_rxw->pgm_sock_err.lost_count = peer_rxw->cumulative_losses - peer_rxw->ack_cumulative_losses;
		peer_rxw->ack_cumulative_losses = peer_rxw->cumulative_losses;

		peer_rxw->waiting_link.data = peer_rxw;
		peer_rxw->waiting_link.next = transport->peers_waiting;
		transport->peers_waiting = &peer_rxw->waiting_link;
	}

out_unlock:
	g_static_mutex_unlock (&peer->mutex);
	g_static_mutex_unlock (&transport->mutex);

out:
	return retval;
}

/* NCF confirming receipt of a NAK from this transport or another on the LAN segment.
 *
 * Packet contents will match exactly the sent NAK, although not really that helpful.
 *
 * if NCF is valid, returns 0.  on error, -EINVAL is returned.
 */

static int
on_ncf (
	pgm_peer_t*		peer,
	struct pgm_header*	header,
	gpointer		data,
	gsize			len
	)
{
	g_trace ("INFO","on_ncf()");

	int retval;
	pgm_transport_t* transport = peer->transport;

	if ((retval = pgm_verify_ncf (header, data, len)) != 0)
	{
		g_trace ("INFO", "Invalid NCF, ignoring.");
		peer->cumulative_stats[PGM_PC_RECEIVER_MALFORMED_NCFS]++;
		peer->cumulative_stats[PGM_PC_RECEIVER_PACKETS_DISCARDED]++;
		goto out;
	}

	const struct pgm_nak* ncf = (struct pgm_nak*)data;
	const struct pgm_nak6* ncf6 = (struct pgm_nak6*)data;
		
/* NCF_SRC_NLA may contain our transport unicast NLA, we don't really care */
	struct sockaddr_storage ncf_src_nla;
	pgm_nla_to_sockaddr (&ncf->nak_src_nla_afi, (struct sockaddr*)&ncf_src_nla);

#if 0
	if (pgm_sockaddr_cmp ((struct sockaddr*)&ncf_src_nla, (struct sockaddr*)&transport->send_addr) != 0) {
		retval = -EINVAL;
		peer->cumulative_stats[PGM_PC_RECEIVER_PACKETS_DISCARDED]++;
		goto out;
	}
#endif

/* NCF_GRP_NLA contains our transport multicast group */ 
	struct sockaddr_storage ncf_grp_nla;
	pgm_nla_to_sockaddr ((ncf->nak_src_nla_afi == AFI_IP6) ? &ncf6->nak6_grp_nla_afi : &ncf->nak_grp_nla_afi,
				(struct sockaddr*)&ncf_grp_nla);

	if (pgm_sockaddr_cmp ((struct sockaddr*)&ncf_grp_nla, (struct sockaddr*)&transport->send_gsr.gsr_group) != 0) {
		g_trace ("INFO", "NCF not destined for this multicast group.");
		retval = -EINVAL;
		peer->cumulative_stats[PGM_PC_RECEIVER_PACKETS_DISCARDED]++;
		goto out;
	}

	g_static_mutex_lock (&transport->mutex);
	g_static_mutex_lock (&peer->mutex);

	pgm_time_update_now();
	pgm_rxw_ncf (peer->rxw, g_ntohl (ncf->nak_sqn), pgm_time_now + transport->nak_rdata_ivl, pgm_time_now + nak_rb_ivl(transport));

/* check NCF list */
	const guint32* ncf_list = NULL;
	guint ncf_list_len = 0;
	if (header->pgm_options & PGM_OPT_PRESENT)
	{
		const struct pgm_opt_length* opt_len = (ncf->nak_src_nla_afi == AFI_IP6) ?
							(const struct pgm_opt_length*)(ncf6 + 1) :
							(const struct pgm_opt_length*)(ncf + 1);
		if (opt_len->opt_type != PGM_OPT_LENGTH)
		{
			g_trace ("INFO", "First PGM Option in NCF incorrect, ignoring.");
			retval = -EINVAL;
			peer->cumulative_stats[PGM_PC_RECEIVER_MALFORMED_NCFS]++;
			peer->cumulative_stats[PGM_PC_RECEIVER_PACKETS_DISCARDED]++;
			goto out_unlock;
		}
		if (opt_len->opt_length != sizeof(struct pgm_opt_length))
		{
			g_trace ("INFO", "PGM Length Option has incorrect length, ignoring.");
			retval = -EINVAL;
			peer->cumulative_stats[PGM_PC_RECEIVER_MALFORMED_NCFS]++;
			peer->cumulative_stats[PGM_PC_RECEIVER_PACKETS_DISCARDED]++;
			goto out_unlock;
		}
/* TODO: check for > 16 options & past packet end */
		const struct pgm_opt_header* opt_header = (const struct pgm_opt_header*)opt_len;
		do {
			opt_header = (const struct pgm_opt_header*)((const char*)opt_header + opt_header->opt_length);

			if ((opt_header->opt_type & PGM_OPT_MASK) == PGM_OPT_NAK_LIST)
			{
				ncf_list = ((const struct pgm_opt_nak_list*)(opt_header + 1))->opt_sqn;
				ncf_list_len = ( opt_header->opt_length - sizeof(struct pgm_opt_header) - sizeof(guint8) ) / sizeof(guint32);
				break;
			}
		} while (!(opt_header->opt_type & PGM_OPT_END));
	}

	g_trace ("INFO", "NCF contains 1+%i sequence numbers.", ncf_list_len);
	while (ncf_list_len)
	{
		pgm_rxw_ncf (peer->rxw, g_ntohl (*ncf_list), pgm_time_now + transport->nak_rdata_ivl, pgm_time_now + nak_rb_ivl(transport));
		ncf_list++;
		ncf_list_len--;
	}

/* mark receiver window for flushing on next recv() */
	pgm_rxw_t* peer_rxw = (pgm_rxw_t*)peer->rxw;
	if (peer_rxw->cumulative_losses != peer_rxw->ack_cumulative_losses &&
	    !peer_rxw->waiting_link.data)
	{
		transport->has_lost_data = TRUE;
		peer_rxw->pgm_sock_err.lost_count = peer_rxw->cumulative_losses - peer_rxw->ack_cumulative_losses;
		peer_rxw->ack_cumulative_losses = peer_rxw->cumulative_losses;

		peer_rxw->waiting_link.data = peer_rxw;
		peer_rxw->waiting_link.next = transport->peers_waiting;
		transport->peers_waiting = &peer_rxw->waiting_link;
	}

out_unlock:
	g_static_mutex_unlock (&peer->mutex);
	g_static_mutex_unlock (&transport->mutex);

out:
	return retval;
}

/* Null-NAK, or N-NAK propogated by a DLR for hand waving excitement
 *
 * if NNAK is valid, returns 0.  on error, -EINVAL is returned.
 */

static int
on_nnak (
	pgm_transport_t*	transport,
	struct pgm_header*	header,
	gpointer		data,
	gsize			len
	)
{
	g_trace ("INFO","on_nnak()");
	transport->cumulative_stats[PGM_PC_SOURCE_SELECTIVE_NNAK_PACKETS_RECEIVED]++;

	int retval;
	if ((retval = pgm_verify_nnak (header, data, len)) != 0)
	{
		transport->cumulative_stats[PGM_PC_SOURCE_NNAK_ERRORS]++;
		transport->cumulative_stats[PGM_PC_SOURCE_PACKETS_DISCARDED]++;
		goto out;
	}

	const struct pgm_nak* nnak = (struct pgm_nak*)data;
	const struct pgm_nak6* nnak6 = (struct pgm_nak6*)data;
		
/* NAK_SRC_NLA contains our transport unicast NLA */
	struct sockaddr_storage nnak_src_nla;
	pgm_nla_to_sockaddr (&nnak->nak_src_nla_afi, (struct sockaddr*)&nnak_src_nla);

	if (pgm_sockaddr_cmp ((struct sockaddr*)&nnak_src_nla, (struct sockaddr*)&transport->send_addr) != 0) {
		retval = -EINVAL;
		transport->cumulative_stats[PGM_PC_SOURCE_NNAK_ERRORS]++;
		transport->cumulative_stats[PGM_PC_SOURCE_PACKETS_DISCARDED]++;
		goto out;
	}

/* NAK_GRP_NLA containers our transport multicast group */ 
	struct sockaddr_storage nnak_grp_nla;
	pgm_nla_to_sockaddr ((nnak->nak_src_nla_afi == AFI_IP6) ? &nnak6->nak6_grp_nla_afi : &nnak->nak_grp_nla_afi,
				(struct sockaddr*)&nnak_grp_nla);

	if (pgm_sockaddr_cmp ((struct sockaddr*)&nnak_grp_nla, (struct sockaddr*)&transport->send_gsr.gsr_group) != 0) {
		retval = -EINVAL;
		transport->cumulative_stats[PGM_PC_SOURCE_NNAK_ERRORS]++;
		transport->cumulative_stats[PGM_PC_SOURCE_PACKETS_DISCARDED]++;
		goto out;
	}

/* check NNAK list */
	guint nnak_list_len = 0;
	if (header->pgm_options & PGM_OPT_PRESENT)
	{
		const struct pgm_opt_length* opt_len = (nnak->nak_src_nla_afi == AFI_IP6) ?
							(const struct pgm_opt_length*)(nnak6 + 1) :
							(const struct pgm_opt_length*)(nnak + 1);
		if (opt_len->opt_type != PGM_OPT_LENGTH)
		{
			retval = -EINVAL;
			transport->cumulative_stats[PGM_PC_SOURCE_NNAK_ERRORS]++;
			transport->cumulative_stats[PGM_PC_SOURCE_PACKETS_DISCARDED]++;
			goto out;
		}
		if (opt_len->opt_length != sizeof(struct pgm_opt_length))
		{
			retval = -EINVAL;
			transport->cumulative_stats[PGM_PC_SOURCE_NNAK_ERRORS]++;
			transport->cumulative_stats[PGM_PC_SOURCE_PACKETS_DISCARDED]++;
			goto out;
		}
/* TODO: check for > 16 options & past packet end */
		const struct pgm_opt_header* opt_header = (const struct pgm_opt_header*)opt_len;
		do {
			opt_header = (const struct pgm_opt_header*)((const char*)opt_header + opt_header->opt_length);

			if ((opt_header->opt_type & PGM_OPT_MASK) == PGM_OPT_NAK_LIST)
			{
				nnak_list_len = ( opt_header->opt_length - sizeof(struct pgm_opt_header) - sizeof(guint8) ) / sizeof(guint32);
				break;
			}
		} while (!(opt_header->opt_type & PGM_OPT_END));
	}

	transport->cumulative_stats[PGM_PC_SOURCE_SELECTIVE_NNAKS_RECEIVED] += 1 + nnak_list_len;

out:
	return retval;
}

/* ambient/heartbeat SPM's
 *
 * heartbeat: ihb_tmr decaying between ihb_min and ihb_max 2x after last packet
 *
 * on success, 0 is returned.  on error, -1 is returned, and errno set
 * appropriately.
 */

static inline int
send_spm (
	pgm_transport_t*	transport
	)
{
	g_static_mutex_lock (&transport->mutex);
	int result = send_spm_unlocked (transport);
	g_static_mutex_unlock (&transport->mutex);
	return result;
}

static int
send_spm_unlocked (
	pgm_transport_t*	transport
	)
{
	g_trace ("SPM","send_spm");

/* recycles a transport global packet */
	struct pgm_header *header = (struct pgm_header*)transport->spm_packet;
	struct pgm_spm *spm = (struct pgm_spm*)(header + 1);

	spm->spm_sqn		= g_htonl (transport->spm_sqn++);
	g_static_rw_lock_reader_lock (&transport->txw_lock);
	spm->spm_trail		= g_htonl (pgm_txw_trail(transport->txw));
	spm->spm_lead		= g_htonl (pgm_txw_lead(transport->txw));
	g_static_rw_lock_reader_unlock (&transport->txw_lock);

/* checksum optional for SPMs */
	header->pgm_checksum	= 0;
	header->pgm_checksum	= pgm_csum_fold (pgm_csum_partial ((char*)header, transport->spm_len, 0));

	gssize sent = pgm_sendto (transport,
				TRUE,				/* rate limited */
				TRUE,				/* with router alert */
				header,
				transport->spm_len,
				MSG_CONFIRM,			/* not expecting a reply */
				(struct sockaddr*)&transport->send_gsr.gsr_group,
				pgm_sockaddr_len(&transport->send_gsr.gsr_group));

	if ( sent != (gssize)transport->spm_len )
	{
		return -1;
	}

	transport->cumulative_stats[PGM_PC_SOURCE_BYTES_SENT] += transport->spm_len;
	return 0;
}

/* send SPM-request to a new peer, this packet type has no contents
 *
 * on success, 0 is returned.  on error, -1 is returned, and errno set
 * appropriately.
 */

static int
send_spmr (
	pgm_peer_t*	peer
	)
{
	g_trace ("INFO","send_spmr");

	pgm_transport_t* transport = peer->transport;

/* cache peer information */
	const guint16	peer_sport = peer->tsi.sport;
	struct sockaddr_storage peer_nla;
	memcpy (&peer_nla, &peer->local_nla, sizeof(struct sockaddr_storage));

	const gsize tpdu_length = sizeof(struct pgm_header);
	guint8 buf[ tpdu_length ];
	struct pgm_header *header = (struct pgm_header*)buf;
	memcpy (header->pgm_gsi, &peer->tsi.gsi, sizeof(pgm_gsi_t));
/* dport & sport reversed communicating upstream */
	header->pgm_sport	= transport->dport;
	header->pgm_dport	= peer_sport;
	header->pgm_type	= PGM_SPMR;
	header->pgm_options	= 0;
	header->pgm_tsdu_length	= 0;
	header->pgm_checksum	= 0;
	header->pgm_checksum	= pgm_csum_fold (pgm_csum_partial ((char*)header, tpdu_length, 0));

/* send multicast SPMR TTL 1 */
	g_trace ("INFO", "send multicast SPMR to %s", inet_ntoa( ((struct sockaddr_in*)&transport->send_gsr.gsr_group)->sin_addr ));
	pgm_sockaddr_multicast_hops (transport->send_sock, pgm_sockaddr_family(&transport->send_gsr.gsr_group), 1);
	gssize sent = pgm_sendto (transport,
				FALSE,			/* not rate limited */
				FALSE,			/* regular socket */
				header,
				tpdu_length,
				MSG_CONFIRM,		/* not expecting a reply */
				(struct sockaddr*)&transport->send_gsr.gsr_group,
				pgm_sockaddr_len(&transport->send_gsr.gsr_group));

/* send unicast SPMR with regular TTL */
	g_trace ("INFO", "send unicast SPMR to %s", inet_ntoa( ((struct sockaddr_in*)&peer->local_nla)->sin_addr ));
	pgm_sockaddr_multicast_hops (transport->send_sock, pgm_sockaddr_family(&transport->send_gsr.gsr_group), transport->hops);
	sent += pgm_sendto (transport,
				FALSE,
				FALSE,
				header,
				tpdu_length,
				MSG_CONFIRM,		/* not expecting a reply */
				(struct sockaddr*)&peer_nla,
				pgm_sockaddr_len(&peer_nla));

	peer->spmr_expiry = 0;

	if ( sent != (gssize)(tpdu_length * 2) ) 
	{
		return -1;
	}

	transport->cumulative_stats[PGM_PC_SOURCE_BYTES_SENT] += tpdu_length * 2;

	return 0;
}

/* send selective NAK for one sequence number.
 *
 * on success, 0 is returned.  on error, -1 is returned, and errno set
 * appropriately.
 */
static int
send_nak (
	pgm_peer_t*		peer,
	guint32			sequence_number
	)
{
#ifdef TRANSPORT_DEBUG
	char s[INET6_ADDRSTRLEN];
	pgm_sockaddr_ntop(&peer->nla, s, sizeof(s));
	g_trace ("INFO", "send_nak(%" G_GUINT32_FORMAT ") -> %s:%hu", sequence_number, s, g_ntohs(((struct sockaddr_in*)&peer->nla)->sin_port));
#endif

	pgm_transport_t* transport = peer->transport;

	const guint16	peer_sport = peer->tsi.sport;
	struct sockaddr_storage peer_nla;
	memcpy (&peer_nla, &peer->nla, sizeof(struct sockaddr_storage));

	gsize tpdu_length = sizeof(struct pgm_header) + sizeof(struct pgm_nak);
	if (pgm_sockaddr_family(&peer_nla) == AF_INET6) {
		tpdu_length += sizeof(struct pgm_nak6) - sizeof(struct pgm_nak);
	}
	guint8 buf[ tpdu_length ];

	struct pgm_header *header = (struct pgm_header*)buf;
	struct pgm_nak *nak = (struct pgm_nak*)(header + 1);
	struct pgm_nak6 *nak6 = (struct pgm_nak6*)(header + 1);
	memcpy (header->pgm_gsi, &peer->tsi.gsi, sizeof(pgm_gsi_t));

/* dport & sport swap over for a nak */
	header->pgm_sport	= transport->dport;
	header->pgm_dport	= peer_sport;
	header->pgm_type        = PGM_NAK;
        header->pgm_options     = 0;
        header->pgm_tsdu_length = 0;

/* NAK */
	nak->nak_sqn		= g_htonl (sequence_number);

/* source nla */
	pgm_sockaddr_to_nla ((struct sockaddr*)&peer_nla, (char*)&nak->nak_src_nla_afi);

/* group nla: we match the NAK NLA to the same as advertised by the source, we might
 * be listening to multiple multicast groups
 */
	pgm_sockaddr_to_nla ((struct sockaddr*)&peer->group_nla, (nak->nak_src_nla_afi == AFI_IP6) ?
								(char*)&nak6->nak6_grp_nla_afi :
								(char*)&nak->nak_grp_nla_afi );

        header->pgm_checksum    = 0;
        header->pgm_checksum	= pgm_csum_fold (pgm_csum_partial ((char*)header, tpdu_length, 0));

	gssize sent = pgm_sendto (transport,
				FALSE,			/* not rate limited */
				TRUE,			/* with router alert */
				header,
				tpdu_length,
				MSG_CONFIRM,		/* not expecting a reply */
				(struct sockaddr*)&peer_nla,
				pgm_sockaddr_len(&peer_nla));

	if ( sent != (gssize)tpdu_length )
	{
		return -1;
	}

	peer->cumulative_stats[PGM_PC_RECEIVER_SELECTIVE_NAK_PACKETS_SENT]++;
	peer->cumulative_stats[PGM_PC_RECEIVER_SELECTIVE_NAKS_SENT]++;

	return 0;
}

/* send a NAK confirm (NCF) message with provided sequence number list.
 *
 * on success, 0 is returned.  on error, -1 is returned, and errno set
 * appropriately.
 */

static int
send_ncf (
	pgm_transport_t*	transport,
	struct sockaddr*	nak_src_nla,
	struct sockaddr*	nak_grp_nla,
	guint32			sequence_number,
	gboolean		is_parity		/* send parity NCF */
	)
{
	g_trace ("INFO", "send_ncf()");

	gsize tpdu_length = sizeof(struct pgm_header) + sizeof(struct pgm_nak);
	if (pgm_sockaddr_family(nak_src_nla) == AF_INET6) {
		tpdu_length += sizeof(struct pgm_nak6) - sizeof(struct pgm_nak);
	}
	guint8 buf[ tpdu_length ];

	struct pgm_header *header = (struct pgm_header*)buf;
	struct pgm_nak *ncf = (struct pgm_nak*)(header + 1);
	struct pgm_nak6 *ncf6 = (struct pgm_nak6*)(header + 1);
	memcpy (header->pgm_gsi, &transport->tsi.gsi, sizeof(pgm_gsi_t));
	
	header->pgm_sport	= transport->tsi.sport;
	header->pgm_dport	= transport->dport;
	header->pgm_type        = PGM_NCF;
        header->pgm_options     = is_parity ? PGM_OPT_PARITY : 0;
        header->pgm_tsdu_length = 0;

/* NCF */
	ncf->nak_sqn		= g_htonl (sequence_number);

/* source nla */
	pgm_sockaddr_to_nla (nak_src_nla, (char*)&ncf->nak_src_nla_afi);

/* group nla */
	pgm_sockaddr_to_nla (nak_grp_nla, (ncf->nak_src_nla_afi == AFI_IP6) ?
						(char*)&ncf6->nak6_grp_nla_afi :
						(char*)&ncf->nak_grp_nla_afi );

        header->pgm_checksum    = 0;
        header->pgm_checksum	= pgm_csum_fold (pgm_csum_partial ((char*)header, tpdu_length, 0));

	gssize sent = pgm_sendto (transport,
				FALSE,			/* not rate limited */
				TRUE,			/* with router alert */
				header,
				tpdu_length,
				MSG_CONFIRM,		/* not expecting a reply */
				(struct sockaddr*)&transport->send_gsr.gsr_group,
				pgm_sockaddr_len(&transport->send_gsr.gsr_group));

	if ( sent != (gssize)tpdu_length )
	{
		return -1;
	}

	transport->cumulative_stats[PGM_PC_SOURCE_BYTES_SENT] += tpdu_length;

	return 0;
}

/* Send a parity NAK requesting on-demand parity packet generation.
 *
 * on success, 0 is returned.  on error, -1 is returned, and errno set
 * appropriately.
 */
static int
send_parity_nak (
	pgm_peer_t*		peer,
	guint32			nak_tg_sqn,	/* transmission group (shifted) */
	guint32			nak_pkt_cnt	/* count of parity packets to request */
	)
{
	g_trace ("INFO", "send_parity_nak(%u, %u)", nak_tg_sqn, nak_pkt_cnt);

	pgm_transport_t* transport = peer->transport;

	guint16	peer_sport = peer->tsi.sport;
	struct sockaddr_storage peer_nla;
	memcpy (&peer_nla, &peer->nla, sizeof(struct sockaddr_storage));

	gsize tpdu_length = sizeof(struct pgm_header) + sizeof(struct pgm_nak);
	if (pgm_sockaddr_family(&peer_nla) == AF_INET6) {
		tpdu_length += sizeof(struct pgm_nak6) - sizeof(struct pgm_nak);
	}
	guint8 buf[ tpdu_length ];

	struct pgm_header *header = (struct pgm_header*)buf;
	struct pgm_nak *nak = (struct pgm_nak*)(header + 1);
	struct pgm_nak6 *nak6 = (struct pgm_nak6*)(header + 1);
	memcpy (header->pgm_gsi, &peer->tsi.gsi, sizeof(pgm_gsi_t));

/* dport & sport swap over for a nak */
	header->pgm_sport	= transport->dport;
	header->pgm_dport	= peer_sport;
	header->pgm_type        = PGM_NAK;
        header->pgm_options     = PGM_OPT_PARITY;	/* this is a parity packet */
        header->pgm_tsdu_length = 0;

/* NAK */
	nak->nak_sqn		= g_htonl (nak_tg_sqn | (nak_pkt_cnt - 1) );

/* source nla */
	pgm_sockaddr_to_nla ((struct sockaddr*)&peer_nla, (char*)&nak->nak_src_nla_afi);

/* group nla: we match the NAK NLA to the same as advertised by the source, we might
 * be listening to multiple multicast groups
 */
	pgm_sockaddr_to_nla ((struct sockaddr*)&peer->group_nla, (nak->nak_src_nla_afi == AFI_IP6) ?
									(char*)&nak6->nak6_grp_nla_afi :
									(char*)&nak->nak_grp_nla_afi );

        header->pgm_checksum    = 0;
        header->pgm_checksum	= pgm_csum_fold (pgm_csum_partial ((char*)header, tpdu_length, 0));

	gssize sent = pgm_sendto (transport,
				FALSE,			/* not rate limited */
				TRUE,			/* with router alert */
				header,
				tpdu_length,
				MSG_CONFIRM,		/* not expecting a reply */
				(struct sockaddr*)&peer_nla,
				pgm_sockaddr_len(&peer_nla));

	if ( sent != (gssize)tpdu_length )
	{
		return -1;
	}

	peer->cumulative_stats[PGM_PC_RECEIVER_PARITY_NAK_PACKETS_SENT]++;
	peer->cumulative_stats[PGM_PC_RECEIVER_PARITY_NAKS_SENT]++;

	return 0;
}

/* A NAK packet with a OPT_NAK_LIST option extension
 *
 * on success, 0 is returned.  on error, -1 is returned, and errno set
 * appropriately.
 */

#ifndef PGM_SINGLE_NAK
static int
send_nak_list (
	pgm_peer_t*	peer,
	pgm_sqn_list_t*	sqn_list
	)
{
	g_assert (sqn_list->len > 1);
	g_assert (sqn_list->len <= 63);

	pgm_transport_t* transport = peer->transport;

	guint16	peer_sport = peer->tsi.sport;
	struct sockaddr_storage peer_nla;
	memcpy (&peer_nla, &peer->nla, sizeof(struct sockaddr_storage));

	gsize tpdu_length = sizeof(struct pgm_header) + sizeof(struct pgm_nak)
			+ sizeof(struct pgm_opt_length)		/* includes header */
			+ sizeof(struct pgm_opt_header) + sizeof(struct pgm_opt_nak_list)
			+ ( (sqn_list->len-1) * sizeof(guint32) );
	if (pgm_sockaddr_family(&peer_nla) == AFI_IP6) {
		tpdu_length += sizeof(struct pgm_nak6) - sizeof(struct pgm_nak);
	}
	guint8 buf[ tpdu_length ];

	struct pgm_header *header = (struct pgm_header*)buf;
	struct pgm_nak *nak = (struct pgm_nak*)(header + 1);
	struct pgm_nak6 *nak6 = (struct pgm_nak6*)(header + 1);
	memcpy (header->pgm_gsi, &peer->tsi.gsi, sizeof(pgm_gsi_t));

/* dport & sport swap over for a nak */
	header->pgm_sport	= transport->dport;
	header->pgm_dport	= peer_sport;
	header->pgm_type        = PGM_NAK;
        header->pgm_options     = PGM_OPT_PRESENT | PGM_OPT_NETWORK;
        header->pgm_tsdu_length = 0;

/* NAK */
	nak->nak_sqn		= g_htonl (sqn_list->sqn[0]);

/* source nla */
	pgm_sockaddr_to_nla ((struct sockaddr*)&peer_nla, (char*)&nak->nak_src_nla_afi);

/* group nla */
	pgm_sockaddr_to_nla ((struct sockaddr*)&peer->group_nla, (nak->nak_src_nla_afi == AFI_IP6) ? 
								(char*)&nak6->nak6_grp_nla_afi :
								(char*)&nak->nak_grp_nla_afi );

/* OPT_NAK_LIST */
	struct pgm_opt_length* opt_len = (nak->nak_src_nla_afi == AFI_IP6) ? 
						(struct pgm_opt_length*)(nak6 + 1) :
						(struct pgm_opt_length*)(nak + 1);
	opt_len->opt_type	= PGM_OPT_LENGTH;
	opt_len->opt_length	= sizeof(struct pgm_opt_length);
	opt_len->opt_total_length = g_htons (	sizeof(struct pgm_opt_length) +
						sizeof(struct pgm_opt_header) +
						sizeof(struct pgm_opt_nak_list) +
						( (sqn_list->len-1) * sizeof(guint32) ) );
	struct pgm_opt_header* opt_header = (struct pgm_opt_header*)(opt_len + 1);
	opt_header->opt_type	= PGM_OPT_NAK_LIST | PGM_OPT_END;
	opt_header->opt_length	= sizeof(struct pgm_opt_header) + sizeof(struct pgm_opt_nak_list)
				+ ( (sqn_list->len-1) * sizeof(guint32) );
	struct pgm_opt_nak_list* opt_nak_list = (struct pgm_opt_nak_list*)(opt_header + 1);
	opt_nak_list->opt_reserved = 0;

#ifdef TRANSPORT_DEBUG
	char s[INET6_ADDRSTRLEN];
	pgm_sockaddr_ntop(&peer->nla, s, sizeof(s));
	char nak1[1024];
	sprintf (nak1, "send_nak_list( %" G_GUINT32_FORMAT " + [", sqn_list->sqn[0]);
#endif
	for (unsigned i = 1; i < sqn_list->len; i++) {
		opt_nak_list->opt_sqn[i-1] = g_htonl (sqn_list->sqn[i]);

#ifdef TRANSPORT_DEBUG
		char nak2[1024];
		sprintf (nak2, "%" G_GUINT32_FORMAT " ", sqn_list->sqn[i]);
		strcat (nak1, nak2);
#endif
	}

#ifdef TRANSPORT_DEBUG
	g_trace ("INFO", "%s]%i ) -> %s:%hu", nak1, sqn_list->len, s, g_ntohs(((struct sockaddr_in*)&peer->nla)->sin_port));
#endif

        header->pgm_checksum    = 0;
        header->pgm_checksum	= pgm_csum_fold (pgm_csum_partial ((char*)header, tpdu_length, 0));

	gssize sent = pgm_sendto (transport,
				FALSE,			/* not rate limited */
				FALSE,			/* regular socket */
				header,
				tpdu_length,
				MSG_CONFIRM,		/* not expecting a reply */
				(struct sockaddr*)&peer_nla,
				pgm_sockaddr_len(&peer_nla));

	if ( sent != (gssize)tpdu_length )
	{
		return -1;
	}

	peer->cumulative_stats[PGM_PC_RECEIVER_SELECTIVE_NAK_PACKETS_SENT]++;
	peer->cumulative_stats[PGM_PC_RECEIVER_SELECTIVE_NAKS_SENT] += 1 + sqn_list->len;

	return 0;
}

/* A NCF packet with a OPT_NAK_LIST option extension
 *
 * on success, 0 is returned.  on error, -1 is returned, and errno set
 * appropriately.
 */
static int
send_ncf_list (
	pgm_transport_t*	transport,
	struct sockaddr*	nak_src_nla,
	struct sockaddr*	nak_grp_nla,
	pgm_sqn_list_t*		sqn_list,
	gboolean		is_parity		/* send parity NCF */
	)
{
	g_assert (sqn_list->len > 1);
	g_assert (sqn_list->len <= 63);
	g_assert (pgm_sockaddr_family(nak_src_nla) == pgm_sockaddr_family(nak_grp_nla));

	gsize tpdu_length = sizeof(struct pgm_header) + sizeof(struct pgm_nak)
			+ sizeof(struct pgm_opt_length)		/* includes header */
			+ sizeof(struct pgm_opt_header) + sizeof(struct pgm_opt_nak_list)
			+ ( (sqn_list->len-1) * sizeof(guint32) );
	if (pgm_sockaddr_family(nak_src_nla) == AFI_IP6) {
		tpdu_length += sizeof(struct pgm_nak6) - sizeof(struct pgm_nak);
	}
	guint8 buf[ tpdu_length ];

	struct pgm_header *header = (struct pgm_header*)buf;
	struct pgm_nak *ncf = (struct pgm_nak*)(header + 1);
	struct pgm_nak6 *ncf6 = (struct pgm_nak6*)(header + 1);
	memcpy (header->pgm_gsi, &transport->tsi.gsi, sizeof(pgm_gsi_t));

	header->pgm_sport	= transport->tsi.sport;
	header->pgm_dport	= transport->dport;
	header->pgm_type        = PGM_NCF;
        header->pgm_options     = is_parity ? (PGM_OPT_PRESENT | PGM_OPT_NETWORK | PGM_OPT_PARITY) : (PGM_OPT_PRESENT | PGM_OPT_NETWORK);
        header->pgm_tsdu_length = 0;

/* NCF */
	ncf->nak_sqn		= g_htonl (sqn_list->sqn[0]);

/* source nla */
	pgm_sockaddr_to_nla (nak_src_nla, (char*)&ncf->nak_src_nla_afi);

/* group nla */
	pgm_sockaddr_to_nla (nak_grp_nla, (ncf->nak_src_nla_afi == AFI_IP6) ? 
						(char*)&ncf6->nak6_grp_nla_afi :
						(char*)&ncf->nak_grp_nla_afi );

/* OPT_NAK_LIST */
	struct pgm_opt_length* opt_len = (ncf->nak_src_nla_afi == AFI_IP6) ?
						(struct pgm_opt_length*)(ncf6 + 1) :
						(struct pgm_opt_length*)(ncf + 1);
	opt_len->opt_type	= PGM_OPT_LENGTH;
	opt_len->opt_length	= sizeof(struct pgm_opt_length);
	opt_len->opt_total_length = g_htons (	sizeof(struct pgm_opt_length) +
						sizeof(struct pgm_opt_header) +
						sizeof(struct pgm_opt_nak_list) +
						( (sqn_list->len-1) * sizeof(guint32) ) );
	struct pgm_opt_header* opt_header = (struct pgm_opt_header*)(opt_len + 1);
	opt_header->opt_type	= PGM_OPT_NAK_LIST | PGM_OPT_END;
	opt_header->opt_length	= sizeof(struct pgm_opt_header) + sizeof(struct pgm_opt_nak_list)
				+ ( (sqn_list->len-1) * sizeof(guint32) );
	struct pgm_opt_nak_list* opt_nak_list = (struct pgm_opt_nak_list*)(opt_header + 1);
	opt_nak_list->opt_reserved = 0;

#ifdef TRANSPORT_DEBUG
	char nak1[1024];
	sprintf (nak1, "send_ncf_list( %" G_GUINT32_FORMAT " + [", sqn_list->sqn[0]);
#endif
	for (unsigned i = 1; i < sqn_list->len; i++) {
		opt_nak_list->opt_sqn[i-1] = g_htonl (sqn_list->sqn[i]);

#ifdef TRANSPORT_DEBUG
		char nak2[1024];
		sprintf (nak2, "%" G_GUINT32_FORMAT " ", sqn_list->sqn[i]);
		strcat (nak1, nak2);
#endif
	}

#ifdef TRANSPORT_DEBUG
	g_trace ("INFO", "%s]%i )", nak1, sqn_list->len);
#endif

        header->pgm_checksum    = 0;
        header->pgm_checksum	= pgm_csum_fold (pgm_csum_partial ((char*)header, tpdu_length, 0));

	gssize sent = pgm_sendto (transport,
				FALSE,			/* not rate limited */
				TRUE,			/* with router alert */
				header,
				tpdu_length,
				MSG_CONFIRM,		/* not expecting a reply */
				(struct sockaddr*)&transport->send_gsr.gsr_group,
				pgm_sockaddr_len(&transport->send_gsr.gsr_group));

	if ( sent != (gssize)tpdu_length )
	{
		return -1;
	}

	transport->cumulative_stats[PGM_PC_SOURCE_BYTES_SENT] += tpdu_length;

	return 0;
}
#endif /* !PGM_SINGLE_NAK */

/* check all receiver windows for packets in BACK-OFF_STATE, on expiration send a NAK.
 * update transport::next_nak_rb_timestamp for next expiration time.
 *
 * peer object is locked before entry.
 */

static void
nak_rb_state (
	pgm_peer_t*		peer
	)
{
	pgm_rxw_t* rxw = (pgm_rxw_t*)peer->rxw;
	pgm_transport_t* transport = peer->transport;
	GList* list;
#ifndef PGM_SINGLE_NAK
	pgm_sqn_list_t nak_list;
	nak_list.len = 0;
#endif

	g_trace ("INFO", "nak_rb_state(len=%u)", g_list_length(rxw->backoff_queue->tail));

/* send all NAKs first, lack of data is blocking contiguous processing and its 
 * better to get the notification out a.s.a.p. even though it might be waiting
 * in a kernel queue.
 *
 * alternative: after each packet check for incoming data and return to the
 * event loop.  bias for shorter loops as retry count increases.
 */
	list = rxw->backoff_queue->tail;
	if (!list) {
		g_warning ("backoff queue is empty in nak_rb_state.");
		return;
	}

	guint dropped_invalid = 0;

/* have not learned this peers NLA */
	const gboolean is_valid_nla = (((struct sockaddr_in*)&peer->nla)->sin_addr.s_addr != INADDR_ANY);

/* TODO: process BOTH selective and parity NAKs? */

/* calculate current transmission group for parity enabled peers */
	if (peer->use_ondemand_parity)
	{
		const guint32 tg_sqn_mask = 0xffffffff << peer->tg_sqn_shift;

/* NAKs only generated previous to current transmission group */
		const guint32 current_tg_sqn = ((pgm_rxw_t*)peer->rxw)->lead & tg_sqn_mask;

		guint32 nak_tg_sqn = 0;
		guint32 nak_pkt_cnt = 0;

/* parity NAK generation */

		while (list)
		{
			GList* next_list_el = list->prev;
			pgm_rxw_packet_t* rp = (pgm_rxw_packet_t*)list->data;

/* check this packet for state expiration */
			if (pgm_time_after_eq(pgm_time_now, rp->nak_rb_expiry))
			{
				if (!is_valid_nla)
				{
					dropped_invalid++;
					g_trace ("INFO", "lost data #%u due to no peer NLA.", rp->sequence_number);
					pgm_rxw_mark_lost (rxw, rp->sequence_number);

/* mark receiver window for flushing on next recv() */
					if (!rxw->waiting_link.data)
					{
						rxw->waiting_link.data = rxw;
						rxw->waiting_link.next = transport->peers_waiting;
						transport->peers_waiting = &rxw->waiting_link;
					}

					list = next_list_el;
					continue;
				}

/* TODO: parity nak lists */
				const guint32 tg_sqn = rp->sequence_number & tg_sqn_mask;
				if (	( nak_pkt_cnt && tg_sqn == nak_tg_sqn ) ||
					( !nak_pkt_cnt && tg_sqn != current_tg_sqn )	)
				{
/* remove from this state */
					pgm_rxw_pkt_state_unlink (rxw, rp);

					if (!nak_pkt_cnt++)
						nak_tg_sqn = tg_sqn;
					rp->nak_transmit_count++;

					rp->state = PGM_PKT_WAIT_NCF_STATE;
					g_queue_push_head_link (rxw->wait_ncf_queue, &rp->link_);

#ifdef PGM_ABSOLUTE_EXPIRY
					rp->nak_rpt_expiry = rp->nak_rb_expiry + transport->nak_rpt_ivl;
					while (pgm_time_after_eq(pgm_time_now, rp->nak_rpt_expiry){
						rp->nak_rpt_expiry += transport->nak_rpt_ivl;
						rp->ncf_retry_count++;
					}
#else
					rp->nak_rpt_expiry = pgm_time_now + transport->nak_rpt_ivl;
#endif
				}
				else
				{	/* different transmission group */
					break;
				}
			}
			else
			{	/* packet expires some time later */
				break;
			}

			list = next_list_el;
		}

		if (nak_pkt_cnt)
		{
			send_parity_nak (peer, nak_tg_sqn, nak_pkt_cnt);
		}
	}
	else
	{

/* select NAK generation */

		while (list)
		{
			GList* next_list_el = list->prev;
			pgm_rxw_packet_t* rp = (pgm_rxw_packet_t*)list->data;

/* check this packet for state expiration */
			if (pgm_time_after_eq(pgm_time_now, rp->nak_rb_expiry))
			{
				if (!is_valid_nla) {
					dropped_invalid++;
					g_trace ("INFO", "lost data #%u due to no peer NLA.", rp->sequence_number);
					pgm_rxw_mark_lost (rxw, rp->sequence_number);

/* mark receiver window for flushing on next recv() */
					if (!rxw->waiting_link.data)
					{
						rxw->waiting_link.data = rxw;
						rxw->waiting_link.next = transport->peers_waiting;
						transport->peers_waiting = &rxw->waiting_link;
					}

					list = next_list_el;
					continue;
				}

/* remove from this state */
				pgm_rxw_pkt_state_unlink (rxw, rp);

#if PGM_SINGLE_NAK
				if (transport->can_send_nak)
					send_nak (transport, peer, rp->sequence_number);
				pgm_time_update_now();
#else
				nak_list.sqn[nak_list.len++] = rp->sequence_number;
#endif

				rp->nak_transmit_count++;

				rp->state = PGM_PKT_WAIT_NCF_STATE;
				g_queue_push_head_link (rxw->wait_ncf_queue, &rp->link_);

/* we have two options here, calculate the expiry time in the new state relative to the current
 * state execution time, skipping missed expirations due to delay in state processing, or base
 * from the actual current time.
 */
#ifdef PGM_ABSOLUTE_EXPIRY
				rp->nak_rpt_expiry = rp->nak_rb_expiry + transport->nak_rpt_ivl;
				while (pgm_time_after_eq(pgm_time_now, rp->nak_rpt_expiry){
					rp->nak_rpt_expiry += transport->nak_rpt_ivl;
					rp->ncf_retry_count++;
				}
#else
				rp->nak_rpt_expiry = pgm_time_now + transport->nak_rpt_ivl;
g_trace("INFO", "rp->nak_rpt_expiry in %f seconds.",
		pgm_to_secsf( rp->nak_rpt_expiry - pgm_time_now ) );
#endif

#ifndef PGM_SINGLE_NAK
				if (nak_list.len == G_N_ELEMENTS(nak_list.sqn)) {
					if (transport->can_send_nak)
						send_nak_list (peer, &nak_list);
					pgm_time_update_now();
					nak_list.len = 0;
				}
#endif
			}
			else
			{	/* packet expires some time later */
				break;
			}

			list = next_list_el;
		}

#ifndef PGM_SINGLE_NAK
		if (transport->can_send_nak && nak_list.len)
		{
			if (nak_list.len > 1) {
				send_nak_list (peer, &nak_list);
			} else {
				g_assert (nak_list.len == 1);
				send_nak (peer, nak_list.sqn[0]);
			}
		}
#endif

	}

	if (dropped_invalid)
	{
		g_warning ("dropped %u messages due to invalid NLA.", dropped_invalid);

/* mark receiver window for flushing on next recv() */
		if (rxw->cumulative_losses != rxw->ack_cumulative_losses &&
		    !rxw->waiting_link.data)
		{
			transport->has_lost_data = TRUE;
			rxw->pgm_sock_err.lost_count = rxw->cumulative_losses - rxw->ack_cumulative_losses;
			rxw->ack_cumulative_losses = rxw->cumulative_losses;

			rxw->waiting_link.data = rxw;
			rxw->waiting_link.next = transport->peers_waiting;
			transport->peers_waiting = &rxw->waiting_link;
		}
	}

	if (rxw->backoff_queue->length == 0)
	{
		g_assert ((struct rxw_packet*)rxw->backoff_queue->head == NULL);
		g_assert ((struct rxw_packet*)rxw->backoff_queue->tail == NULL);
	}
	else
	{
		g_assert ((struct rxw_packet*)rxw->backoff_queue->head != NULL);
		g_assert ((struct rxw_packet*)rxw->backoff_queue->tail != NULL);
	}

	if (rxw->backoff_queue->tail)
		g_trace ("INFO", "next expiry set in %f seconds.", pgm_to_secsf((float)next_nak_rb_expiry(rxw) - (float)pgm_time_now));
	else
		g_trace ("INFO", "backoff queue empty.");
}

/* check this peer for NAK state timers, uses the tail of each queue for the nearest
 * timer execution.
 */

static void
check_peer_nak_state (
	pgm_transport_t*	transport
	)
{
	if (!transport->peers_list) {
		return;
	}

	GList* list = transport->peers_list;
	do {
		GList* next = list->next;
		pgm_peer_t* peer = list->data;
		pgm_rxw_t* rxw = (pgm_rxw_t*)peer->rxw;

		g_static_mutex_lock (&peer->mutex);

		if (peer->spmr_expiry)
		{
			if (pgm_time_after_eq (pgm_time_now, peer->spmr_expiry))
			{
				if (transport->can_send_nak)
					send_spmr (peer);
				else
					peer->spmr_expiry = 0;
			}
		}

		if (rxw->backoff_queue->tail)
		{
			if (pgm_time_after_eq (pgm_time_now, next_nak_rb_expiry(rxw)))
			{
				nak_rb_state (peer);
			}
		}
		
		if (rxw->wait_ncf_queue->tail)
		{
			if (pgm_time_after_eq (pgm_time_now, next_nak_rpt_expiry(rxw)))
			{
				nak_rpt_state (peer);
			}
		}

		if (rxw->wait_data_queue->tail)
		{
			if (pgm_time_after_eq (pgm_time_now, next_nak_rdata_expiry(rxw)))
			{
				nak_rdata_state (peer);
			}
		}

/* expired, remove from hash table and linked list */
		if (pgm_time_after_eq (pgm_time_now, peer->expiry))
		{
			if (((pgm_rxw_t*)peer->rxw)->committed_count)
			{
				g_trace ("INFO", "peer expiration postponed due to committed data, tsi %s", pgm_print_tsi (&peer->tsi));
				peer->expiry += transport->peer_expiry;
				g_static_mutex_unlock (&peer->mutex);
			}
			else
			{
				g_warning ("peer expired, tsi %s", pgm_print_tsi (&peer->tsi));
				g_hash_table_remove (transport->peers_hashtable, &peer->tsi);
				transport->peers_list = g_list_remove_link (transport->peers_list, &peer->link_);
				g_static_mutex_unlock (&peer->mutex);
				pgm_peer_unref (peer);
			}
		}
		else
		{
			g_static_mutex_unlock (&peer->mutex);
		}

		list = next;
	} while (list);

/* check for waiting contiguous packets */
	if (transport->peers_waiting && !transport->is_waiting_read)
	{
		g_trace ("INFO","prod rx thread");
		if (!pgm_notify_send (&transport->waiting_notify)) {
			g_critical ("send to waiting notify channel failed :(");
		}
		transport->is_waiting_read = TRUE;
	}
}

/* find the next state expiration time among the transports peers.
 *
 * on success, returns the earliest of the expiration parameter or next
 * peer expiration time.
 */
static pgm_time_t
min_nak_expiry (
	pgm_time_t		expiration,
	pgm_transport_t*	transport
	)
{
	if (!transport->peers_list) {
		goto out;
	}

	GList* list = transport->peers_list;
	do {
		GList* next = list->next;
		pgm_peer_t* peer = (pgm_peer_t*)list->data;
		pgm_rxw_t* rxw = (pgm_rxw_t*)peer->rxw;
	
		g_static_mutex_lock (&peer->mutex);

		if (peer->spmr_expiry)
		{
			if (pgm_time_after_eq (expiration, peer->spmr_expiry))
			{
				expiration = peer->spmr_expiry;
			}
		}

		if (rxw->backoff_queue->tail)
		{
			if (pgm_time_after_eq (expiration, next_nak_rb_expiry(rxw)))
			{
				expiration = next_nak_rb_expiry(rxw);
			}
		}

		if (rxw->wait_ncf_queue->tail)
		{
			if (pgm_time_after_eq (expiration, next_nak_rpt_expiry(rxw)))
			{
				expiration = next_nak_rpt_expiry(rxw);
			}
		}

		if (rxw->wait_data_queue->tail)
		{
			if (pgm_time_after_eq (expiration, next_nak_rdata_expiry(rxw)))
			{
				expiration = next_nak_rdata_expiry(rxw);
			}
		}
	
		g_static_mutex_unlock (&peer->mutex);

		list = next;
	} while (list);

out:
	return expiration;
}

/* check WAIT_NCF_STATE, on expiration move back to BACK-OFF_STATE, on exceeding NAK_NCF_RETRIES
 * cancel the sequence number.
 */
static void
nak_rpt_state (
	pgm_peer_t*		peer
	)
{
	pgm_rxw_t* rxw = (pgm_rxw_t*)peer->rxw;
	pgm_transport_t* transport = peer->transport;
	GList* list = rxw->wait_ncf_queue->tail;

	g_trace ("INFO", "nak_rpt_state(len=%u)", g_list_length(rxw->wait_ncf_queue->tail));

	guint dropped_invalid = 0;
	guint dropped = 0;

/* have not learned this peers NLA */
	const gboolean is_valid_nla = (((struct sockaddr_in*)&peer->nla)->sin_addr.s_addr != INADDR_ANY);

	while (list)
	{
		GList* next_list_el = list->prev;
		pgm_rxw_packet_t* rp = (pgm_rxw_packet_t*)list->data;

/* check this packet for state expiration */
		if (pgm_time_after_eq(pgm_time_now, rp->nak_rpt_expiry))
		{
			if (!is_valid_nla) {
				dropped_invalid++;
				g_trace ("INFO", "lost data #%u due to no peer NLA.", rp->sequence_number);
				pgm_rxw_mark_lost (rxw, rp->sequence_number);

/* mark receiver window for flushing on next recv() */
				if (!rxw->waiting_link.data)
				{
					rxw->waiting_link.data = rxw;
					rxw->waiting_link.next = transport->peers_waiting;
					transport->peers_waiting = &rxw->waiting_link;
				}

				list = next_list_el;
				continue;
			}

			if (++rp->ncf_retry_count >= transport->nak_ncf_retries)
			{
/* cancellation */
				dropped++;
				g_trace ("INFO", "lost data #%u due to cancellation.", rp->sequence_number);

				const guint32 fail_time = pgm_time_now - rp->t0;
				if (!peer->max_fail_time) {
					peer->max_fail_time = peer->min_fail_time = fail_time;
				}
				else
				{
					if (fail_time > peer->max_fail_time)
						peer->max_fail_time = fail_time;
					else if (fail_time < peer->min_fail_time)
						peer->min_fail_time = fail_time;
				}

				pgm_rxw_mark_lost (rxw, rp->sequence_number);

/* mark receiver window for flushing on next recv() */
				if (!rxw->waiting_link.data)
				{
					rxw->waiting_link.data = rxw;
					rxw->waiting_link.next = transport->peers_waiting;
					transport->peers_waiting = &rxw->waiting_link;
				}

				peer->cumulative_stats[PGM_PC_RECEIVER_NAKS_FAILED_NCF_RETRIES_EXCEEDED]++;
			}
			else
			{
/* retry */
				g_trace("INFO", "retry #%u attempt %u/%u.", rp->sequence_number, rp->ncf_retry_count, transport->nak_ncf_retries);
				pgm_rxw_pkt_state_unlink (rxw, rp);
				rp->state = PGM_PKT_BACK_OFF_STATE;
				g_queue_push_head_link (rxw->backoff_queue, &rp->link_);
//				rp->nak_rb_expiry = rp->nak_rpt_expiry + nak_rb_ivl(transport);
				rp->nak_rb_expiry = pgm_time_now + nak_rb_ivl(transport);
			}
		}
		else
		{
/* packet expires some time later */
			g_trace("INFO", "#%u retry is delayed %f seconds.", rp->sequence_number, pgm_to_secsf(rp->nak_rpt_expiry - pgm_time_now));
			break;
		}
		

		list = next_list_el;
	}

	if (rxw->wait_ncf_queue->length == 0)
	{
		g_assert ((pgm_rxw_packet_t*)rxw->wait_ncf_queue->head == NULL);
		g_assert ((pgm_rxw_packet_t*)rxw->wait_ncf_queue->tail == NULL);
	}
	else
	{
		g_assert ((pgm_rxw_packet_t*)rxw->wait_ncf_queue->head);
		g_assert ((pgm_rxw_packet_t*)rxw->wait_ncf_queue->tail);
	}

	if (dropped_invalid) {
		g_warning ("dropped %u messages due to invalid NLA.", dropped_invalid);
	}

	if (dropped) {
		g_trace ("INFO", "dropped %u messages due to ncf cancellation, "
				"rxw_sqns %" G_GUINT32_FORMAT
				" bo %" G_GUINT32_FORMAT
				" ncf %" G_GUINT32_FORMAT
				" wd %" G_GUINT32_FORMAT
				" lost %" G_GUINT32_FORMAT
				" frag %" G_GUINT32_FORMAT,
				dropped,
				pgm_rxw_sqns(rxw),
				rxw->backoff_queue->length,
				rxw->wait_ncf_queue->length,
				rxw->wait_data_queue->length,
				rxw->lost_count,
				rxw->fragment_count);
	}

/* mark receiver window for flushing on next recv() */
	if (rxw->cumulative_losses != rxw->ack_cumulative_losses &&
	    !rxw->waiting_link.data)
	{
		transport->has_lost_data = TRUE;
		rxw->pgm_sock_err.lost_count = rxw->cumulative_losses - rxw->ack_cumulative_losses;
		rxw->ack_cumulative_losses = rxw->cumulative_losses;

		rxw->waiting_link.data = rxw;
		rxw->waiting_link.next = transport->peers_waiting;
		transport->peers_waiting = &rxw->waiting_link;
	}

	if (rxw->wait_ncf_queue->tail)
	{
		if (next_nak_rpt_expiry(rxw) > pgm_time_now)
		{
			g_trace ("INFO", "next expiry set in %f seconds.", pgm_to_secsf(next_nak_rpt_expiry(rxw) - pgm_time_now));
		} else {
			g_trace ("INFO", "next expiry set in -%f seconds.", pgm_to_secsf(pgm_time_now - next_nak_rpt_expiry(rxw)));
		}
	}
	else
	{
		g_trace ("INFO", "wait ncf queue empty.");
	}
}

/* check WAIT_DATA_STATE, on expiration move back to BACK-OFF_STATE, on exceeding NAK_DATA_RETRIES
 * canel the sequence number.
 */
static void
nak_rdata_state (
	pgm_peer_t*		peer
	)
{
	pgm_rxw_t* rxw = (pgm_rxw_t*)peer->rxw;
	pgm_transport_t* transport = peer->transport;
	GList* list = rxw->wait_data_queue->tail;

	g_trace ("INFO", "nak_rdata_state(len=%u)", g_list_length(rxw->wait_data_queue->tail));

	guint dropped_invalid = 0;
	guint dropped = 0;

/* have not learned this peers NLA */
	const gboolean is_valid_nla = (((struct sockaddr_in*)&peer->nla)->sin_addr.s_addr != INADDR_ANY);

	while (list)
	{
		GList* next_list_el = list->prev;
		pgm_rxw_packet_t* rp = (pgm_rxw_packet_t*)list->data;

/* check this packet for state expiration */
		if (pgm_time_after_eq(pgm_time_now, rp->nak_rdata_expiry))
		{
			if (!is_valid_nla) {
				dropped_invalid++;
				g_trace ("INFO", "lost data #%u due to no peer NLA.", rp->sequence_number);
				pgm_rxw_mark_lost (rxw, rp->sequence_number);

/* mark receiver window for flushing on next recv() */
				if (!rxw->waiting_link.data)
				{
					rxw->waiting_link.data = rxw;
					rxw->waiting_link.next = transport->peers_waiting;
					transport->peers_waiting = &rxw->waiting_link;
				}

				list = next_list_el;
				continue;
			}

			if (++rp->data_retry_count >= transport->nak_data_retries)
			{
/* cancellation */
				dropped++;
				g_trace ("INFO", "lost data #%u due to cancellation.", rp->sequence_number);

				const guint32 fail_time = pgm_time_now - rp->t0;
				if (fail_time > peer->max_fail_time)		peer->max_fail_time = fail_time;
				else if (fail_time < peer->min_fail_time)	peer->min_fail_time = fail_time;

				pgm_rxw_mark_lost (rxw, rp->sequence_number);

/* mark receiver window for flushing on next recv() */
				if (!rxw->waiting_link.data)
				{
					rxw->waiting_link.data = rxw;
					rxw->waiting_link.next = transport->peers_waiting;
					transport->peers_waiting = &rxw->waiting_link;
				}

				peer->cumulative_stats[PGM_PC_RECEIVER_NAKS_FAILED_DATA_RETRIES_EXCEEDED]++;

				list = next_list_el;
				continue;
			}

/* remove from this state */
			pgm_rxw_pkt_state_unlink (rxw, rp);

/* retry back to back-off state */
			g_trace("INFO", "retry #%u attempt %u/%u.", rp->sequence_number, rp->data_retry_count, transport->nak_data_retries);
			rp->state = PGM_PKT_BACK_OFF_STATE;
			g_queue_push_head_link (rxw->backoff_queue, &rp->link_);
//			rp->nak_rb_expiry = rp->nak_rdata_expiry + nak_rb_ivl(transport);
			rp->nak_rb_expiry = pgm_time_now + nak_rb_ivl(transport);
		}
		else
		{	/* packet expires some time later */
			break;
		}
		

		list = next_list_el;
	}

	if (rxw->wait_data_queue->length == 0)
	{
		g_assert ((pgm_rxw_packet_t*)rxw->wait_data_queue->head == NULL);
		g_assert ((pgm_rxw_packet_t*)rxw->wait_data_queue->tail == NULL);
	}
	else
	{
		g_assert ((pgm_rxw_packet_t*)rxw->wait_data_queue->head);
		g_assert ((pgm_rxw_packet_t*)rxw->wait_data_queue->tail);
	}

	if (dropped_invalid) {
		g_warning ("dropped %u messages due to invalid NLA.", dropped_invalid);
	}

	if (dropped) {
		g_trace ("INFO", "dropped %u messages due to data cancellation.", dropped);
	}

/* mark receiver window for flushing on next recv() */
	if (rxw->cumulative_losses != rxw->ack_cumulative_losses &&
	    !rxw->waiting_link.data)
	{
		transport->has_lost_data = TRUE;
		rxw->pgm_sock_err.lost_count = rxw->cumulative_losses - rxw->ack_cumulative_losses;
		rxw->ack_cumulative_losses = rxw->cumulative_losses;

		rxw->waiting_link.data = rxw;
		rxw->waiting_link.next = transport->peers_waiting;
		transport->peers_waiting = &rxw->waiting_link;
	}

	if (rxw->wait_data_queue->tail)
		g_trace ("INFO", "next expiry set in %f seconds.", pgm_to_secsf(next_nak_rdata_expiry(rxw) - pgm_time_now));
	else
		g_trace ("INFO", "wait data queue empty.");
}

/* cancel any pending heartbeat SPM and schedule a new one
 *
 * on success, 0 is returned.  on error, -1 is returned, and errno set
 * appropriately.
 */
static int
pgm_reset_heartbeat_spm (pgm_transport_t* transport)
{
	int retval = 0;

	g_static_mutex_lock (&transport->mutex);

/* re-set spm timer */
	transport->spm_heartbeat_state = 1;
	transport->next_heartbeat_spm = pgm_time_update_now() + transport->spm_heartbeat_interval[transport->spm_heartbeat_state++];

/* prod timer thread if sleeping */
	if (pgm_time_after( transport->next_poll, transport->next_heartbeat_spm ))
	{
		transport->next_poll = transport->next_heartbeat_spm;
		g_trace ("INFO","pgm_reset_heartbeat_spm: prod timer thread");
		if (!pgm_notify_send (&transport->timer_notify)) {
			g_critical ("send to timer notify channel failed :(");
			retval = -EINVAL;
		}
	}

	g_static_mutex_unlock (&transport->mutex);

	return retval;
}

/* state helper for resuming sends
 */
#define STATE(x)	(transport->pkt_dontwait_state.x)

/* send one PGM data packet, transmit window owned memory.
 *
 * on success, returns number of data bytes pushed into the transmit window and
 * attempted to send to the socket layer.  on non-blocking sockets, -1 is returned
 * if the packet sizes would exceed the current rate limit. on invalid arguments,
 * -EINVAL is returned.
 *
 * ! always returns successful if data is pushed into the transmit window, even if
 * sendto() double fails ¡  we don't want the application to try again as that is the
 * reliable transports role.
 */
static gssize
pgm_transport_send_one (
	pgm_transport_t*	transport,
	gpointer		tsdu,
	gsize			tsdu_length,
	int			flags
	)
{
	g_return_val_if_fail (transport != NULL, -EINVAL);
	g_return_val_if_fail (tsdu != NULL, -EINVAL);
	g_return_val_if_fail (tsdu_length <= transport->max_tsdu, -EMSGSIZE);

	g_assert( !(flags & MSG_WAITALL && !(flags & MSG_DONTWAIT)) );

/* continue if send would block */
	if (transport->is_apdu_eagain) {
		goto retry_send;
	}

/* retrieve packet storage from transmit window
 */
	STATE(tpdu_length) = pgm_transport_pkt_offset (FALSE) + tsdu_length;
	STATE(pkt) = (guint8*)tsdu - pgm_transport_pkt_offset (FALSE);

	struct pgm_header *header = (struct pgm_header*)STATE(pkt);
	struct pgm_data *odata = (struct pgm_data*)(header + 1);
	memcpy (header->pgm_gsi, &transport->tsi.gsi, sizeof(pgm_gsi_t));
	header->pgm_sport	= transport->tsi.sport;
	header->pgm_dport	= transport->dport;
	header->pgm_type        = PGM_ODATA;
        header->pgm_options     = 0;
        header->pgm_tsdu_length = g_htons (tsdu_length);

	g_static_rw_lock_writer_lock (&transport->txw_lock);

/* ODATA */
        odata->data_sqn         = g_htonl (pgm_txw_next_lead(transport->txw));
        odata->data_trail       = g_htonl (pgm_txw_trail(transport->txw));

        header->pgm_checksum    = 0;
	gsize pgm_header_len	= (guint8*)(odata + 1) - (guint8*)header;
	guint32 unfolded_header = pgm_csum_partial (header, pgm_header_len, 0);
	STATE(unfolded_odata)	= pgm_csum_partial ((guint8*)(odata + 1), tsdu_length, 0);
        header->pgm_checksum	= pgm_csum_fold (pgm_csum_block_add (unfolded_header, STATE(unfolded_odata), pgm_header_len));

/* add to transmit window */
	pgm_txw_push (transport->txw, STATE(pkt), STATE(tpdu_length));

	gssize sent;
retry_send:
	sent = pgm_sendto (transport,
				TRUE,			/* rate limited */
				FALSE,			/* regular socket */
				STATE(pkt),
				STATE(tpdu_length),
				(flags & MSG_DONTWAIT && flags & MSG_WAITALL) ?
					flags & ~(MSG_DONTWAIT | MSG_WAITALL) :
					flags,
				(struct sockaddr*)&transport->send_gsr.gsr_group,
				pgm_sockaddr_len(&transport->send_gsr.gsr_group));
	if (sent < 0 && errno == EAGAIN)
	{
		transport->is_apdu_eagain = TRUE;
		return -1;
	}

/* save unfolded odata for retransmissions */
	*(guint32*)(void*)&((struct pgm_header*)STATE(pkt))->pgm_sport = STATE(unfolded_odata);

/* release txw lock here in order to allow spms to lock mutex */
	g_static_rw_lock_writer_unlock (&transport->txw_lock);

	transport->is_apdu_eagain = FALSE;
	pgm_reset_heartbeat_spm (transport);

	if ( sent == (gssize)STATE(tpdu_length) )
	{
		transport->cumulative_stats[PGM_PC_SOURCE_DATA_BYTES_SENT] += tsdu_length;
		transport->cumulative_stats[PGM_PC_SOURCE_DATA_MSGS_SENT]  ++;
		transport->cumulative_stats[PGM_PC_SOURCE_BYTES_SENT]	   += STATE(tpdu_length) + transport->iphdr_len;
	}

/* check for end of transmission group */
	if (transport->use_proactive_parity)
	{
		const guint32 odata_sqn = ((struct pgm_data*)(((struct pgm_header*)STATE(pkt)) + 1))->data_sqn;
		const guint32 tg_sqn_mask = 0xffffffff << transport->tg_sqn_shift;
		if (!((g_ntohl(odata_sqn) + 1) & ~tg_sqn_mask))
		{
			pgm_schedule_proactive_nak (transport, g_ntohl(odata_sqn) & tg_sqn_mask);
		}
	}

	return (gssize)tsdu_length;
}

/* send one PGM original data packet, callee owned memory.
 *
 * on success, returns number of data bytes pushed into the transmit window and
 * attempted to send to the socket layer.  on non-blocking sockets, -1 is returned
 * if the packet sizes would exceed the current rate limit.
 */
static gssize
pgm_transport_send_one_copy (
	pgm_transport_t*	transport,
	gconstpointer		tsdu,
	gsize			tsdu_length,
	int			flags
	)
{
	g_return_val_if_fail (transport != NULL, -EINVAL);
	if (tsdu_length) {
		g_return_val_if_fail (tsdu != NULL, -EINVAL);
		g_return_val_if_fail (tsdu_length <= transport->max_tsdu, -EMSGSIZE);
	}

	g_assert( !(flags & MSG_WAITALL && !(flags & MSG_DONTWAIT)) );

/* continue if blocked mid-apdu */
	if (transport->is_apdu_eagain) {
		goto retry_send;
	}

	g_static_rw_lock_writer_lock (&transport->txw_lock);
	STATE(pkt) = pgm_txw_alloc (transport->txw);

/* retrieve packet storage from transmit window */
	STATE(tpdu_length) = pgm_transport_pkt_offset (FALSE) + tsdu_length;

	struct pgm_header* header = (struct pgm_header*)STATE(pkt);
	struct pgm_data *odata = (struct pgm_data*)(header + 1);
	memcpy (header->pgm_gsi, &transport->tsi.gsi, sizeof(pgm_gsi_t));
	header->pgm_sport	= transport->tsi.sport;
	header->pgm_dport	= transport->dport;
	header->pgm_type        = PGM_ODATA;
        header->pgm_options     = 0;
        header->pgm_tsdu_length = g_htons (tsdu_length);

/* ODATA */
        odata->data_sqn         = g_htonl (pgm_txw_next_lead(transport->txw));
        odata->data_trail       = g_htonl (pgm_txw_trail(transport->txw));

        header->pgm_checksum    = 0;

	gsize pgm_header_len	= (guint8*)(odata + 1) - (guint8*)header;
	guint32 unfolded_header	= pgm_csum_partial (header, pgm_header_len, 0);
	STATE(unfolded_odata)	= pgm_csum_partial_copy (tsdu, odata + 1, tsdu_length, 0);
	header->pgm_checksum	= pgm_csum_fold (pgm_csum_block_add (unfolded_header, STATE(unfolded_odata), pgm_header_len));

/* add to transmit window */
	pgm_txw_push (transport->txw, STATE(pkt), STATE(tpdu_length));

	gssize sent;
retry_send:
	sent = pgm_sendto (transport,
				TRUE,			/* rate limited */
				FALSE,			/* regular socket */
				STATE(pkt),
				STATE(tpdu_length),
				(flags & MSG_DONTWAIT && flags & MSG_WAITALL) ?
					flags & ~(MSG_DONTWAIT | MSG_WAITALL) :
					flags,
				(struct sockaddr*)&transport->send_gsr.gsr_group,
				pgm_sockaddr_len(&transport->send_gsr.gsr_group));
	if (sent < 0 && errno == EAGAIN)
	{
		transport->is_apdu_eagain = TRUE;
		return -1;
	}

/* save unfolded odata for retransmissions */
	*(guint32*)(void*)&((struct pgm_header*)STATE(pkt))->pgm_sport = STATE(unfolded_odata);

/* release txw lock here in order to allow spms to lock mutex */
	g_static_rw_lock_writer_unlock (&transport->txw_lock);

	transport->is_apdu_eagain = FALSE;
	pgm_reset_heartbeat_spm (transport);

	if ( sent == (gssize)STATE(tpdu_length) )
	{
		transport->cumulative_stats[PGM_PC_SOURCE_DATA_BYTES_SENT] += tsdu_length;
		transport->cumulative_stats[PGM_PC_SOURCE_DATA_MSGS_SENT]  ++;
		transport->cumulative_stats[PGM_PC_SOURCE_BYTES_SENT]	   += STATE(tpdu_length) + transport->iphdr_len;
	}

/* check for end of transmission group */
	if (transport->use_proactive_parity)
	{
		guint32 odata_sqn = ((struct pgm_data*)(((struct pgm_header*)STATE(pkt)) + 1))->data_sqn;
		guint32 tg_sqn_mask = 0xffffffff << transport->tg_sqn_shift;
		if (!((g_ntohl(odata_sqn) + 1) & ~tg_sqn_mask))
		{
			pgm_schedule_proactive_nak (transport, g_ntohl(odata_sqn) & tg_sqn_mask);
		}
	}

/* return data payload length sent */
	return (gssize)tsdu_length;
}

/* send one PGM original data packet, callee owned scatter/gather io vector
 *
 *    ⎢ DATA₀ ⎢
 *    ⎢ DATA₁ ⎢ → pgm_transport_send_onev() →  ⎢ TSDU₀ ⎢ → libc
 *    ⎢   ⋮   ⎢
 *
 * on success, returns number of data bytes pushed into the transmit window and
 * attempted to send to the socket layer.  on non-blocking sockets, -1 is returned
 * if the packet sizes would exceed the current rate limit.
 */
static gssize
pgm_transport_send_onev (
	pgm_transport_t*	transport,
	const struct iovec*	vector,
	guint			count,		/* number of items in vector */
	int			flags
	)
{
	g_return_val_if_fail (transport != NULL, -EINVAL);
	if (count) {
		g_return_val_if_fail (vector != NULL, -EINVAL);
	} else {
/* pass on zero length call so we don't have to check count on first iteration. */
		return pgm_transport_send_one_copy (transport, NULL, 0, flags);
	}
	g_return_val_if_fail (transport != NULL, -EINVAL);

	g_assert( !(flags & MSG_WAITALL && !(flags & MSG_DONTWAIT)) );

/* continue if blocked on send */
	if (transport->is_apdu_eagain) {
		goto retry_send;
	}

	STATE(tsdu_length) = 0;
	for (unsigned i = 0; i < count; i++)
	{
#ifdef TRANSPORT_DEBUG
		if (vector[i].iov_len)
		{
			g_assert( vector[i].iov_base );
		}
#endif
		STATE(tsdu_length) += vector[i].iov_len;
	}
	g_return_val_if_fail (STATE(tsdu_length) <= transport->max_tsdu, -EMSGSIZE);

	g_static_rw_lock_writer_lock (&transport->txw_lock);
	STATE(pkt) = pgm_txw_alloc (transport->txw);

/* retrieve packet storage from transmit window */
	STATE(tpdu_length) = pgm_transport_pkt_offset (FALSE) + STATE(tsdu_length);

	struct pgm_header *header = (struct pgm_header*)STATE(pkt);
	memcpy (header->pgm_gsi, &transport->tsi.gsi, sizeof(pgm_gsi_t));
	header->pgm_sport	= transport->tsi.sport;
	header->pgm_dport	= transport->dport;
	header->pgm_type        = PGM_ODATA;
        header->pgm_options     = 0;
        header->pgm_tsdu_length = g_htons (STATE(tsdu_length));

/* ODATA */
	struct pgm_data *odata = (struct pgm_data*)(header + 1);
        odata->data_sqn         = g_htonl (pgm_txw_next_lead(transport->txw));
        odata->data_trail       = g_htonl (pgm_txw_trail(transport->txw));

        header->pgm_checksum    = 0;
	gsize pgm_header_len	= (guint8*)(odata + 1) - (guint8*)header;
	guint32 unfolded_header	= pgm_csum_partial (header, pgm_header_len, 0);

/* unroll first iteration to make friendly branch prediction */
	guint8*	dst		= (guint8*)(odata + 1);
	STATE(unfolded_odata)	= pgm_csum_partial_copy ((const guint8*)vector[0].iov_base, dst, vector[0].iov_len, 0);

/* iterate over one or more vector elements to perform scatter/gather checksum & copy */
	for (unsigned i = 1; i < count; i++)
	{
		dst += vector[i-1].iov_len;
		guint32 unfolded_element = pgm_csum_partial_copy ((const guint8*)vector[i].iov_base, dst, vector[i].iov_len, 0);
		STATE(unfolded_odata) = pgm_csum_block_add (STATE(unfolded_odata), unfolded_element, vector[i-1].iov_len);
	}

	header->pgm_checksum	= pgm_csum_fold (pgm_csum_block_add (unfolded_header, STATE(unfolded_odata), pgm_header_len));

/* add to transmit window */
	pgm_txw_push (transport->txw, STATE(pkt), STATE(tpdu_length));

	gssize sent;
retry_send:
	sent = pgm_sendto (transport,
				TRUE,			/* rate limited */
				FALSE,			/* regular socket */
				STATE(pkt),
				STATE(tpdu_length),
				(flags & MSG_DONTWAIT && flags & MSG_WAITALL) ?
					flags & ~(MSG_DONTWAIT | MSG_WAITALL) :
					flags,
				(struct sockaddr*)&transport->send_gsr.gsr_group,
				pgm_sockaddr_len(&transport->send_gsr.gsr_group));
	if (sent < 0 && errno == EAGAIN)
	{
		transport->is_apdu_eagain = TRUE;
		return -1;
	}

/* save unfolded odata for retransmissions */
	*(guint32*)(void*)&((struct pgm_header*)STATE(pkt))->pgm_sport = STATE(unfolded_odata);

/* release txw lock here in order to allow spms to lock mutex */
	g_static_rw_lock_writer_unlock (&transport->txw_lock);

	transport->is_apdu_eagain = FALSE;
	pgm_reset_heartbeat_spm (transport);

	if ( sent == (gssize)STATE(tpdu_length) )
	{
		transport->cumulative_stats[PGM_PC_SOURCE_DATA_BYTES_SENT] += STATE(tsdu_length);
		transport->cumulative_stats[PGM_PC_SOURCE_DATA_MSGS_SENT]  ++;
		transport->cumulative_stats[PGM_PC_SOURCE_BYTES_SENT]	   += STATE(tpdu_length) + transport->iphdr_len;
	}

/* check for end of transmission group */
	if (transport->use_proactive_parity)
	{
		guint32 odata_sqn = ((struct pgm_data*)(((struct pgm_header*)STATE(pkt)) + 1))->data_sqn;
		guint32 tg_sqn_mask = 0xffffffff << transport->tg_sqn_shift;
		if (!((g_ntohl(odata_sqn) + 1) & ~tg_sqn_mask))
		{
			pgm_schedule_proactive_nak (transport, g_ntohl(odata_sqn) & tg_sqn_mask);
		}
	}

/* return data payload length sent */
	return (gssize)STATE(tsdu_length);
}

/* send PGM original data, callee owned memory.  if larger than maximum TPDU
 * size will be fragmented.
 *
 * on success, returns number of data bytes pushed into the transmit window and
 * attempted to send to the socket layer.  on non-blocking sockets, -1 is returned
 * if the packet sizes would exceed the current rate limit.
 */
gssize
pgm_transport_send (
	pgm_transport_t*	transport,
	gconstpointer		apdu,
	gsize			apdu_length,
	int			flags		/* MSG_DONTWAIT = rate non-blocking,
						   MSG_WAITALL  = packet blocking   */
	)
{
	g_return_val_if_fail (transport != NULL, -EINVAL);

/* reject on closed transport */
	if (!transport->is_open) {
		errno = ECONNRESET;
		return -1;
	}

/* pass on non-fragment calls */
	if (apdu_length < transport->max_tsdu) {
		return pgm_transport_send_one_copy (transport, apdu, apdu_length, flags);
	}
	g_return_val_if_fail (apdu != NULL, -EINVAL);
	g_return_val_if_fail (apdu_length <= (transport->txw_sqns * pgm_transport_max_tsdu (transport, TRUE)), -EMSGSIZE);

	g_assert( !(flags & MSG_WAITALL && !(flags & MSG_DONTWAIT)) );

	gsize bytes_sent	= 0;		/* counted at IP layer */
	guint packets_sent	= 0;		/* IP packets */
	gsize data_bytes_sent	= 0;

/* continue if blocked mid-apdu */
	if (transport->is_apdu_eagain) {
		goto retry_send;
	}

/* if non-blocking calculate total wire size and check rate limit */
	STATE(is_rate_limited) = FALSE;
	if (flags & MSG_DONTWAIT && flags & MSG_WAITALL)
	{
		gsize header_length = pgm_transport_pkt_offset (TRUE);
		gsize tpdu_length = 0;
		gsize offset_	  = 0;
		do {
			gsize tsdu_length = MIN( pgm_transport_max_tsdu (transport, TRUE), apdu_length - offset_ );
			tpdu_length += transport->iphdr_len + header_length + tsdu_length;
			offset_ += tsdu_length;
		} while (offset_ < apdu_length);

/* calculation includes one iphdr length already */
		int result = pgm_rate_check (transport->rate_control, tpdu_length - transport->iphdr_len, flags);
		if (result == -1) {
			return (gssize)result;
		}

		STATE(is_rate_limited) = TRUE;
	}

	STATE(data_bytes_offset) = 0;

	g_static_rw_lock_writer_lock (&transport->txw_lock);
	STATE(first_sqn)	= pgm_txw_next_lead(transport->txw);

	do {
/* retrieve packet storage from transmit window */
		gsize header_length = pgm_transport_pkt_offset (TRUE);
		STATE(tsdu_length) = MIN( pgm_transport_max_tsdu (transport, TRUE), apdu_length - STATE(data_bytes_offset) );
		STATE(tpdu_length) = header_length + STATE(tsdu_length);

		STATE(pkt) = pgm_txw_alloc(transport->txw);
		struct pgm_header *header = (struct pgm_header*)STATE(pkt);
		memcpy (header->pgm_gsi, &transport->tsi.gsi, sizeof(pgm_gsi_t));
		header->pgm_sport	= transport->tsi.sport;
		header->pgm_dport	= transport->dport;
		header->pgm_type        = PGM_ODATA;
	        header->pgm_options     = PGM_OPT_PRESENT;
	        header->pgm_tsdu_length = g_htons (STATE(tsdu_length));

/* ODATA */
		struct pgm_data *odata = (struct pgm_data*)(header + 1);
	        odata->data_sqn         = g_htonl (pgm_txw_next_lead(transport->txw));
	        odata->data_trail       = g_htonl (pgm_txw_trail(transport->txw));

/* OPT_LENGTH */
		struct pgm_opt_length* opt_len = (struct pgm_opt_length*)(odata + 1);
		opt_len->opt_type	= PGM_OPT_LENGTH;
		opt_len->opt_length	= sizeof(struct pgm_opt_length);
		opt_len->opt_total_length	= g_htons (	sizeof(struct pgm_opt_length) +
								sizeof(struct pgm_opt_header) +
								sizeof(struct pgm_opt_fragment) );
/* OPT_FRAGMENT */
		struct pgm_opt_header* opt_header = (struct pgm_opt_header*)(opt_len + 1);
		opt_header->opt_type	= PGM_OPT_FRAGMENT | PGM_OPT_END;
		opt_header->opt_length	= sizeof(struct pgm_opt_header) +
						sizeof(struct pgm_opt_fragment);
		struct pgm_opt_fragment* opt_fragment = (struct pgm_opt_fragment*)(opt_header + 1);
		opt_fragment->opt_reserved	= 0;
		opt_fragment->opt_sqn		= g_htonl (STATE(first_sqn));
		opt_fragment->opt_frag_off	= g_htonl (STATE(data_bytes_offset));
		opt_fragment->opt_frag_len	= g_htonl (apdu_length);

/* TODO: the assembly checksum & copy routine is faster than memcpy & pgm_cksum on >= opteron hardware */
	        header->pgm_checksum    = 0;

		gsize pgm_header_len	= (guint8*)(opt_fragment + 1) - (guint8*)header;
		guint32 unfolded_header = pgm_csum_partial (header, pgm_header_len, 0);
		STATE(unfolded_odata)	= pgm_csum_partial_copy ((const guint8*)apdu + STATE(data_bytes_offset), opt_fragment + 1, STATE(tsdu_length), 0);
		header->pgm_checksum	= pgm_csum_fold (pgm_csum_block_add (unfolded_header, STATE(unfolded_odata), pgm_header_len));

/* add to transmit window */
		pgm_txw_push (transport->txw, STATE(pkt), STATE(tpdu_length));

		gssize sent;
retry_send:
		sent = pgm_sendto (transport,
					!STATE(is_rate_limited),	/* rate limit on blocking */
					FALSE,				/* regular socket */
					STATE(pkt),
					STATE(tpdu_length),
					(flags & MSG_DONTWAIT && flags & MSG_WAITALL) ?
						flags & ~(MSG_DONTWAIT | MSG_WAITALL) :
						flags,
					(struct sockaddr*)&transport->send_gsr.gsr_group,
					pgm_sockaddr_len(&transport->send_gsr.gsr_group));
		if (sent < 0 && errno == EAGAIN)
		{
			transport->is_apdu_eagain = TRUE;
			goto blocked;
		}

/* save unfolded odata for retransmissions */
		*(guint32*)(void*)&((struct pgm_header*)STATE(pkt))->pgm_sport = STATE(unfolded_odata);

		if (sent == (gssize)STATE(tpdu_length))
		{
			bytes_sent += STATE(tpdu_length) + transport->iphdr_len;	/* as counted at IP layer */
			packets_sent++;							/* IP packets */
			data_bytes_sent += STATE(tsdu_length);
		}

		STATE(data_bytes_offset) += STATE(tsdu_length);

/* check for end of transmission group */
		if (transport->use_proactive_parity)
		{
			guint32 odata_sqn = ((struct pgm_data*)(((struct pgm_header*)STATE(pkt)) + 1))->data_sqn;
			guint32 tg_sqn_mask = 0xffffffff << transport->tg_sqn_shift;
			if (!((g_ntohl(odata_sqn) + 1) & ~tg_sqn_mask))
			{
				pgm_schedule_proactive_nak (transport, g_ntohl(odata_sqn) & tg_sqn_mask);
			}
		}

	} while ( STATE(data_bytes_offset)  < apdu_length);
	g_assert( STATE(data_bytes_offset) == apdu_length );

/* release txw lock here in order to allow spms to lock mutex */
	g_static_rw_lock_writer_unlock (&transport->txw_lock);

	transport->is_apdu_eagain = FALSE;
	pgm_reset_heartbeat_spm (transport);

	transport->cumulative_stats[PGM_PC_SOURCE_BYTES_SENT]      += bytes_sent;
	transport->cumulative_stats[PGM_PC_SOURCE_DATA_MSGS_SENT]  += packets_sent;
	transport->cumulative_stats[PGM_PC_SOURCE_DATA_BYTES_SENT] += data_bytes_sent;

	return (gssize)apdu_length;

blocked:
	if (bytes_sent)
	{
		pgm_reset_heartbeat_spm (transport);

		transport->cumulative_stats[PGM_PC_SOURCE_BYTES_SENT]      += bytes_sent;
		transport->cumulative_stats[PGM_PC_SOURCE_DATA_MSGS_SENT]  += packets_sent;
		transport->cumulative_stats[PGM_PC_SOURCE_DATA_BYTES_SENT] += data_bytes_sent;
	}

	errno = EAGAIN;
	return -1;
}

/* send PGM original data, callee owned scatter/gather IO vector.  if larger than maximum TPDU
 * size will be fragmented.
 *
 * is_one_apdu = true:
 *
 *    ⎢ DATA₀ ⎢
 *    ⎢ DATA₁ ⎢ → pgm_transport_sendv() →  ⎢ ⋯ TSDU₁ TSDU₀ ⎢ → libc
 *    ⎢   ⋮   ⎢
 *
 * is_one_apdu = false:
 *
 *    ⎢ APDU₀ ⎢                            ⎢ ⋯ TSDU₁,₀ TSDU₀,₀ ⎢
 *    ⎢ APDU₁ ⎢ → pgm_transport_sendv() →  ⎢ ⋯ TSDU₁,₁ TSDU₀,₁ ⎢ → libc
 *    ⎢   ⋮   ⎢                            ⎢     ⋮       ⋮     ⎢
 *
 * on success, returns number of data bytes pushed into the transmit window and
 * attempted to send to the socket layer.  on non-blocking sockets, -1 is returned
 * if the packet sizes would exceed the current rate limit.
 */
gssize
pgm_transport_sendv (
	pgm_transport_t*	transport,
	const struct iovec*	vector,
	guint			count,		/* number of items in vector */
	int			flags,		/* MSG_DONTWAIT = rate non-blocking,
						   MSG_WAITALL  = packet blocking   */
	gboolean		is_one_apdu	/* true  = vector = apdu,
                                                   false = vector::iov_base = apdu */
	)
{
	g_return_val_if_fail (transport != NULL, -EINVAL);

/* reject on closed transport */
	if (!transport->is_open) {
		errno = ECONNRESET;
		return -1;
	}

/* pass on zero length as cannot count vector lengths */
	if (count == 0) {
		return pgm_transport_send_one_copy (transport, NULL, count, flags);
	}
	g_return_val_if_fail (vector != NULL, -EINVAL);

	g_assert( !(flags & MSG_WAITALL && !(flags & MSG_DONTWAIT)) );

	gsize bytes_sent	= 0;
	guint packets_sent	= 0;
	gsize data_bytes_sent	= 0;

/* continue if blocked mid-apdu */
	if (transport->is_apdu_eagain) {
		if (is_one_apdu) {
			if (STATE(apdu_length) < transport->max_tsdu) {
				return pgm_transport_send_onev (transport, vector, count, flags);
			} else {
				goto retry_one_apdu_send;
			}
		} else {
			goto retry_send;
		}
	}

/* calculate (total) APDU length */
	STATE(apdu_length)	= 0;
	for (unsigned i = 0; i < count; i++)
	{
#ifdef TRANSPORT_DEBUG
		if (vector[i].iov_len)
		{
			g_assert( vector[i].iov_base );
		}
#endif
		STATE(apdu_length) += vector[i].iov_len;
	}

/* pass on non-fragment calls */
	if (is_one_apdu && STATE(apdu_length) < transport->max_tsdu) {
		return pgm_transport_send_onev (transport, vector, count, flags);
	}
	g_return_val_if_fail (STATE(apdu_length) <= (transport->txw_sqns * pgm_transport_max_tsdu (transport, TRUE)), -EMSGSIZE);

/* if non-blocking calculate total wire size and check rate limit */
	STATE(is_rate_limited) = FALSE;
	if (flags & MSG_DONTWAIT && flags & MSG_WAITALL)
        {
		gsize header_length = pgm_transport_pkt_offset (TRUE);
                gsize tpdu_length = 0;
		guint offset_	  = 0;
		do {
			gsize tsdu_length = MIN( pgm_transport_max_tsdu (transport, TRUE), STATE(apdu_length) - offset_ );
			tpdu_length += transport->iphdr_len + header_length + tsdu_length;
			offset_     += tsdu_length;
		} while (offset_ < STATE(apdu_length));

/* calculation includes one iphdr length already */
                int result = pgm_rate_check (transport->rate_control, tpdu_length - transport->iphdr_len, flags);
                if (result == -1) {
			return (gssize)result;
                }
		STATE(is_rate_limited) = TRUE;
        }

/* non-fragmented packets can be forwarded onto basic send() */
	if (!is_one_apdu)
	{
		for (STATE(data_pkt_offset) = 0; STATE(data_pkt_offset) < count; STATE(data_pkt_offset)++)
		{
			gssize sent;
retry_send:
			sent = pgm_transport_send (transport, vector[STATE(data_pkt_offset)].iov_base, vector[STATE(data_pkt_offset)].iov_len, flags);
			if (sent < 0 && errno == EAGAIN)
			{
				transport->is_apdu_eagain = TRUE;
				return -1;
			}

			if (sent == (gssize)vector[STATE(data_pkt_offset)].iov_len)
			{
				data_bytes_sent += vector[STATE(data_pkt_offset)].iov_len;
			}
		}

		transport->is_apdu_eagain = FALSE;
		return (gssize)data_bytes_sent;
	}

	STATE(data_bytes_offset)	= 0;
	STATE(vector_index)		= 0;
	STATE(vector_offset)		= 0;

	g_static_rw_lock_writer_lock (&transport->txw_lock);
	STATE(first_sqn)		= pgm_txw_next_lead(transport->txw);

	do {
/* retrieve packet storage from transmit window */
		gsize header_length = pgm_transport_pkt_offset (TRUE);
		STATE(tsdu_length) = MIN( pgm_transport_max_tsdu (transport, TRUE), STATE(apdu_length) - STATE(data_bytes_offset) );
		STATE(tpdu_length) = header_length + STATE(tsdu_length);

		STATE(pkt) = pgm_txw_alloc(transport->txw);
		struct pgm_header *header = (struct pgm_header*)STATE(pkt);
		memcpy (header->pgm_gsi, &transport->tsi.gsi, sizeof(pgm_gsi_t));
		header->pgm_sport	= transport->tsi.sport;
		header->pgm_dport	= transport->dport;
		header->pgm_type        = PGM_ODATA;
	        header->pgm_options     = PGM_OPT_PRESENT;
	        header->pgm_tsdu_length = g_htons (STATE(tsdu_length));

/* ODATA */
		struct pgm_data *odata = (struct pgm_data*)(header + 1);
	        odata->data_sqn         = g_htonl (pgm_txw_next_lead(transport->txw));
	        odata->data_trail       = g_htonl (pgm_txw_trail(transport->txw));

/* OPT_LENGTH */
		struct pgm_opt_length* opt_len = (struct pgm_opt_length*)(odata + 1);
		opt_len->opt_type	= PGM_OPT_LENGTH;
		opt_len->opt_length	= sizeof(struct pgm_opt_length);
		opt_len->opt_total_length	= g_htons (	sizeof(struct pgm_opt_length) +
								sizeof(struct pgm_opt_header) +
								sizeof(struct pgm_opt_fragment) );
/* OPT_FRAGMENT */
		struct pgm_opt_header* opt_header = (struct pgm_opt_header*)(opt_len + 1);
		opt_header->opt_type	= PGM_OPT_FRAGMENT | PGM_OPT_END;
		opt_header->opt_length	= sizeof(struct pgm_opt_header) +
						sizeof(struct pgm_opt_fragment);
		struct pgm_opt_fragment* opt_fragment = (struct pgm_opt_fragment*)(opt_header + 1);
		opt_fragment->opt_reserved	= 0;
		opt_fragment->opt_sqn		= g_htonl (STATE(first_sqn));
		opt_fragment->opt_frag_off	= g_htonl (STATE(data_bytes_offset));
		opt_fragment->opt_frag_len	= g_htonl (STATE(apdu_length));

/* checksum & copy */
	        header->pgm_checksum    = 0;
		gsize pgm_header_len	= (guint8*)(opt_fragment + 1) - (guint8*)header;
		guint32 unfolded_header = pgm_csum_partial (header, pgm_header_len, 0);

/* iterate over one or more vector elements to perform scatter/gather checksum & copy
 *
 * STATE(vector_index)	- index into application scatter/gather vector
 * STATE(vector_offset) - current offset into current vector element
 * STATE(unfolded_odata)- checksum accumulator
 */
		const guint8* src	= (const guint8*)vector[STATE(vector_index)].iov_base + STATE(vector_offset);
		guint8* dst		= (guint8*)(opt_fragment + 1);
		gsize src_length	= vector[STATE(vector_index)].iov_len - STATE(vector_offset);
		gsize dst_length	= 0;
		gsize copy_length	= MIN( STATE(tsdu_length), src_length );
		STATE(unfolded_odata)	= pgm_csum_partial_copy (src, dst, copy_length, 0);

		for(;;)
		{
			if (copy_length == src_length)
			{
/* application packet complete */
				STATE(vector_index)++;
				STATE(vector_offset) = 0;
			}
			else
			{
/* data still remaining */
				STATE(vector_offset) += copy_length;
			}

			dst_length += copy_length;

			if (dst_length == STATE(tsdu_length))
			{
/* transport packet complete */
				break;
			}

			src		= (const guint8*)vector[STATE(vector_index)].iov_base + STATE(vector_offset);
			dst	       += copy_length;
			src_length	= vector[STATE(vector_index)].iov_len - STATE(vector_offset);
			copy_length	= MIN( STATE(tsdu_length) - dst_length, src_length );
			guint32 unfolded_element = pgm_csum_partial_copy (src, dst, copy_length, 0);
			STATE(unfolded_odata) = pgm_csum_block_add (STATE(unfolded_odata), unfolded_element, dst_length);
		}

		header->pgm_checksum	= pgm_csum_fold (pgm_csum_block_add (unfolded_header, STATE(unfolded_odata), pgm_header_len));

/* add to transmit window */
		pgm_txw_push (transport->txw, STATE(pkt), STATE(tpdu_length));

		gssize sent;
retry_one_apdu_send:
		sent = pgm_sendto (transport,
					!STATE(is_rate_limited),	/* rate limited on blocking */
					FALSE,				/* regular socket */
					STATE(pkt),
					STATE(tpdu_length),
					(flags & MSG_DONTWAIT && flags & MSG_WAITALL) ?
						flags & ~(MSG_DONTWAIT | MSG_WAITALL) :
						flags,
					(struct sockaddr*)&transport->send_gsr.gsr_group,
					pgm_sockaddr_len(&transport->send_gsr.gsr_group));
		if (sent < 0 && errno == EAGAIN)
		{
			transport->is_apdu_eagain = TRUE;
			goto blocked;
		}

/* save unfolded odata for retransmissions */
		*(guint32*)(void*)&((struct pgm_header*)STATE(pkt))->pgm_sport = STATE(unfolded_odata);

		if ( sent == (gssize)STATE(tpdu_length))
		{
			bytes_sent += STATE(tpdu_length) + transport->iphdr_len;	/* as counted at IP layer */
			packets_sent++;							/* IP packets */
			data_bytes_sent += STATE(tsdu_length);
		}

		STATE(data_bytes_offset) += STATE(tsdu_length);

/* check for end of transmission group */
		if (transport->use_proactive_parity)
		{
			guint32 odata_sqn = ((struct pgm_data*)(((struct pgm_header*)STATE(pkt)) + 1))->data_sqn;
			guint32 tg_sqn_mask = 0xffffffff << transport->tg_sqn_shift;
			if (!((g_ntohl(odata_sqn) + 1) & ~tg_sqn_mask))
			{
				pgm_schedule_proactive_nak (transport, g_ntohl(odata_sqn) & tg_sqn_mask);
			}
		}

	} while ( STATE(data_bytes_offset)  < STATE(apdu_length) );
	g_assert( STATE(data_bytes_offset) == STATE(apdu_length) );

/* release txw lock here in order to allow spms to lock mutex */
	g_static_rw_lock_writer_unlock (&transport->txw_lock);

	transport->is_apdu_eagain = FALSE;
	pgm_reset_heartbeat_spm (transport);

	transport->cumulative_stats[PGM_PC_SOURCE_BYTES_SENT]      += bytes_sent;
	transport->cumulative_stats[PGM_PC_SOURCE_DATA_MSGS_SENT]  += packets_sent;
	transport->cumulative_stats[PGM_PC_SOURCE_DATA_BYTES_SENT] += data_bytes_sent;

	return (gssize)STATE(apdu_length);

blocked:
	if (bytes_sent)
	{
		pgm_reset_heartbeat_spm (transport);

		transport->cumulative_stats[PGM_PC_SOURCE_BYTES_SENT]      += bytes_sent;
		transport->cumulative_stats[PGM_PC_SOURCE_DATA_MSGS_SENT]  += packets_sent;
		transport->cumulative_stats[PGM_PC_SOURCE_DATA_BYTES_SENT] += data_bytes_sent;
	}

	errno = EAGAIN;
	return -1;
}

/* send PGM original data, transmit window owned scatter/gather IO vector.
 *
 *    ⎢ TSDU₀ ⎢
 *    ⎢ TSDU₁ ⎢ → pgm_transport_send_packetv() →  ⎢ ⋯ TSDU₁ TSDU₀ ⎢ → libc
 *    ⎢   ⋮   ⎢
 *
 * on success, returns number of data bytes pushed into the transmit window and
 * attempted to send to the socket layer.  on non-blocking sockets, -1 is returned
 * if the packet sizes would exceed the current rate limit.
 */
gssize
pgm_transport_send_packetv (
	pgm_transport_t*	transport,
	const struct iovec*	vector,		/* packet */
	guint			count,
	int			flags,		/* MSG_DONTWAIT = rate non-blocking,
						   MSG_WAITALL  = packet blocking   */
	gboolean		is_one_apdu	/* true: vector = apdu,
                                                  false: vector::iov_base = apdu */
	)
{
	g_return_val_if_fail (transport != NULL, -EINVAL);

/* reject on closed transport */
	if (!transport->is_open) {
		errno = ECONNRESET;
		return -1;
	}

/* pass on zero length as cannot count vector lengths */
	if (count == 0) {
		return pgm_transport_send_one_copy (transport, NULL, count, flags);
	}
	g_return_val_if_fail (vector != NULL, -EINVAL);
	if (count == 1) {
		return pgm_transport_send_one (transport, vector->iov_base, vector->iov_len, flags);
	}

	g_assert( !(flags & MSG_WAITALL && !(flags & MSG_DONTWAIT)) );

	gsize bytes_sent	= 0;
	guint packets_sent	= 0;
	gsize data_bytes_sent	= 0;

/* continue if blocked mid-apdu */
	if (transport->is_apdu_eagain) {
		goto retry_send;
	}

	STATE(is_rate_limited) = FALSE;
	if (flags & MSG_DONTWAIT && flags & MSG_WAITALL)
	{
		gsize total_tpdu_length = 0;
		for (guint i = 0; i < count; i++)
		{
			total_tpdu_length += transport->iphdr_len + pgm_transport_pkt_offset (is_one_apdu) + vector[i].iov_len;
		}

/* calculation includes one iphdr length already */
		int result = pgm_rate_check (transport->rate_control, total_tpdu_length - transport->iphdr_len, flags);
		if (result == -1) {
			return (gssize)result;
		}

		STATE(is_rate_limited) = TRUE;
	}

	g_static_rw_lock_writer_lock (&transport->txw_lock);

	if (is_one_apdu)
	{
		STATE(apdu_length)	= 0;
		STATE(first_sqn)	= pgm_txw_next_lead(transport->txw);
		for (guint i = 0; i < count; i++)
		{
			g_return_val_if_fail (vector[i].iov_len <= transport->max_tsdu_fragment, -EMSGSIZE);
			STATE(apdu_length) += vector[i].iov_len;
		}
	}

	for (STATE(vector_index) = 0; STATE(vector_index) < count; STATE(vector_index)++)
	{
		STATE(tsdu_length) = vector[STATE(vector_index)].iov_len;
		STATE(tpdu_length) = pgm_transport_pkt_offset (is_one_apdu) + STATE(tsdu_length);

		STATE(pkt) = (guint8*)vector[STATE(vector_index)].iov_base - pgm_transport_pkt_offset (is_one_apdu);
		struct pgm_header *header = (struct pgm_header*)STATE(pkt);
		memcpy (header->pgm_gsi, &transport->tsi.gsi, sizeof(pgm_gsi_t));
		header->pgm_sport	= transport->tsi.sport;
		header->pgm_dport	= transport->dport;
		header->pgm_type        = PGM_ODATA;
	        header->pgm_options     = is_one_apdu ? PGM_OPT_PRESENT : 0;
	        header->pgm_tsdu_length = g_htons (STATE(tsdu_length));

/* ODATA */
		struct pgm_data *odata = (struct pgm_data*)(header + 1);
	        odata->data_sqn         = g_htonl (pgm_txw_next_lead(transport->txw));
	        odata->data_trail       = g_htonl (pgm_txw_trail(transport->txw));

		gpointer dst = NULL;

		if (is_one_apdu)
		{
/* OPT_LENGTH */
			struct pgm_opt_length* opt_len = (struct pgm_opt_length*)(odata + 1);
			opt_len->opt_type	= PGM_OPT_LENGTH;
			opt_len->opt_length	= sizeof(struct pgm_opt_length);
			opt_len->opt_total_length	= g_htons (	sizeof(struct pgm_opt_length) +
									sizeof(struct pgm_opt_header) +
									sizeof(struct pgm_opt_fragment) );
/* OPT_FRAGMENT */
			struct pgm_opt_header* opt_header = (struct pgm_opt_header*)(opt_len + 1);
			opt_header->opt_type	= PGM_OPT_FRAGMENT | PGM_OPT_END;
			opt_header->opt_length	= sizeof(struct pgm_opt_header) +
							sizeof(struct pgm_opt_fragment);
			struct pgm_opt_fragment* opt_fragment = (struct pgm_opt_fragment*)(opt_header + 1);
			opt_fragment->opt_reserved	= 0;
			opt_fragment->opt_sqn		= g_htonl (STATE(first_sqn));
			opt_fragment->opt_frag_off	= g_htonl (STATE(data_bytes_offset));
			opt_fragment->opt_frag_len	= g_htonl (STATE(apdu_length));

			dst = opt_fragment + 1;
		}
		else
		{
			dst = odata + 1;
		}

/* TODO: the assembly checksum & copy routine is faster than memcpy & pgm_cksum on >= opteron hardware */
	        header->pgm_checksum    = 0;

		gsize pgm_header_len	= (guint8*)dst - (guint8*)header;
		guint32 unfolded_header = pgm_csum_partial (header, pgm_header_len, 0);
		STATE(unfolded_odata)	= pgm_csum_partial ((guint8*)vector[STATE(vector_index)].iov_base, STATE(tsdu_length), 0);
		header->pgm_checksum	= pgm_csum_fold (pgm_csum_block_add (unfolded_header, STATE(unfolded_odata), pgm_header_len));

/* add to transmit window */
		pgm_txw_push (transport->txw, STATE(pkt), STATE(tpdu_length));
		gssize sent;
retry_send:
		sent = pgm_sendto (transport,
					!STATE(is_rate_limited),	/* rate limited on blocking */
					FALSE,				/* regular socket */
					STATE(pkt),
					STATE(tpdu_length),
					(flags & MSG_DONTWAIT && flags & MSG_WAITALL) ?
						flags & ~(MSG_DONTWAIT | MSG_WAITALL) :
						flags,
					(struct sockaddr*)&transport->send_gsr.gsr_group,
					pgm_sockaddr_len(&transport->send_gsr.gsr_group));
		if (sent < 0 && errno == EAGAIN)
		{
			transport->is_apdu_eagain = TRUE;
			goto blocked;
		}

/* save unfolded odata for retransmissions */
		*(guint32*)(void*)&((struct pgm_header*)STATE(pkt))->pgm_sport = STATE(unfolded_odata);

		if (sent == (gssize)STATE(tpdu_length))
		{
			bytes_sent += STATE(tpdu_length) + transport->iphdr_len;	/* as counted at IP layer */
			packets_sent++;							/* IP packets */
			data_bytes_sent += STATE(tsdu_length);
		}

		STATE(data_bytes_offset) += STATE(tsdu_length);

/* check for end of transmission group */
		if (transport->use_proactive_parity)
		{
			guint32 odata_sqn = ((struct pgm_data*)(((struct pgm_header*)STATE(pkt)) + 1))->data_sqn;
			guint32 tg_sqn_mask = 0xffffffff << transport->tg_sqn_shift;
			if (!((g_ntohl(odata_sqn) + 1) & ~tg_sqn_mask))
			{
				pgm_schedule_proactive_nak (transport, g_ntohl(odata_sqn) & tg_sqn_mask);
			}
		}

	}
#ifdef TRANSPORT_DEBUG
	if (is_one_apdu)
	{
		g_assert( STATE(data_bytes_offset) == STATE(apdu_length) );
	}
#endif

/* release txw lock here in order to allow spms to lock mutex */
	g_static_rw_lock_writer_unlock (&transport->txw_lock);

	transport->is_apdu_eagain = FALSE;
	pgm_reset_heartbeat_spm (transport);

	transport->cumulative_stats[PGM_PC_SOURCE_BYTES_SENT]      += bytes_sent;
	transport->cumulative_stats[PGM_PC_SOURCE_DATA_MSGS_SENT]  += packets_sent;
	transport->cumulative_stats[PGM_PC_SOURCE_DATA_BYTES_SENT] += data_bytes_sent;

	return (gssize)data_bytes_sent;

blocked:
	if (bytes_sent)
	{
		pgm_reset_heartbeat_spm (transport);

		transport->cumulative_stats[PGM_PC_SOURCE_BYTES_SENT]      += bytes_sent;
		transport->cumulative_stats[PGM_PC_SOURCE_DATA_MSGS_SENT]  += packets_sent;
		transport->cumulative_stats[PGM_PC_SOURCE_DATA_BYTES_SENT] += data_bytes_sent;
	}

	errno = EAGAIN;
	return -1;
}

/* cleanup resuming send state helper 
 */
#undef STATE

/* send repair packet.
 *
 * on success, 0 is returned.  on error, -1 is returned, and errno set
 * appropriately.
 */
static int
send_rdata (
	pgm_transport_t*	transport,
	G_GNUC_UNUSED guint32	sequence_number,
	gpointer		data,
	gsize			len,
	gboolean		has_saved_partial_csum
	)
{
/* update previous odata/rdata contents */
	struct pgm_header* header = (struct pgm_header*)data;
	struct pgm_data* rdata    = (struct pgm_data*)(header + 1);
	header->pgm_type          = PGM_RDATA;

/* RDATA */
        rdata->data_trail       = g_htonl (pgm_txw_trail(transport->txw));

	guint32 unfolded_odata	= 0;
	if (has_saved_partial_csum)
	{
		unfolded_odata	= *(guint32*)(void*)&header->pgm_sport;
	}
	header->pgm_sport	= transport->tsi.sport;
	header->pgm_dport	= transport->dport;

        header->pgm_checksum    = 0;

	gsize pgm_header_len	= len - g_ntohs(header->pgm_tsdu_length);
	guint32 unfolded_header = pgm_csum_partial (header, pgm_header_len, 0);
	if (!has_saved_partial_csum)
	{
		unfolded_odata	= pgm_csum_partial ((guint8*)data + pgm_header_len, g_ntohs(header->pgm_tsdu_length), 0);
	}
	header->pgm_checksum	= pgm_csum_fold (pgm_csum_block_add (unfolded_header, unfolded_odata, pgm_header_len));

	gssize sent = pgm_sendto (transport,
				TRUE,			/* rate limited */
				TRUE,			/* with router alert */
				header,
				len,
				MSG_CONFIRM,		/* not expecting a reply */
				(struct sockaddr*)&transport->send_gsr.gsr_group,
				pgm_sockaddr_len(&transport->send_gsr.gsr_group));

/* re-save unfolded payload for further retransmissions */
	if (has_saved_partial_csum)
	{
		*(guint32*)(void*)&header->pgm_sport = unfolded_odata;
	}

/* re-set spm timer: we are already in the timer thread, no need to prod timers
 */
	g_static_mutex_lock (&transport->mutex);
	transport->spm_heartbeat_state = 1;
	transport->next_heartbeat_spm = pgm_time_update_now() + transport->spm_heartbeat_interval[transport->spm_heartbeat_state++];
	g_static_mutex_unlock (&transport->mutex);

	if ( sent != (gssize)len )
	{
		return -1;
	}

	transport->cumulative_stats[PGM_PC_SOURCE_SELECTIVE_BYTES_RETRANSMITTED] += g_ntohs(header->pgm_tsdu_length);
	transport->cumulative_stats[PGM_PC_SOURCE_SELECTIVE_MSGS_RETRANSMITTED]++;	/* impossible to determine APDU count */
	transport->cumulative_stats[PGM_PC_SOURCE_BYTES_SENT] += len + transport->iphdr_len;

	return 0;
}

/* Enable FEC for this transport, specifically Reed Solmon encoding RS(n,k), common
 * setting is RS(255, 223).
 *
 * inputs:
 *
 * n = FEC Block size = [k+1, 255]
 * k = original data packets == transmission group size = [2, 4, 8, 16, 32, 64, 128]
 * m = symbol size = 8 bits
 *
 * outputs:
 *
 * h = 2t = n - k = parity packets
 *
 * when h > k parity packets can be lost.
 *
 * on success, returns 0.  on invalid setting, returns -EINVAL.
 */

int
pgm_transport_set_fec (
	pgm_transport_t*	transport,
	guint			proactive_h,		/* 0 == no pro-active parity */
	gboolean		use_ondemand_parity,
	gboolean		use_varpkt_len,
	guint			default_n,
	guint			default_k
	)
{
	g_return_val_if_fail (transport != NULL, -EINVAL);
	g_return_val_if_fail ((default_k & (default_k -1)) == 0, -EINVAL);
	g_return_val_if_fail (default_k >= 2 && default_k <= 128, -EINVAL);
	g_return_val_if_fail (default_n >= default_k + 1 && default_n <= 255, -EINVAL);

	guint default_h = default_n - default_k;

	g_return_val_if_fail (proactive_h <= default_h, -EINVAL);

/* check validity of parameters */
	if ( default_k > 223 &&
		( (default_h * 223.0) / default_k ) < 1.0 )
	{
		g_error ("k/h ratio too low to generate parity data.");
		return -EINVAL;
	}

	g_static_mutex_lock (&transport->mutex);
	transport->use_proactive_parity	= proactive_h > 0;
	transport->use_ondemand_parity	= use_ondemand_parity;
	transport->use_varpkt_len	= use_varpkt_len;
	transport->rs_n			= default_n;
	transport->rs_k			= default_k;
	transport->rs_proactive_h	= proactive_h;
	transport->tg_sqn_shift		= pgm_power2_log2 (transport->rs_k);

	g_static_mutex_unlock (&transport->mutex);

	return 0;
}

/* declare transport only for sending, discard any incoming SPM, ODATA,
 * RDATA, etc, packets.
 */
int
pgm_transport_set_send_only (
	pgm_transport_t*	transport,
	gboolean		send_only
	)
{
	g_return_val_if_fail (transport != NULL, -EINVAL);

	g_static_mutex_lock (&transport->mutex);
	transport->can_recv	= !send_only;
	g_static_mutex_unlock (&transport->mutex);

	return 0;
}

/* declare transport only for receiving, no transmit window will be created
 * and no SPM broadcasts sent.
 */
int
pgm_transport_set_recv_only (
	pgm_transport_t*	transport,
	gboolean		is_passive	/* don't send any request or responses */
	)
{
	g_return_val_if_fail (transport != NULL, -EINVAL);

	g_static_mutex_lock (&transport->mutex);
	transport->can_send_data	= FALSE;
	transport->can_send_nak		= !is_passive;
	g_static_mutex_unlock (&transport->mutex);

	return 0;
}

/* on unrecoverable data loss shutdown transport from further transmission or
 * receiving.
 */
int
pgm_transport_set_close_on_failure (
	pgm_transport_t*	transport,
	gboolean		close_on_failure
	)
{
	g_return_val_if_fail (transport != NULL, -EINVAL);

	g_static_mutex_lock (&transport->mutex);
	transport->will_close_on_failure = close_on_failure;
	g_static_mutex_unlock (&transport->mutex);

	return 0;
}


#define SOCKADDR_TO_LEVEL(sa)	( (AF_INET == pgm_sockaddr_family((sa))) ? IPPROTO_IP : IPPROTO_IPV6 )
#define TRANSPORT_TO_LEVEL(t)	SOCKADDR_TO_LEVEL( &(t)->recv_gsr[0].gsr_group )


/* for any-source applications (ASM), join a new group
 */
int
pgm_transport_join_group (
	pgm_transport_t*	transport,
	struct group_req*	gr,
	gsize			len
	)
{
	g_return_val_if_fail (transport != NULL, -EINVAL);
	g_return_val_if_fail (gr != NULL, -EINVAL);
	g_return_val_if_fail (sizeof(struct group_req) == len, -EINVAL);
	g_return_val_if_fail (transport->recv_gsr_len < IP_MAX_MEMBERSHIPS, -EINVAL);

/* verify not duplicate group/interface pairing */
	for (unsigned i = 0; i < transport->recv_gsr_len; i++)
	{
		if (pgm_sockaddr_cmp ((struct sockaddr*)&gr->gr_group, (struct sockaddr*)&transport->recv_gsr[i].gsr_group)  == 0 &&
		    pgm_sockaddr_cmp ((struct sockaddr*)&gr->gr_group, (struct sockaddr*)&transport->recv_gsr[i].gsr_source) == 0 &&
			(gr->gr_interface == transport->recv_gsr[i].gsr_interface ||
			                0 == transport->recv_gsr[i].gsr_interface    )
                   )
		{
#ifdef TRANSPORT_DEBUG
			char s[INET6_ADDRSTRLEN];
			pgm_sockaddr_ntop (&gr->gr_group, s, sizeof(s));
			if (transport->recv_gsr[i].gsr_interface) {
				g_trace("INFO", "transport has already joined group %s on interface %i.", s, gr->gr_interface);
			} else {
				g_trace("INFO", "transport has already joined group %s on all interfaces.", s);
			}
#endif
			return -EINVAL;
		}
	}

	transport->recv_gsr[transport->recv_gsr_len].gsr_interface = 0;
	memcpy (&transport->recv_gsr[transport->recv_gsr_len].gsr_group, &gr->gr_group, pgm_sockaddr_len(&gr->gr_group));
	memcpy (&transport->recv_gsr[transport->recv_gsr_len].gsr_source, &gr->gr_group, pgm_sockaddr_len(&gr->gr_group));
	transport->recv_gsr_len++;
	return setsockopt(transport->recv_sock, TRANSPORT_TO_LEVEL(transport), MCAST_JOIN_GROUP, gr, len);
}

/* for any-source applications (ASM), leave a joined group.
 */
int
pgm_transport_leave_group (
	pgm_transport_t*	transport,
	struct group_req*	gr,
	gsize			len
	)
{
	g_return_val_if_fail (transport != NULL, -EINVAL);
	g_return_val_if_fail (gr != NULL, -EINVAL);
	g_return_val_if_fail (sizeof(struct group_req) == len, -EINVAL);
	g_return_val_if_fail (transport->recv_gsr_len == 0, -EINVAL);

	for (unsigned i = 0; i < transport->recv_gsr_len;)
	{
		if ((pgm_sockaddr_cmp ((struct sockaddr*)&gr->gr_group, (struct sockaddr*)&transport->recv_gsr[i].gsr_group) == 0) &&
/* drop all matching receiver entries */
		            (gr->gr_interface == 0 ||
/* drop all sources with matching interface */
			     gr->gr_interface == transport->recv_gsr[i].gsr_interface) )
		{
			transport->recv_gsr_len--;
			if (i < (IP_MAX_MEMBERSHIPS-1))
			{
				memmove (&transport->recv_gsr[i], &transport->recv_gsr[i+1], (transport->recv_gsr_len - i) * sizeof(struct group_source_req));
				continue;
			}
		}
		i++;
	}
	return setsockopt(transport->recv_sock, TRANSPORT_TO_LEVEL(transport), MCAST_LEAVE_GROUP, gr, len);
}

/* for any-source applications (ASM), turn off a given source
 */
int
pgm_transport_block_source (
	pgm_transport_t*	transport,
	struct group_source_req* gsr,
	gsize			len
	)
{
	g_return_val_if_fail (transport != NULL, -EINVAL);
	g_return_val_if_fail (gsr != NULL, -EINVAL);
	g_return_val_if_fail (sizeof(struct group_source_req) == len, -EINVAL);
	return setsockopt(transport->recv_sock, TRANSPORT_TO_LEVEL(transport), MCAST_BLOCK_SOURCE, gsr, len);
}

/* for any-source applications (ASM), re-allow a blocked source
 */
int
pgm_transport_unblock_source (
	pgm_transport_t*	transport,
	struct group_source_req* gsr,
	gsize			len
	)
{
	g_return_val_if_fail (transport != NULL, -EINVAL);
	g_return_val_if_fail (gsr != NULL, -EINVAL);
	g_return_val_if_fail (sizeof(struct group_source_req) == len, -EINVAL);
	return setsockopt(transport->recv_sock, TRANSPORT_TO_LEVEL(transport), MCAST_UNBLOCK_SOURCE, gsr, len);
}

/* for controlled-source applications (SSM), join each group/source pair.
 *
 * SSM joins are allowed on top of ASM in order to merge a remote source onto the local segment.
 */
int
pgm_transport_join_source_group (
	pgm_transport_t*	transport,
	struct group_source_req* gsr,
	gsize			len
	)
{
	g_return_val_if_fail (transport != NULL, -EINVAL);
	g_return_val_if_fail (gsr != NULL, -EINVAL);
	g_return_val_if_fail (sizeof(struct group_source_req) == len, -EINVAL);
	g_return_val_if_fail (transport->recv_gsr_len < IP_MAX_MEMBERSHIPS, -EINVAL);

/* verify if existing group/interface pairing */
	for (unsigned i = 0; i < transport->recv_gsr_len; i++)
	{
		if (pgm_sockaddr_cmp ((struct sockaddr*)&gsr->gsr_group, (struct sockaddr*)&transport->recv_gsr[i].gsr_group) == 0 &&
			(gsr->gsr_interface == transport->recv_gsr[i].gsr_interface ||
			                  0 == transport->recv_gsr[i].gsr_interface    )
                   )
		{
			if (pgm_sockaddr_cmp ((struct sockaddr*)&gsr->gsr_source, (struct sockaddr*)&transport->recv_gsr[i].gsr_source) == 0)
			{
#ifdef TRANSPORT_DEBUG
				char s1[INET6_ADDRSTRLEN], s2[INET6_ADDRSTRLEN];
				pgm_sockaddr_ntop (&gsr->gsr_group, s1, sizeof(s1));
				pgm_sockaddr_ntop (&gsr->gsr_source, s2, sizeof(s2));
				if (transport->recv_gsr[i].gsr_interface) {
					g_trace("INFO", "transport has already joined group %s from source %s on interface %i.", s1, s2, gsr->gsr_interface);
				} else {
					g_trace("INFO", "transport has already joined group %s from source %s on all interfaces.", s1, s2);
				}
#endif
				return -EINVAL;
			}
			break;
		}
	}

	memcpy (&transport->recv_gsr[transport->recv_gsr_len], &gsr, sizeof(struct group_source_req));
	transport->recv_gsr_len++;
	return setsockopt(transport->recv_sock, TRANSPORT_TO_LEVEL(transport), MCAST_JOIN_SOURCE_GROUP, gsr, len);
}

/* for controlled-source applications (SSM), leave each group/source pair
 */
int
pgm_transport_leave_source_group (
	pgm_transport_t*	transport,
	struct group_source_req* gsr,
	gsize			len
	)
{
	g_return_val_if_fail (transport != NULL, -EINVAL);
	g_return_val_if_fail (gsr != NULL, -EINVAL);
	g_return_val_if_fail (sizeof(struct group_source_req) == len, -EINVAL);
	g_return_val_if_fail (transport->recv_gsr_len == 0, -EINVAL);

/* verify if existing group/interface pairing */
	for (unsigned i = 0; i < transport->recv_gsr_len; i++)
	{
		if (pgm_sockaddr_cmp ((struct sockaddr*)&gsr->gsr_group, (struct sockaddr*)&transport->recv_gsr[i].gsr_group)   == 0 &&
		    pgm_sockaddr_cmp ((struct sockaddr*)&gsr->gsr_source, (struct sockaddr*)&transport->recv_gsr[i].gsr_source) == 0 &&
		    gsr->gsr_interface == transport->recv_gsr[i].gsr_interface)
		{
			transport->recv_gsr_len--;
			if (i < (IP_MAX_MEMBERSHIPS-1))
			{
				memmove (&transport->recv_gsr[i], &transport->recv_gsr[i+1], (transport->recv_gsr_len - i) * sizeof(struct group_source_req));
				break;
			}
		}
	}

	return setsockopt(transport->recv_sock, TRANSPORT_TO_LEVEL(transport), MCAST_LEAVE_SOURCE_GROUP, gsr, len);
}

int
pgm_transport_msfilter (
	pgm_transport_t*	transport,
	struct group_filter*	gf_list,
	gsize			len
	)
{
	g_return_val_if_fail (transport != NULL, -EINVAL);
	g_return_val_if_fail (gf_list != NULL, -EINVAL);
	g_return_val_if_fail (len > 0, -EINVAL);
	g_return_val_if_fail (GROUP_FILTER_SIZE(gf_list->gf_numsrc) == len, -EINVAL);
	return setsockopt(transport->recv_sock, TRANSPORT_TO_LEVEL(transport), MCAST_MSFILTER, gf_list, len);
}

static GSource*
pgm_create_timer (
	pgm_transport_t*	transport
	)
{
	g_return_val_if_fail (transport != NULL, NULL);

	GSource *source = g_source_new (&g_pgm_timer_funcs, sizeof(pgm_timer_t));
	pgm_timer_t *timer = (pgm_timer_t*)source;

	timer->transport = transport;

	return source;
}

/* on success, returns id of GSource 
 */
static int
pgm_add_timer_full (
	pgm_transport_t*	transport,
	gint			priority
	)
{
	g_return_val_if_fail (transport != NULL, -EINVAL);

	GSource* source = pgm_create_timer (transport);

	if (priority != G_PRIORITY_DEFAULT)
		g_source_set_priority (source, priority);

	guint id = g_source_attach (source, transport->timer_context);
	g_source_unref (source);

	return id;
}

static int
pgm_add_timer (
	pgm_transport_t*	transport
	)
{
	return pgm_add_timer_full (transport, G_PRIORITY_HIGH_IDLE);
}

/* determine which timer fires next: spm (ihb_tmr), nak_rb_ivl, nak_rpt_ivl, or nak_rdata_ivl
 * and check whether its already due.
 */

static gboolean
pgm_timer_prepare (
	GSource*		source,
	gint*			timeout
	)
{
	pgm_timer_t* pgm_timer = (pgm_timer_t*)source;
	pgm_transport_t* transport = pgm_timer->transport;
	glong msec;

	g_static_mutex_lock (&transport->mutex);
	pgm_time_t now = pgm_time_update_now();
	pgm_time_t expiration = now + pgm_secs( 30 );

	if (transport->can_send_data)
	{
		expiration = transport->spm_heartbeat_state ? MIN(transport->next_heartbeat_spm, transport->next_ambient_spm) : transport->next_ambient_spm;
		g_trace ("SPM","spm %" G_GINT64_FORMAT " usec", (gint64)expiration - (gint64)now);
	}

/* save the nearest timer */
	if (transport->can_recv)
	{
		g_static_rw_lock_reader_lock (&transport->peers_lock);
		expiration = min_nak_expiry (expiration, transport);
		g_static_rw_lock_reader_unlock (&transport->peers_lock);
	}

	transport->next_poll = pgm_timer->expiration = expiration;
	g_static_mutex_unlock (&transport->mutex);

/* advance time again to adjust for processing time out of the event loop, this
 * could cause further timers to expire even before checking for new wire data.
 */
	msec = pgm_to_msecs((gint64)expiration - (gint64)now);
	if (msec < 0)
		msec = 0;
	else
		msec = MIN (G_MAXINT, (guint)msec);

	*timeout = (gint)msec;

	g_trace ("SPM","expiration in %i msec", (gint)msec);

	return (msec == 0);
}

static gboolean
pgm_timer_check (
	GSource*		source
	)
{
	g_trace ("SPM","pgm_timer_check");

	pgm_timer_t* pgm_timer = (pgm_timer_t*)source;
	const pgm_time_t now = pgm_time_update_now();

	gboolean retval = ( pgm_time_after_eq(now, pgm_timer->expiration) );
	if (!retval) g_thread_yield();
	return retval;
}

/* call all timers, assume that time_now has been updated by either pgm_timer_prepare
 * or pgm_timer_check and no other method calls here.
 */

static gboolean
pgm_timer_dispatch (
	GSource*			source,
	G_GNUC_UNUSED GSourceFunc	callback,
	G_GNUC_UNUSED gpointer		user_data
	)
{
	g_trace ("SPM","pgm_timer_dispatch");

	pgm_timer_t* pgm_timer = (pgm_timer_t*)source;
	pgm_transport_t* transport = pgm_timer->transport;

/* find which timers have expired and call each */
	if (transport->can_send_data)
	{
		g_static_mutex_lock (&transport->mutex);
		if ( pgm_time_after_eq (pgm_time_now, transport->next_ambient_spm) )
		{
			send_spm_unlocked (transport);
			transport->spm_heartbeat_state = 0;
			transport->next_ambient_spm = pgm_time_now + transport->spm_ambient_interval;
		}
		else if ( transport->spm_heartbeat_state &&
			 pgm_time_after_eq (pgm_time_now, transport->next_heartbeat_spm) )
		{
			send_spm_unlocked (transport);
		
			if (transport->spm_heartbeat_interval[transport->spm_heartbeat_state])
			{
				transport->next_heartbeat_spm = pgm_time_now + transport->spm_heartbeat_interval[transport->spm_heartbeat_state++];
			}
			else
			{	/* transition heartbeat to ambient */
				transport->spm_heartbeat_state = 0;
			}
		}
		g_static_mutex_unlock (&transport->mutex);
	}

	if (transport->can_recv)
	{
		g_static_mutex_lock (&transport->waiting_mutex);
		g_static_mutex_lock (&transport->mutex);
		g_static_rw_lock_reader_lock (&transport->peers_lock);
		check_peer_nak_state (transport);
		g_static_rw_lock_reader_unlock (&transport->peers_lock);
		g_static_mutex_unlock (&transport->mutex);
		g_static_mutex_unlock (&transport->waiting_mutex);
	}

	return TRUE;
}


/* TODO: this should be in on_io_data to be more streamlined, or a generic options parser.
 */

static int
get_opt_fragment (
	struct pgm_opt_header*		opt_header,
	struct pgm_opt_fragment**	opt_fragment
	)
{
	int retval = 0;

	g_assert (opt_header->opt_type == PGM_OPT_LENGTH);
	g_assert (opt_header->opt_length == sizeof(struct pgm_opt_length));
//	struct pgm_opt_length* opt_len = (struct pgm_opt_length*)opt_header;

/* always at least two options, first is always opt_length */
	do {
		opt_header = (struct pgm_opt_header*)((char*)opt_header + opt_header->opt_length);

		if ((opt_header->opt_type & PGM_OPT_MASK) == PGM_OPT_FRAGMENT)
		{
			*opt_fragment = (struct pgm_opt_fragment*)(opt_header + 1);
			retval = 1;
			goto out;
		}

	} while (!(opt_header->opt_type & PGM_OPT_END));

	*opt_fragment = NULL;

out:
	return retval;
}

/* ODATA packet with any of the following options:
 *
 * OPT_FRAGMENT - this TPDU part of a larger APDU.
 *
 * returns:
 *	0
 *	-EINVAL if pipe proding failed
 */

static int
on_odata (
	pgm_peer_t*		sender,
	struct pgm_header*	header,
	gpointer		data,
	G_GNUC_UNUSED gsize	len
	)
{
	g_trace ("INFO","on_odata");

	int retval = 0;
	pgm_transport_t* transport = sender->transport;
	struct pgm_data* odata = (struct pgm_data*)data;
	odata->data_sqn = g_ntohl (odata->data_sqn);

/* pre-allocate from glib allocator (not slice allocator) full APDU packet for first new fragment, re-use
 * through to event handler.
 */
	struct pgm_opt_fragment* opt_fragment;
	const pgm_time_t nak_rb_expiry = pgm_time_update_now () + nak_rb_ivl(transport);
	const guint16 opt_total_length = (header->pgm_options & PGM_OPT_PRESENT) ? g_ntohs(*(guint16*)( (char*)( odata + 1 ) + sizeof(guint16))) : 0;

	guint msg_count = 0;

	g_static_mutex_lock (&sender->mutex);
	if (opt_total_length > 0 && get_opt_fragment((gpointer)(odata + 1), &opt_fragment))
	{
		g_trace ("INFO","push fragment (sqn #%u trail #%u apdu_first_sqn #%u fragment_offset %u apdu_len %u)",
			odata->data_sqn, g_ntohl (odata->data_trail), g_ntohl (opt_fragment->opt_sqn), g_ntohl (opt_fragment->opt_frag_off), g_ntohl (opt_fragment->opt_frag_len));
		guint32 lead_before_push = ((pgm_rxw_t*)sender->rxw)->lead;
		retval = pgm_rxw_push_fragment_copy (sender->rxw,
					(char*)(odata + 1) + opt_total_length,
					g_ntohs (header->pgm_tsdu_length),
					odata->data_sqn,
					g_ntohl (odata->data_trail),
					opt_fragment,
					nak_rb_expiry);
/* new APDU only when reported first APDU sequence number extends the window lead */
		if ( pgm_uint32_lte (lead_before_push, g_ntohl (opt_fragment->opt_sqn) ) )
		{
			msg_count++;
		}
	}
	else
	{
		retval = pgm_rxw_push_copy (sender->rxw,
					(char*)(odata + 1) + opt_total_length,
					g_ntohs (header->pgm_tsdu_length),
					odata->data_sqn,
					g_ntohl (odata->data_trail),
					nak_rb_expiry);
		msg_count++;
	}

	g_static_mutex_unlock (&sender->mutex);

	gboolean flush_naks = FALSE;

	switch (retval) {
	case PGM_RXW_CREATED_PLACEHOLDER:
		flush_naks = TRUE;
		break;

	case PGM_RXW_DUPLICATE:
		sender->cumulative_stats[PGM_PC_RECEIVER_DUP_DATAS]++;
		sender->cumulative_stats[PGM_PC_RECEIVER_PACKETS_DISCARDED]++;
		break;

	case PGM_RXW_MALFORMED_APDU:
		sender->cumulative_stats[PGM_PC_RECEIVER_MALFORMED_ODATA]++;

	case PGM_RXW_NOT_IN_TXW:
	case PGM_RXW_APDU_LOST:
		sender->cumulative_stats[PGM_PC_RECEIVER_PACKETS_DISCARDED]++;
		break;

	default:
		break;
	}

	sender->cumulative_stats[PGM_PC_RECEIVER_DATA_BYTES_RECEIVED] += g_ntohs (header->pgm_tsdu_length);
	sender->cumulative_stats[PGM_PC_RECEIVER_DATA_MSGS_RECEIVED]  += msg_count;

	if (flush_naks)
	{
/* flush out 1st time nak packets */
		g_static_mutex_lock (&transport->mutex);

		if (pgm_time_after (transport->next_poll, nak_rb_expiry))
		{
			transport->next_poll = nak_rb_expiry;
			g_trace ("INFO","on_odata: prod timer thread");
			if (!pgm_notify_send (&transport->timer_notify)) {
				g_critical ("send to timer notify channel failed :(");
				retval = -EINVAL;
			}
		}

		g_static_mutex_unlock (&transport->mutex);
	}

	return retval;
}

/* identical to on_odata except for statistics
 */

static int
on_rdata (
	pgm_peer_t*		sender,
	struct pgm_header*	header,
	gpointer		data,
	G_GNUC_UNUSED gsize	len
	)
{
	g_trace ("INFO","on_rdata");

	int retval = 0;
	struct pgm_transport_t* transport = sender->transport;
	struct pgm_data* rdata = (struct pgm_data*)data;
	rdata->data_sqn = g_ntohl (rdata->data_sqn);

	gboolean flush_naks = FALSE;
	const pgm_time_t nak_rb_expiry = pgm_time_update_now () + nak_rb_ivl(transport);

/* parity RDATA needs to be decoded */
	if (header->pgm_options & PGM_OPT_PARITY)
	{
		const guint32 tg_sqn_mask = 0xffffffff << transport->tg_sqn_shift;
		const guint32 tg_sqn = rdata->data_sqn & tg_sqn_mask;

		const gboolean is_var_pktlen = header->pgm_options & PGM_OPT_VAR_PKTLEN;
		const gboolean is_op_encoded = header->pgm_options & PGM_OPT_PRESENT;		/* non-encoded options? */

/* determine payload location */
		guint8* rdata_bytes = (guint8*)(rdata + 1);
		struct pgm_opt_fragment* rdata_opt_fragment = NULL;
		if ((header->pgm_options & PGM_OPT_PRESENT) && get_opt_fragment((struct pgm_opt_header*)rdata_bytes, &rdata_opt_fragment))
		{
			guint16 opt_total_length = g_ntohs(*(guint16*)( (char*)( rdata + 1 ) + sizeof(guint16)));
			rdata_bytes += opt_total_length;
		}

/* create list of sequence numbers for each k packet in the FEC block */
		guint rs_h = 0;
		const gsize parity_length = g_ntohs (header->pgm_tsdu_length);
		guint32 target_sqn = tg_sqn - 1;
		guint8* src[ sender->rs_n ];
		guint8* src_opts[ sender->rs_n ];
		guint32 offsets[ sender->rs_k ];
		for (guint32 i = tg_sqn; i != (tg_sqn + sender->rs_k); i++)
		{
			struct pgm_opt_fragment* opt_fragment = NULL;
			gpointer packet = NULL;
			guint16 length = 0;
			gboolean is_parity = FALSE;
			int status = pgm_rxw_peek (sender->rxw, i, &opt_fragment, &packet, &length, &is_parity);

			if (status == PGM_RXW_DUPLICATE)	/* already committed */
				goto out;
			if (status == PGM_RXW_NOT_IN_TXW)
				goto out;

			if (length == 0 && !is_parity) {	/* nothing */

				if (target_sqn == tg_sqn - 1)
				{
/* keep parity packet here */
					target_sqn = i;
					src[ sender->rs_k + rs_h ] = rdata_bytes;
					src_opts[ sender->rs_k + rs_h ] = (guint8*)rdata_opt_fragment;
					offsets[ i - tg_sqn ] = sender->rs_k + rs_h++;

/* move repair to receive window ownership */
					pgm_rxw_push_nth_parity_copy (sender->rxw,
									i,
									g_ntohl (rdata->data_trail),
									rdata_opt_fragment,
									rdata_bytes,
									parity_length,
									nak_rb_expiry);
				}
				else
				{
/* transmission group incomplete */
					g_trace ("INFO", "transmission group incomplete, awaiting further repair packets.");
					goto out;
				}

			} else if (is_parity) {			/* repair data */
				src[ sender->rs_k + rs_h ] = packet;
				src_opts[ sender->rs_k + rs_h ] = (guint8*)opt_fragment;
				offsets[ i - tg_sqn ] = sender->rs_k + rs_h++;
			} else {				/* original data */
				src[ i - tg_sqn ] = packet;
				src_opts[ i - tg_sqn ] = (guint8*)opt_fragment;
				offsets[ i - tg_sqn ] = i - tg_sqn;
				if (!is_var_pktlen && length != parity_length) {
					g_warning ("Variable TSDU length without OPT_VAR_PKTLEN.\n");
					goto out;
				}

				pgm_rxw_zero_pad (sender->rxw, packet, length, parity_length);
			}
		}

/* full transmission group, now allocate new packets */
		for (unsigned i = 0; i < sender->rs_k; i++)
		{
			if (offsets[ i ] >= sender->rs_k)
			{
				src[ i ] = pgm_rxw_alloc (sender->rxw);
				memset (src[ i ], 0, parity_length);

				if (is_op_encoded) {
					src_opts[ i ] = g_slice_alloc0 (sizeof(struct pgm_opt_fragment));
				}
			}
		}

/* decode payload */
		pgm_rs_decode_parity_appended (sender->rs, (void**)(void*)src, offsets, parity_length);

/* decode opt_fragment option */
		if (is_op_encoded)
		{
			pgm_rs_decode_parity_appended (sender->rs, (void**)(void*)src_opts, offsets, sizeof(struct pgm_opt_fragment));
		}

/* treat decoded packet as selective repair(s) */
		g_static_mutex_lock (&sender->mutex);
		gsize repair_length = parity_length;
		for (unsigned i = 0; i < sender->rs_k; i++)
		{
			if (offsets[ i ] >= sender->rs_k)
			{
/* extract TSDU length is variable packet option was found on parity packet */
				if (is_var_pktlen)
				{
					repair_length = *(guint16*)( src[i] + parity_length - 2 );
				}

				if (is_op_encoded)
				{
					retval = pgm_rxw_push_nth_repair (sender->rxw,
								tg_sqn + i,
								g_ntohl (rdata->data_trail),
								(struct pgm_opt_fragment*)src_opts[ i ],
								src[ i ],
								repair_length,
								nak_rb_expiry);
					g_slice_free1 (sizeof(struct pgm_opt_fragment), src_opts[ i ]);
				}
				else
				{
					retval = pgm_rxw_push_nth_repair (sender->rxw,
								tg_sqn + i,
								g_ntohl (rdata->data_trail),
								NULL,
								src[ i ],
								repair_length,
								nak_rb_expiry);
				}
				switch (retval) {
				case PGM_RXW_CREATED_PLACEHOLDER:
				case PGM_RXW_DUPLICATE:
					g_warning ("repaired packets not matching receive window state.");
					break;

				case PGM_RXW_MALFORMED_APDU:
					sender->cumulative_stats[PGM_PC_RECEIVER_MALFORMED_RDATA]++;

				case PGM_RXW_NOT_IN_TXW:
				case PGM_RXW_APDU_LOST:
					sender->cumulative_stats[PGM_PC_RECEIVER_PACKETS_DISCARDED]++;
					break;

				default:
					break;
				}

				sender->cumulative_stats[PGM_PC_RECEIVER_DATA_BYTES_RECEIVED] += repair_length;
				sender->cumulative_stats[PGM_PC_RECEIVER_DATA_MSGS_RECEIVED]++;
			}
		}
		g_static_mutex_unlock (&sender->mutex);
	}
	else
	{
/* selective RDATA */

		struct pgm_opt_fragment* opt_fragment;
		const guint16 opt_total_length = (header->pgm_options & PGM_OPT_PRESENT) ? g_ntohs(*(guint16*)( (char*)( rdata + 1 ) + sizeof(guint16))) : 0;

		if (opt_total_length > 0 && get_opt_fragment((gpointer)(rdata + 1), &opt_fragment))
		{
			g_trace ("INFO","push fragment (sqn #%u trail #%u apdu_first_sqn #%u fragment_offset %u apdu_len %u)",
				 rdata->data_sqn, g_ntohl (rdata->data_trail), g_ntohl (opt_fragment->opt_sqn), g_ntohl (opt_fragment->opt_frag_off), g_ntohl (opt_fragment->opt_frag_len));
			g_static_mutex_lock (&sender->mutex);
			retval = pgm_rxw_push_fragment_copy (sender->rxw,
						(char*)(rdata + 1) + opt_total_length,
						g_ntohs (header->pgm_tsdu_length),
						rdata->data_sqn,
						g_ntohl (rdata->data_trail),
						opt_fragment,
						nak_rb_expiry);
		}
		else
		{
			g_static_mutex_lock (&sender->mutex);
			retval = pgm_rxw_push_copy (sender->rxw,
						(char*)(rdata + 1) + opt_total_length,
						g_ntohs (header->pgm_tsdu_length),
						rdata->data_sqn,
						g_ntohl (rdata->data_trail),
						nak_rb_expiry);
		}
		g_static_mutex_unlock (&sender->mutex);

		switch (retval) {
		case PGM_RXW_CREATED_PLACEHOLDER:
			flush_naks = TRUE;
			break;

		case PGM_RXW_DUPLICATE:
			sender->cumulative_stats[PGM_PC_RECEIVER_DUP_DATAS]++;
			sender->cumulative_stats[PGM_PC_RECEIVER_PACKETS_DISCARDED]++;
			break;

		case PGM_RXW_MALFORMED_APDU:
			sender->cumulative_stats[PGM_PC_RECEIVER_MALFORMED_RDATA]++;

		case PGM_RXW_NOT_IN_TXW:
		case PGM_RXW_APDU_LOST:
			sender->cumulative_stats[PGM_PC_RECEIVER_PACKETS_DISCARDED]++;
			break;

		default:
			break;
		}

		sender->cumulative_stats[PGM_PC_RECEIVER_DATA_BYTES_RECEIVED] += g_ntohs (header->pgm_tsdu_length);
		sender->cumulative_stats[PGM_PC_RECEIVER_DATA_MSGS_RECEIVED]++;
	}

	if (flush_naks)
	{
/* flush out 1st time nak packets */
		g_static_mutex_lock (&transport->mutex);

		if (pgm_time_after (transport->next_poll, nak_rb_expiry))
		{
			transport->next_poll = nak_rb_expiry;
			g_trace ("INFO","on_odata: prod timer thread");
			if (!pgm_notify_send (&transport->timer_notify)) {
				g_critical ("write to timer pipe failed :(");
				retval = -EINVAL;
			}
		}

		g_static_mutex_unlock (&transport->mutex);
	}

out:
	return retval;
}

/* eof */
