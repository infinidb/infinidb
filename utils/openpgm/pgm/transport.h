/* vim:ts=8:sts=4:sw=4:noai:noexpandtab
 * 
 * PGM transport.
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

#ifndef __PGM_TRANSPORT_H__
#define __PGM_TRANSPORT_H__

#include <poll.h>
#ifdef CONFIG_HAVE_EPOLL
#	include <sys/epoll.h>
#endif

#include <glib.h>


#ifndef __PGM_GSI_H__
#   include <pgm/gsi.h>
#endif

#ifndef __PGM_SOCKADDR_H__
#   include <pgm/sockaddr.h>
#endif

#ifndef __PGM_TIMER_H__
#   include <pgm/timer.h>
#endif

#ifndef __PGM_NOTIFY_H__
#   include <pgm/notify.h>
#endif


/* maximum length of TSI as a string */
#define PGM_TSISTRLEN		(sizeof("000.000.000.000.000.000.00000"))


/* Performance Counters */

typedef enum {
/* source side */
    PGM_PC_SOURCE_DATA_BYTES_SENT,
    PGM_PC_SOURCE_DATA_MSGS_SENT,	    /* msgs = packets not APDUs */
/*  PGM_PC_SOURCE_BYTES_BUFFERED, */	    /* tx window contents in bytes */
/*  PGM_PC_SOURCE_MSGS_BUFFERED, */
    PGM_PC_SOURCE_BYTES_SENT,
/*  PGM_PC_SOURCE_RAW_NAKS_RECEIVED, */
    PGM_PC_SOURCE_CKSUM_ERRORS,
    PGM_PC_SOURCE_MALFORMED_NAKS,
    PGM_PC_SOURCE_PACKETS_DISCARDED,
    PGM_PC_SOURCE_PARITY_BYTES_RETRANSMITTED,
    PGM_PC_SOURCE_SELECTIVE_BYTES_RETRANSMITTED,
    PGM_PC_SOURCE_PARITY_MSGS_RETRANSMITTED,
    PGM_PC_SOURCE_SELECTIVE_MSGS_RETRANSMITTED,
    PGM_PC_SOURCE_PARITY_NAK_PACKETS_RECEIVED,
    PGM_PC_SOURCE_SELECTIVE_NAK_PACKETS_RECEIVED,   /* total packets */
    PGM_PC_SOURCE_PARITY_NAKS_RECEIVED,
    PGM_PC_SOURCE_SELECTIVE_NAKS_RECEIVED,	    /* serial numbers */
    PGM_PC_SOURCE_PARITY_NAKS_IGNORED,
    PGM_PC_SOURCE_SELECTIVE_NAKS_IGNORED,
/*  PGM_PC_SOURCE_ACK_ERRORS, */
/*  PGM_PC_SOURCE_PGMCC_ACKER, */
    PGM_PC_SOURCE_TRANSMISSION_CURRENT_RATE,
/*  PGM_PC_SOURCE_ACK_PACKETS_RECEIVED, */
    PGM_PC_SOURCE_PARITY_NNAK_PACKETS_RECEIVED,
    PGM_PC_SOURCE_SELECTIVE_NNAK_PACKETS_RECEIVED,
    PGM_PC_SOURCE_PARITY_NNAKS_RECEIVED,
    PGM_PC_SOURCE_SELECTIVE_NNAKS_RECEIVED,
    PGM_PC_SOURCE_NNAK_ERRORS,

/* marker */
    PGM_PC_SOURCE_MAX
} pgm_pc_source_e;

typedef enum {
/* receiver side */
    PGM_PC_RECEIVER_DATA_BYTES_RECEIVED,
    PGM_PC_RECEIVER_DATA_MSGS_RECEIVED,
    PGM_PC_RECEIVER_NAK_FAILURES,
    PGM_PC_RECEIVER_BYTES_RECEIVED,
/*  PGM_PC_RECEIVER_CKSUM_ERRORS, */	    /* inherently same as source */
    PGM_PC_RECEIVER_MALFORMED_SPMS,
    PGM_PC_RECEIVER_MALFORMED_ODATA,
    PGM_PC_RECEIVER_MALFORMED_RDATA,
    PGM_PC_RECEIVER_MALFORMED_NCFS,
    PGM_PC_RECEIVER_PACKETS_DISCARDED,
    PGM_PC_RECEIVER_LOSSES,
/*  PGM_PC_RECEIVER_BYTES_DELIVERED_TO_APP, */
/*  PGM_PC_RECEIVER_MSGS_DELIVERED_TO_APP, */
    PGM_PC_RECEIVER_DUP_SPMS,
    PGM_PC_RECEIVER_DUP_DATAS,
    PGM_PC_RECEIVER_PARITY_NAK_PACKETS_SENT,
    PGM_PC_RECEIVER_SELECTIVE_NAK_PACKETS_SENT,
    PGM_PC_RECEIVER_PARITY_NAKS_SENT,
    PGM_PC_RECEIVER_SELECTIVE_NAKS_SENT,
    PGM_PC_RECEIVER_PARITY_NAKS_RETRANSMITTED,
    PGM_PC_RECEIVER_SELECTIVE_NAKS_RETRANSMITTED,
    PGM_PC_RECEIVER_PARITY_NAKS_FAILED,
    PGM_PC_RECEIVER_SELECTIVE_NAKS_FAILED,
    PGM_PC_RECEIVER_NAKS_FAILED_RXW_ADVANCED,
    PGM_PC_RECEIVER_NAKS_FAILED_NCF_RETRIES_EXCEEDED,
    PGM_PC_RECEIVER_NAKS_FAILED_DATA_RETRIES_EXCEEDED,
/*  PGM_PC_RECEIVER_NAKS_FAILED_GEN_EXPIRED */
    PGM_PC_RECEIVER_NAK_FAILURES_DELIVERED,
    PGM_PC_RECEIVER_SELECTIVE_NAKS_SUPPRESSED,
    PGM_PC_RECEIVER_NAK_ERRORS,
/*  PGM_PC_RECEIVER_LAST_ACTIVITY, */
/*  PGM_PC_RECEIVER_NAK_SVC_TIME_MIN, */
    PGM_PC_RECEIVER_NAK_SVC_TIME_MEAN,
/*  PGM_PC_RECEIVER_NAK_SVC_TIME_MAX, */
/*  PGM_PC_RECEIVER_NAK_FAIL_TIME_MIN, */
    PGM_PC_RECEIVER_NAK_FAIL_TIME_MEAN,
/*  PGM_PC_RECEIVER_NAK_FAIL_TIME_MAX, */
/*  PGM_PC_RECEIVER_TRANSMIT_MIN, */
    PGM_PC_RECEIVER_TRANSMIT_MEAN,
/*  PGM_PC_RECEIVER_TRANSMIT_MAX, */
/*  PGM_PC_RECEIVER_ACKS_SENT, */

/* marker */
    PGM_PC_RECEIVER_MAX
} pgm_pc_receiver_e;

typedef struct pgm_transport_t pgm_transport_t;

struct pgm_tsi_t {            /* transport session identifier */
    pgm_gsi_t   gsi;	    /* global session identifier */
    guint16     sport;	    /* source port: a random number to help detect session re-starts */
};

typedef struct pgm_tsi_t pgm_tsi_t;

#ifndef __PGM_MSGV_H__
#   include <pgm/msgv.h>
#endif

struct pgm_sqn_list_t {
    guint	    len;
    guint32         sqn[63];	/* list of sequence numbers */
};

typedef struct pgm_sqn_list_t pgm_sqn_list_t;

struct pgm_peer_t {
    gint		ref_count;		    /* atomic integer */

    pgm_tsi_t           tsi;
    struct sockaddr_storage	group_nla;
    struct sockaddr_storage	nla, local_nla;	    /* nla = advertised, local_nla = from packet */
    struct sockaddr_storage	redirect_nla;	    /* from dlr */
    pgm_time_t		spmr_expiry;

    GStaticMutex	mutex;

    gpointer            rxw;		/* pgm_rxw_t */
    pgm_transport_t*    transport;
    GList		link_;

    gpointer		rs;
    gboolean		use_proactive_parity;	    /* indicating availability from this source */
    gboolean		use_ondemand_parity;
    guint		rs_n;
    guint		rs_k;
    guint32		tg_sqn_shift;		    /* log2 (rs_k) */

    guint32		spm_sqn;
    pgm_time_t		expiry;

    pgm_time_t		last_packet;
    guint32		cumulative_stats[PGM_PC_RECEIVER_MAX];
    guint32		snap_stats[PGM_PC_RECEIVER_MAX];

    guint32		min_fail_time;
    guint32		max_fail_time;
};

typedef struct pgm_peer_t pgm_peer_t;

struct pgm_transport_t {
    pgm_tsi_t           tsi;
    guint16		dport;

    guint16		udp_encap_port;

    GStaticMutex	mutex;
    GThread		*timer_thread;
    GMainLoop		*timer_loop;
    GMainContext	*timer_context;
    guint               timer_id;
    gboolean		is_bound;
    gboolean		is_open;
    gboolean            has_lost_data;
    gboolean		will_close_on_failure;

    gboolean		can_send_data;			/* and SPMs */
    gboolean		can_send_nak;
    gboolean		can_recv;
    gboolean		is_edge_triggered_recv;

    GCond		*thread_cond;
    GMutex		*thread_mutex;

    struct group_source_req send_gsr;			/* multicast */
    struct sockaddr_storage send_addr;			/* unicast nla */
    GStaticMutex	send_mutex;
    int			send_sock;
    GStaticMutex	send_with_router_alert_mutex;
    int			send_with_router_alert_sock;
    struct group_source_req recv_gsr[IP_MAX_MEMBERSHIPS];	/* sa_family = 0 terminated */
    guint		recv_gsr_len;
    int			recv_sock;

    guint16		max_tpdu;
    guint16		max_tsdu;		    /* excluding optional varpkt_len word */
    guint16		max_tsdu_fragment;
    gsize		iphdr_len;
    gboolean		use_multicast_loop;    /* and reuseaddr for UDP encapsulation */
    guint		hops;
    guint		txw_preallocate, txw_sqns, txw_secs, txw_max_rte;
    guint		rxw_preallocate, rxw_sqns, rxw_secs, rxw_max_rte;
    int			sndbuf, rcvbuf;		    /* setsockopt (SO_SNDBUF/SO_RCVBUF) */

    GStaticRWLock	txw_lock;
    gpointer            txw;		   	    /* pgm_txw_t */
    gpointer		rate_control;		    /* rate_t */

    gboolean		is_apdu_eagain;		    /* writer-lock on txw_lock exists
						       as send would block */

    struct {
	guint		    data_pkt_offset;
	gsize		    data_bytes_offset;
	guint32		    first_sqn;
	gpointer	    pkt;
	gsize		    tsdu_length;
	gsize		    tpdu_length;
	guint32		    unfolded_odata;
	gsize		    apdu_length;
	guint		    vector_index;
	gsize		    vector_offset;
	gboolean	    is_rate_limited;
    } pkt_dontwait_state;

    guint32		spm_sqn;
    guint		spm_ambient_interval;	    /* microseconds */
    guint*		spm_heartbeat_interval;	    /* zero terminated, zero lead-pad */
    guint		spm_heartbeat_state;	    /* indexof spm_heartbeat_interval */
    gchar*		spm_packet;
    gsize		spm_len;

    guint		peer_expiry;		    /* from absence of SPMs */
    guint		spmr_expiry;		    /* waiting for peer SPMRs */

    GRand*		rand_;			    /* for calculating nak_rb_ivl from nak_bo_ivl */
    guint		nak_data_retries, nak_ncf_retries;
    guint		nak_bo_ivl, nak_rpt_ivl, nak_rdata_ivl;
    pgm_time_t		next_heartbeat_spm, next_ambient_spm;

    gpointer		rs;
    gboolean		use_proactive_parity;
    gboolean		use_ondemand_parity;
    gboolean		use_varpkt_len;
    guint		rs_n;
    guint		rs_k;
    guint		rs_proactive_h;		    /* 0 <= proactive-h <= ( n - k ) */
    guint		tg_sqn_shift;
    gpointer		parity_buffer;		    /* for parity odata/rdata generation */

    gpointer		rx_buffer;
    struct iovec*	piov;
    guint		piov_len;		    /* # elements in piov */

    GTrashStack*	rx_data;		    /* shared between all receivers for this instance */
    GTrashStack*	rx_packet;
    GStaticMutex	rx_mutex;

    GStaticRWLock	peers_lock;
    GHashTable*		peers_hashtable;	    /* fast lookup */
    GList*		peers_list;		    /* easy iteration */
    GSList*		peers_waiting;		    /* rxw: have or lost data */
    GSList*		peers_committed;	    /* rxw: waiting to free */
    GStaticMutex	waiting_mutex;
    pgm_notify_t	waiting_notify;		    /* timer to rx */
    gboolean		is_waiting_read;

    pgm_notify_t	rdata_notify;		    /* rx to timer */
    GIOChannel*		rdata_channel;
    guint		rdata_id;

    pgm_time_t		next_poll;
    pgm_notify_t	timer_notify;		    /* any to timer */
    GIOChannel*		notify_channel;
    guint               notify_id;
    pgm_notify_t	timer_shutdown;
    GIOChannel*		shutdown_channel;
    guint               shutdown_id;

    guint32		cumulative_stats[PGM_PC_SOURCE_MAX];
    guint32		snap_stats[PGM_PC_SOURCE_MAX];
    pgm_time_t		snap_time;
};


/* global variables */
extern GStaticRWLock pgm_transport_list_lock;
extern GSList* pgm_transport_list;


G_BEGIN_DECLS

int pgm_init (void);

gchar* pgm_print_tsi (const pgm_tsi_t*);
int pgm_print_tsi_r (const pgm_tsi_t*, char*, gsize);
guint pgm_tsi_hash (gconstpointer);
gboolean pgm_tsi_equal (gconstpointer, gconstpointer);
guint pgm_power2_log2 (guint);

void pgm_drop_superuser (void);

int pgm_transport_create (pgm_transport_t**, pgm_gsi_t*, guint16, guint16, struct group_source_req*, gsize, struct group_source_req*);
int pgm_transport_bind (pgm_transport_t*);
int pgm_transport_destroy (pgm_transport_t*, gboolean);

int pgm_transport_set_max_tpdu (pgm_transport_t*, guint16);
int pgm_transport_set_multicast_loop (pgm_transport_t*, gboolean);
int pgm_transport_set_hops (pgm_transport_t*, gint);
int pgm_transport_set_ambient_spm (pgm_transport_t*, guint);
int pgm_transport_set_heartbeat_spm (pgm_transport_t*, const guint*, int);

int pgm_transport_set_peer_expiry (pgm_transport_t*, guint);
int pgm_transport_set_spmr_expiry (pgm_transport_t*, guint);

int pgm_transport_set_txw_preallocate (pgm_transport_t*, guint);
int pgm_transport_set_txw_sqns (pgm_transport_t*, guint);
int pgm_transport_set_txw_secs (pgm_transport_t*, guint);
int pgm_transport_set_txw_max_rte (pgm_transport_t*, guint);

int pgm_transport_set_rxw_preallocate (pgm_transport_t*, guint);
int pgm_transport_set_rxw_sqns (pgm_transport_t*, guint);
int pgm_transport_set_rxw_secs (pgm_transport_t*, guint);
int pgm_transport_set_rxw_max_rte (pgm_transport_t*, guint);

int pgm_transport_set_sndbuf (pgm_transport_t*, int);
int pgm_transport_set_rcvbuf (pgm_transport_t*, int);

int pgm_transport_set_nak_bo_ivl (pgm_transport_t*, guint);
int pgm_transport_set_nak_rpt_ivl (pgm_transport_t*, guint);
int pgm_transport_set_nak_rdata_ivl (pgm_transport_t*, guint);
int pgm_transport_set_nak_data_retries (pgm_transport_t*, guint);
int pgm_transport_set_nak_ncf_retries (pgm_transport_t*, guint);

int pgm_transport_set_fec (pgm_transport_t*, guint, gboolean, gboolean, guint, guint);

int pgm_transport_set_send_only (pgm_transport_t*, gboolean);
int pgm_transport_set_recv_only (pgm_transport_t*, gboolean);

int pgm_transport_set_close_on_failure (pgm_transport_t*, gboolean);

gsize pgm_transport_pkt_offset (gboolean);
static inline gsize pgm_transport_max_tsdu (pgm_transport_t* transport, gboolean can_fragment)
{
    gsize max_tsdu = can_fragment ? transport->max_tsdu_fragment : transport->max_tsdu;
    if (transport->use_varpkt_len)
	max_tsdu -= sizeof (guint16);
    return max_tsdu;
}

int pgm_set_nonblocking (int filedes[2]);

gssize pgm_transport_send (pgm_transport_t*, gconstpointer, gsize, int);
gssize pgm_transport_sendv (pgm_transport_t*, const struct iovec*, guint, int, gboolean);
gssize pgm_transport_send_packetv (pgm_transport_t*, const struct iovec*, guint, int, gboolean);

gpointer pgm_packetv_alloc (pgm_transport_t*, gboolean);
void pgm_packetv_free1 (pgm_transport_t*, gpointer, gboolean);

/* receiver side */
gssize pgm_transport_recvmsg (pgm_transport_t*, pgm_msgv_t*, int);
gssize pgm_transport_recvmsgv (pgm_transport_t*, pgm_msgv_t*, gsize, int);
gssize pgm_transport_recv (pgm_transport_t*, gpointer, gsize, int);
gssize pgm_transport_recvfrom (pgm_transport_t*, gpointer, gsize, int, pgm_tsi_t*);

int pgm_transport_select_info (pgm_transport_t*, fd_set*, fd_set*, int*);
int pgm_transport_poll_info (pgm_transport_t*, struct pollfd*, int*, int);
#ifdef CONFIG_HAVE_EPOLL
int pgm_transport_epoll_ctl (pgm_transport_t*, int, int, int);
#endif

int pgm_transport_join_group (pgm_transport_t*, struct group_req*, gsize);
int pgm_transport_leave_group (pgm_transport_t*, struct group_req*, gsize);
int pgm_transport_block_source (pgm_transport_t*, struct group_source_req*, gsize);
int pgm_transport_unblock_source (pgm_transport_t*, struct group_source_req*, gsize);
int pgm_transport_join_source_group (pgm_transport_t*, struct group_source_req*, gsize);
int pgm_transport_leave_source_group (pgm_transport_t*, struct group_source_req*, gsize);
int pgm_transport_msfilter (pgm_transport_t*, struct group_filter*, gsize);

G_END_DECLS

#endif /* __PGM_TRANSPORT_H__ */
