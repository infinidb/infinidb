/* vim:ts=8:sts=4:sw=4:noai:noexpandtab
 * 
 * PGM packet formats, RFC 3208.
 *
 * Copyright (c) 2006 Miru Limited.
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

#ifndef __PGM_PACKET_H__
#define __PGM_PACKET_H__

#include <errno.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>

#include <glib.h>


#ifndef IPPROTO_PGM
#define IPPROTO_PGM 		    113
#endif

/* address family indicator, rfc 1700 (ADDRESS FAMILY NUMBERS) */
#ifndef AFI_IP
#define AFI_IP	    1	    /* IP (IP version 4) */
#define AFI_IP6	    2	    /* IP6 (IP version 6) */
#endif

/*
 * Udp port for UDP encapsulation
 */
#define DEFAULT_UDP_ENCAP_UCAST_PORT 3055
#define DEFAULT_UDP_ENCAP_MCAST_PORT 3056

enum pgm_type_e {
    PGM_SPM = 0x00,	/* 8.1: source path message */
    PGM_POLL = 0x01,	/* 14.7.1: poll request */
    PGM_POLR = 0x02,	/* 14.7.2: poll response */
    PGM_ODATA = 0x04,	/* 8.2: original data */
    PGM_RDATA = 0x05,	/* 8.2: repair data */
    PGM_NAK = 0x08,	/* 8.3: NAK or negative acknowledgement */
    PGM_NNAK = 0x09,	/* 8.3: N-NAK or null negative acknowledgement */
    PGM_NCF = 0x0a,	/* 8.3: NCF or NAK confirmation */
    PGM_SPMR = 0x0c,	/* 13.6: SPM request */
    PGM_MAX = 0xff
};

#define PGM_OPT_LENGTH		    0x00	/* options length */
#define PGM_OPT_FRAGMENT	    0x01	/* fragmentation */
#define PGM_OPT_NAK_LIST	    0x02	/* list of nak entries */
#define PGM_OPT_JOIN		    0x03	/* late joining */
#define PGM_OPT_REDIRECT	    0x07	/* redirect */
#define PGM_OPT_SYN		    0x0d	/* synchronisation */
#define PGM_OPT_FIN		    0x0e	/* session end */
#define PGM_OPT_RST		    0x0f	/* session reset */

#define PGM_OPT_PARITY_PRM	    0x08	/* forward error correction parameters */
#define PGM_OPT_PARITY_GRP	    0x09	/*   group number */
#define PGM_OPT_CURR_TGSIZE	    0x0a	/*   group size */

#define PGM_OPT_CR		    0x10	/* congestion report */
#define PGM_OPT_CRQST		    0x11	/* congestion report request */

#define PGM_OPT_NAK_BO_IVL	    0x04	/* nak back-off interval */
#define PGM_OPT_NAK_BO_RNG	    0x05	/* nak back-off range */
#define PGM_OPT_NBR_UNREACH	    0x0b	/* neighbour unreachable */
#define PGM_OPT_PATH_NLA	    0x0c	/* path nla */

#define PGM_OPT_INVALID		    0x7f	/* option invalidated */

/* byte alignment for packet memory maps */
#pragma pack(push, 1)

/* 8. PGM header */
struct pgm_header {
    guint16	pgm_sport;		/* source port: tsi::sport or UDP port depending on direction */
    guint16	pgm_dport;		/* destination port */
    guint8	pgm_type;		/* version / packet type */
    guint8	pgm_options;		/* options */
#define PGM_OPT_PARITY		0x80	/* parity packet */
#define PGM_OPT_VAR_PKTLEN	0x40	/* + variable sized packets */
#define PGM_OPT_NETWORK		0x02    /* network-significant: must be interpreted by network elements */
#define PGM_OPT_PRESENT		0x01	/* option extension are present */
    guint16	pgm_checksum;		/* checksum */
    guint8	pgm_gsi[6];		/* global source id */
    guint16	pgm_tsdu_length;	/* tsdu length */
				/* tpdu length = th length (header + options) + tsdu length */
};

/* 8.1.  Source Path Messages (SPM) */
struct pgm_spm {
    guint32	spm_sqn;		/* spm sequence number */
    guint32	spm_trail;		/* trailing edge sequence number */
    guint32	spm_lead;		/* leading edge sequence number */
    guint16	spm_nla_afi;		/* nla afi */
    guint16	spm_reserved;		/* reserved */
    struct in_addr spm_nla;		/* path nla */
    /* ... option extensions */
};

struct pgm_spm6 {
    guint32	spm6_sqn;		/* spm sequence number */
    guint32	spm6_trail;		/* trailing edge sequence number */
    guint32	spm6_lead;		/* leading edge sequence number */
    guint16	spm6_nla_afi;		/* nla afi */
    guint16	spm6_reserved;		/* reserved */
    struct in6_addr spm6_nla;		/* path nla */
    /* ... option extensions */
};

/* 8.2.  Data Packet */
struct pgm_data {
    guint32	data_sqn;		/* data packet sequence number */
    guint32	data_trail;		/* trailing edge sequence number */
    /* ... option extensions */
    /* ... data */
};

/* 8.3.  Negative Acknowledgments and Confirmations (NAK, N-NAK, & NCF) */
struct pgm_nak {
    guint32	nak_sqn;		/* requested sequence number */
    guint16	nak_src_nla_afi;	/* nla afi */
    guint16	nak_reserved;		/* reserved */
    struct in_addr nak_src_nla;		/* source nla */
    guint16	nak_grp_nla_afi;	/* nla afi */
    guint16	nak_reserved2;		/* reserved */
    struct in_addr nak_grp_nla;		/* multicast group nla */
    /* ... option extension */
};

struct pgm_nak6 {
    guint32	nak6_sqn;		/* requested sequence number */
    guint16	nak6_src_nla_afi;	/* nla afi */
    guint16	nak6_reserved;		/* reserved */
    struct in6_addr nak6_src_nla;	/* source nla */
    guint16	nak6_grp_nla_afi;	/* nla afi */
    guint16	nak6_reserved2;		/* reserved */
    struct in6_addr nak6_grp_nla;	/* multicast group nla */
    /* ... option extension */
};

/* 9.  Option header (max 16 per packet) */
struct pgm_opt_header {
    guint8	opt_type;		/* option type */
#define PGM_OPT_MASK	0x7f
#define PGM_OPT_END	0x80		/* end of options flag */
    guint8	opt_length;		/* option length */
    guint8	opt_reserved;
#define PGM_OP_ENCODED		0x8	/* F-bit */
#define PGM_OPX_MASK		0x3
#define PGM_OPX_IGNORE		0x0	/* extensibility bits */
#define PGM_OPX_INVALIDATE	0x1
#define PGM_OPX_DISCARD		0x2
#define PGM_OP_ENCODED_NULL	0x80	/* U-bit */
};

/* 9.1.  Option extension length - OPT_LENGTH */
struct pgm_opt_length {
    guint8	opt_type;		/* include header as total length overwrites reserved/OPX bits */
    guint8	opt_length;
    guint16	opt_total_length;	/* total length of all options */
};

/* 9.2.  Option fragment - OPT_FRAGMENT */
struct pgm_opt_fragment {
    guint8	opt_reserved;		/* reserved */
    guint32	opt_sqn;		/* first sequence number */
    guint32	opt_frag_off;		/* offset */
    guint32	opt_frag_len;		/* length */
};

/* 9.3.5.  Option NAK List - OPT_NAK_LIST
 *
 * GNU C allows opt_sqn[0], ISO C89 requireqs opt_sqn[1], ISO C99 permits opt_sqn[]
 */
struct pgm_opt_nak_list {
    guint8	opt_reserved;		/* reserved */
    guint32	opt_sqn[];		/* requested sequence number [62] */
};

/* 9.4.2.  Option Join - OPT_JOIN */
struct pgm_opt_join {
    guint8	opt_reserved;		/* reserved */
    guint32	opt_join_min;		/* minimum sequence number */
};

/* 9.5.5.  Option Redirect - OPT_REDIRECT */
struct pgm_opt_redirect {
    guint8	opt_reserved;		/* reserved */
    guint16	opt_nla_afi;		/* nla afi */
    guint16	opt_reserved2;		/* reserved */
    struct in_addr opt_nla;		/* dlr nla */
};

struct pgm_opt6_redirect {
    guint8	opt6_reserved;		/* reserved */
    guint16	opt6_nla_afi;		/* nla afi */
    guint16	opt6_reserved2;		/* reserved */
    struct in6_addr opt6_nla;		/* dlr nla */
};

/* 9.6.2.  Option Sources - OPT_SYN */
struct pgm_opt_syn {
    guint8	opt_reserved;		/* reserved */
};

/* 9.7.4.  Option End Session - OPT_FIN */
struct pgm_opt_fin {
    guint8	opt_reserved;		/* reserved */
};

/* 9.8.4.  Option Reset - OPT_RST */
struct pgm_opt_rst {
    guint8	opt_reserved;		/* reserved */
};


/*
 * Forward Error Correction - FEC
 */

/* 11.8.1.  Option Parity - OPT_PARITY_PRM */
struct pgm_opt_parity_prm {
    guint8	opt_reserved;		/* reserved */
#define PGM_PARITY_PRM_MASK 0x3
#define PGM_PARITY_PRM_PRO  0x1		/* source provides pro-active parity packets */
#define PGM_PARITY_PRM_OND  0x2		/*                 on-demand parity packets */
    guint32	parity_prm_tgs;		/* transmission group size */
};

/* 11.8.2.  Option Parity Group - OPT_PARITY_GRP */
struct pgm_opt_parity_grp {
    guint8	opt_reserved;		/* reserved */
    guint32	prm_group;		/* parity group number */
};

/* 11.8.3.  Option Current Transmission Group Size - OPT_CURR_TGSIZE */
struct pgm_opt_curr_tgsize {
    guint8	opt_reserved;		/* reserved */
    guint32	prm_atgsize;		/* actual transmission group size */
};

/*
 * Congestion Control
 */

/* 12.7.1.  Option Congestion Report - OPT_CR */
struct pgm_opt_cr {
    guint8	opt_reserved;		/* reserved */
    guint32	opt_cr_lead;		/* congestion report reference sqn */
    guint16	opt_cr_ne_wl;		/* ne worst link */
    guint16	opt_cr_ne_wp;		/* ne worst path */
    guint16	opt_cr_rx_wp;		/* rcvr worst path */
    guint16	opt_reserved2;		/* reserved */
    guint16	opt_nla_afi;		/* nla afi */
    guint16	opt_reserved3;		/* reserved */
    guint32	opt_cr_rcvr;		/* worst receivers nla */
};

/* 12.7.2.  Option Congestion Report Request - OPT_CRQST */
struct pgm_opt_crqst {
    guint8	opt_reserved;		/* reserved */
};


/*
 * SPM Requests
 */

/* 13.6.  SPM Requests */
#if 0
struct pgm_spmr {
    /* ... option extensions */
};
#endif


/*
 * Poll Mechanism
 */

/* 14.7.1.  Poll Request */
struct pgm_poll {
    guint32	poll_sqn;		/* poll sequence number */
    guint16	poll_round;		/* poll round */
    guint16	poll_s_type;		/* poll sub-type */
    guint16	poll_nla_afi;		/* nla afi */
    guint16	poll_reserved;		/* reserved */
    struct in_addr poll_nla;		/* path nla */
    guint32	poll_bo_ivl;		/* poll back-off interval */
    gchar	poll_rand[4];		/* random string */
    guint32	poll_mask;		/* matching bit-mask */
    /* ... option extensions */
};

struct pgm_poll6 {
    guint32	poll6_sqn;		/* poll sequence number */
    guint16	poll6_round;		/* poll round */
    guint16	poll6_s_type;		/* poll sub-type */
    guint16	poll6_nla_afi;		/* nla afi */
    guint16	poll6_reserved;		/* reserved */
    struct in6_addr poll6_nla;		/* path nla */
    guint32	poll6_bo_ivl;		/* poll back-off interval */
    gchar	poll6_rand[4];		/* random string */
    guint32	poll6_mask;		/* matching bit-mask */
    /* ... option extensions */
};

/* 14.7.2.  Poll Response */
struct pgm_polr {
    guint32	polr_sqn;		/* polr sequence number */
    guint16	polr_round;		/* polr round */
    guint16	polr_reserved;		/* reserved */
    /* ... option extensions */
};


/*
 * Implosion Prevention
 */

/* 15.4.1.  Option NAK Back-Off Interval - OPT_NAK_BO_IVL */
struct pgm_opt_nak_bo_ivl {
    guint8	opt_reserved;		/* reserved */
    guint32	opt_nak_bo_ivl;		/* nak back-off interval */
    guint32	opt_nak_bo_ivl_sqn;	/* nak back-off interval sqn */
};

/* 15.4.2.  Option NAK Back-Off Range - OPT_NAK_BO_RNG */
struct pgm_opt_nak_bo_rng {
    guint8	opt_reserved;		/* reserved */
    guint32	opt_nak_max_bo_ivl;	/* maximum nak back-off interval */
    guint32	opt_nak_min_bo_ivl;	/* minimum nak back-off interval */
};

/* 15.4.3.  Option Neighbour Unreachable - OPT_NBR_UNREACH */
struct pgm_opt_nbr_unreach {
    guint8	opt_reserved;		/* reserved */
};

/* 15.4.4.  Option Path - OPT_PATH_NLA */
struct pgm_opt_path_nla {
    guint8	opt_reserved;		/* reserved */
    struct in_addr opt_path_nla;	/* path nla */
};

struct pgm_opt6_path_nla {
    guint8	opt6_reserved;		/* reserved */
    struct in6_addr opt6_path_nla;	/* path nla */
};

#pragma pack(pop)


G_BEGIN_DECLS

int pgm_parse (struct pgm_header*, gsize, struct pgm_header**, gpointer*, gsize*);
int pgm_parse_raw (gpointer, gsize, struct sockaddr*, socklen_t*, struct pgm_header**, gpointer*, gsize*);
int pgm_parse_udp_encap (gpointer, gsize, struct sockaddr*, socklen_t*, struct pgm_header**, gpointer*, gsize*);
gboolean pgm_print_packet (gpointer, gsize);

static inline gboolean pgm_is_upstream (guint8 type)
{
    return (type == PGM_NAK || type == PGM_SPMR || type == PGM_POLR);
}

static inline gboolean pgm_is_peer (guint8 type)
{
    return (type == PGM_SPMR);
}

static inline gboolean pgm_is_downstream (guint8 type)
{
    return (type == PGM_SPM || type == PGM_ODATA || type == PGM_RDATA || type == PGM_POLL || type == PGM_NCF);
}

int pgm_verify_spm (struct pgm_header*, gpointer, gsize);
int pgm_verify_spmr (struct pgm_header*, gpointer, gsize);
int pgm_verify_nak (struct pgm_header*, gpointer, gsize);
int pgm_verify_nnak (struct pgm_header*, gpointer, gsize);
int pgm_verify_ncf (struct pgm_header*, gpointer, gsize);

static inline int pgm_nla_to_sockaddr (gconstpointer nla, struct sockaddr* sa)
{
    int retval = 0;

    sa->sa_family = g_ntohs (*(const guint16*)nla);
    switch (sa->sa_family) {
    case AFI_IP:
	sa->sa_family = AF_INET;
	((struct sockaddr_in*)sa)->sin_addr.s_addr = ((const struct in_addr*)((const guint8*)nla + sizeof(guint32)))->s_addr;
	break;

    case AFI_IP6:
	sa->sa_family = AF_INET6;
	memcpy (&((struct sockaddr_in6*)sa)->sin6_addr, (const struct in6_addr*)((const guint8*)nla + sizeof(guint32)), sizeof(struct in6_addr));
	break;

    default:
	retval = -EINVAL;
	break;
    }

    return retval;
}

static inline int pgm_sockaddr_to_nla (const struct sockaddr* sa, gpointer nla)
{
    int retval = 0;

    *(guint16*)nla = sa->sa_family;
    *(guint16*)((guint8*)nla + sizeof(guint16)) = 0;	/* reserved 16bit space */
    switch (sa->sa_family) {
    case AF_INET:
	*(guint16*)nla = g_htons (AFI_IP);
	((struct in_addr*)((guint8*)nla + sizeof(guint32)))->s_addr = ((const struct sockaddr_in*)sa)->sin_addr.s_addr;
	break;

    case AF_INET6:
	*(guint16*)nla = g_htons (AFI_IP6);
	memcpy ((struct in6_addr*)((guint8*)nla + sizeof(guint32)), &((const struct sockaddr_in6*)sa)->sin6_addr, sizeof(struct in6_addr));
	break;

    default:
	retval = -EINVAL;
	break;
    }

    return retval;
}

const char* pgm_type_string (guint8);
const char* pgm_udpport_string (int);
const char* pgm_gethostbyaddr (const struct in_addr*);
void pgm_ipopt_print (gconstpointer, gsize);

G_END_DECLS

#endif /* __PGM_PACKET_H__ */
