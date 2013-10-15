/* vim:ts=8:sts=8:sw=4:noai:noexpandtab
 *
 * PGM packet formats, RFC 3208.
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

#include <ctype.h>
#include <errno.h>
#include <netdb.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <arpa/inet.h>

#include <glib.h>

#include "pgm/packet.h"
#include "pgm/checksum.h"


/* globals */

static gboolean pgm_print_spm (struct pgm_header*, gpointer, gsize);
static gboolean pgm_print_poll (struct pgm_header*, gpointer, gsize);
static gboolean pgm_print_polr (struct pgm_header*, gpointer, gsize);
static gboolean pgm_print_odata (struct pgm_header*, gpointer, gsize);
static gboolean pgm_print_rdata (struct pgm_header*, gpointer, gsize);
static gboolean pgm_print_nak (struct pgm_header*, gpointer, gsize);
static gboolean pgm_print_nnak (struct pgm_header*, gpointer, gsize);
static gboolean pgm_print_ncf (struct pgm_header*, gpointer, gsize);
static gboolean pgm_print_spmr (struct pgm_header*, gpointer, gsize);
static gssize pgm_print_options (gpointer, gsize);


int
pgm_parse_raw (
	gpointer		data,			/* packet to parse */
	gsize			len,
	struct sockaddr*	dst_addr,
	socklen_t*		dst_addr_len,
	struct pgm_header**	header,			/* return PGM header location */
	gpointer*		packet,			/* and pointer to PGM packet type header */
	gsize*			packet_len
	)
{
/* minimum size should be IP header plus PGM header */
#ifdef __USE_BSD
	if (len < (sizeof(struct ip) + sizeof(struct pgm_header))) 
	{
		printf ("Packet size too small: %" G_GSIZE_FORMAT " bytes, expecting at least %" G_GSIZE_FORMAT " bytes.\n",
			len, (sizeof(struct ip) + sizeof(struct pgm_header)));
		return -1;
	}
#else
	if (len < (sizeof(struct iphdr) + sizeof(struct pgm_header))) 
	{
		printf ("Packet size too small: %" G_GSIZE_FORMAT " bytes, expecting at least %" G_GSIZE_FORMAT " bytes.\n",
			len, (sizeof(struct iphdr) + sizeof(struct pgm_header)));
		return -1;
	}
#endif

/* IP packet header: IPv4
 *
 *  0                   1                   2                   3
 *  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |Version|  HL   |      ToS      |            Length             |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |           Fragment ID         |R|D|M|     Fragment Offset     |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |      TTL      |    Protocol   |           Checksum            |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                       Source IP Address                       |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                    Destination IP Address                     |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * | IP Options when present ...
 * +-+-+-+-+-+-+-+-+-+-+-+-+ ...
 * | Data ...
 * +-+-+- ...
 *
 * IPv6
 *
 *  0                   1                   2                   3
 *  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |Version| Traffic Class |             Flow Label                |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |           Payload Length      |   Next Header |   Hop Limit   |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                                                               |
 * |                       Source IP Address                       |
 * |                                                               |
 * |                                                               |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                                                               |
 * |                     Destination IP Address                    |
 * |                                                               |
 * |                                                               |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * | IP Options when present ...
 * +-+-+-+-+-+-+-+-+-+-+-+-+ ...
 * | Data ...
 * +-+-+- ...
 *
 */

/* decode IP header */
#ifdef __USE_BSD
	const struct ip* ip = (struct ip*)data;
	switch (ip->ip_v)
#else
	const struct iphdr* ip = (struct iphdr*)data;
	switch (ip->version)
#endif
	{
	case 4:
		((struct sockaddr_in*)dst_addr)->sin_family = AF_INET;
#ifdef __USE_BSD
		((struct sockaddr_in*)dst_addr)->sin_addr.s_addr = ip->ip_dst.s_addr;
#else
		((struct sockaddr_in*)dst_addr)->sin_addr.s_addr = ip->daddr;
#endif
		*dst_addr_len = sizeof(struct sockaddr_in);
		break;

	case 6:
		((struct sockaddr_in6*)dst_addr)->sin6_family = AF_INET6;
		g_warning ("IPv6 packet headers are not provided by PF_PACKET capture.");
		*dst_addr_len = sizeof(struct sockaddr_in6);
		return -1;

	default:
#ifdef __USE_BSD
		printf ("unknown IP version (%i) :/\n", ip->ip_v);
#else
		printf ("unknown IP version (%i) :/\n", ip->version);	
#endif
		return -1;
	}

#ifdef __USE_BSD
	gsize ip_header_length = ip->ip_hl * 4;		/* IP header length in 32bit octets */
	if (ip_header_length < sizeof(struct ip))
#else
	gsize ip_header_length = ip->ihl * 4;		/* IP header length in 32bit octets */
	if (ip_header_length < sizeof(struct iphdr))
#endif
	{
		puts ("bad IP header length :(");
		return -1;
	}

#ifdef __USE_BSD
	gsize packet_length = g_ntohs(ip->ip_len);	/* total packet length */
#else
	gsize packet_length = g_ntohs(ip->tot_len);	/* total packet length */
#endif

/* ip_len can equal packet_length - ip_header_length in FreeBSD/NetBSD
 * Stevens/Fenner/Rudolph, Unix Network Programming Vol.1, p.739 
 * 
 * RFC3828 allows partial packets such that len < packet_length with UDP lite
 */
	if (len == packet_length + ip_header_length) {
		packet_length += ip_header_length;
	}

	if (len < packet_length) {			/* redundant: often handled in kernel */
		printf ("truncated IP packet: %i < %i\n", (int)len, (int)packet_length);
		return -1;
	}

/* TCP Segmentation Offload (TSO) might have zero length here */
	if (packet_length < ip_header_length) {
		printf ("bad length: %i < %i\n", (int)packet_length, (int)ip_header_length);
		return -1;
	}

/* packets that fail checksum will generally not be passed upstream except with rfc3828
 */
#if PGM_CHECK_IN_CKSUM
	int sum = in_cksum(data, packet_length, 0);
	if (sum != 0) {
#ifdef __USE_BSD
		int ip_sum = g_ntohs(ip->ip_sum);
#else
		int ip_sum = g_ntohs(ip->check);
#endif
		printf ("bad cksum! %i\n", ip_sum);
		return -2;
	}
#endif

/* fragmentation offset, bit 0: 0, bit 1: do-not-fragment, bit 2: more-fragments */
#ifdef __USE_BSD
	guint offset = g_ntohs(ip->ip_off);
#else
	guint offset = g_ntohs(ip->frag_off);
#endif
	if ((offset & 0x1fff) != 0) {
		puts ("fragmented packet :/");
		return -1;
	}

/* PGM payload, header looks as follows:
 *
 *  0                   1                   2                   3
 *  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |         Source Port           |       Destination Port        |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |      Type     |    Options    |           Checksum            |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                        Global Source ID                   ... |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * | ...    Global Source ID       |           TSDU Length         |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * | Type specific data ...
 * +-+-+-+-+-+-+-+-+-+- ...
 */
	struct pgm_header* pgm_header = (struct pgm_header*)((guint8*)data + ip_header_length);
	gsize pgm_length = packet_length - ip_header_length;

	return pgm_parse (pgm_header, pgm_length, header, packet, packet_len);
}

int
pgm_parse_udp_encap (
	gpointer		data,
	gsize			len,
	G_GNUC_UNUSED struct sockaddr*	dst_addr,
	G_GNUC_UNUSED socklen_t*	dst_addr_len,
	struct pgm_header**	header,
	gpointer*		packet,
	gsize*			packet_len
	)
{
	return pgm_parse ((struct pgm_header*)data, len, header, packet, packet_len);
}

/* will modify packet contents to calculate and check PGM checksum
 */
int
pgm_parse (
	struct pgm_header*	pgm_header,
	gsize			pgm_length,
	struct pgm_header**	header,
	gpointer*		packet,
	gsize*			packet_len
	)
{
	if (pgm_length < sizeof(pgm_header)) {
		puts ("bad packet size :(");
		return -1;
	}

/* pgm_checksum == 0 means no transmitted checksum */
	if (pgm_header->pgm_checksum)
	{
		int sum = pgm_header->pgm_checksum;
		pgm_header->pgm_checksum = 0;
		int pgm_sum = pgm_csum_fold (pgm_csum_partial((const char*)pgm_header, pgm_length, 0));
		pgm_header->pgm_checksum = sum;
		if (pgm_sum != sum) {
			printf ("PGM checksum incorrect, packet %x calculated %x  :(\n", sum, pgm_sum);
			return -2;
		}
	} else {
		if (pgm_header->pgm_type == PGM_ODATA || pgm_header->pgm_type == PGM_RDATA) {
			puts ("PGM checksum mandatory for ODATA and RDATA packets :(");
			return -1;
		}
		puts ("No PGM checksum :O");
	}

/* now decode PGM packet types */
	gpointer pgm_data = pgm_header + 1;
	gssize pgm_data_length = (gssize)pgm_length - sizeof(pgm_header);		/* can equal zero for SPMR's */

	if (pgm_data_length < 0) {
		puts ("bad packet length :(");
		return -1;
	}

	*header = pgm_header;
	*packet = pgm_data;
	*packet_len = (gsize)pgm_data_length;

	return 0;
}

gboolean
pgm_print_packet (
	gpointer	data,
	gsize		len
	)
{
/* minimum size should be IP header plus PGM header */
#ifdef __USE_BSD
	if (len < (sizeof(struct ip) + sizeof(struct pgm_header))) 
#else
	if (len < (sizeof(struct iphdr) + sizeof(struct pgm_header))) 
#endif
	{
		printf ("Packet size too small: %" G_GSIZE_FORMAT " bytes, expecting at least %" G_GSIZE_FORMAT " bytes.\n", len, sizeof(struct pgm_header));
		return FALSE;
	}

/* decode IP header */
#ifdef __USE_BSD
	const struct ip* ip = (struct ip*)data;
	if (ip->ip_v != 4) 				/* IP version, 4 or 6 */
#else
	const struct iphdr* ip = (struct iphdr*)data;
	if (ip->version != 4) 				/* IP version, 4 or 6 */
#endif
	{
		puts ("not IP4 packet :/");		/* v6 not currently handled */
		return FALSE;
	}
	printf ("IP ");

#ifdef __USE_BSD
	gsize ip_header_length = ip->ip_hl * 4;		/* IP header length in 32bit octets */
	if (ip_header_length < sizeof(struct ip)) 
#else
	gsize ip_header_length = ip->ihl * 4;		/* IP header length in 32bit octets */
	if (ip_header_length < sizeof(struct iphdr)) 
#endif
	{
		puts ("bad IP header length :(");
		return FALSE;
	}

#ifdef __USE_BSD
	gsize packet_length = g_ntohs(ip->ip_len);	/* total packet length */
#else
	gsize packet_length = g_ntohs(ip->tot_len);	/* total packet length */
#endif

/* ip_len can equal packet_length - ip_header_length in FreeBSD/NetBSD
 * Stevens/Fenner/Rudolph, Unix Network Programming Vol.1, p.739 
 * 
 * RFC3828 allows partial packets such that len < packet_length with UDP lite
 */
	if (len == packet_length + ip_header_length) {
		packet_length += ip_header_length;
	}

	if (len < packet_length) {				/* redundant: often handled in kernel */
		puts ("truncated IP packet");
		return FALSE;
	}

/* TCP Segmentation Offload (TSO) might have zero length here */
	if (packet_length < ip_header_length) {
		puts ("bad length :(");
		return FALSE;
	}

#ifdef __USE_BSD
	guint offset = g_ntohs(ip->ip_off);
#else
	guint offset = g_ntohs(ip->frag_off);
#endif

/* 3 bits routing priority, 4 bits type of service: delay, throughput, reliability, cost */
#ifdef __USE_BSD
	printf ("(tos 0x%x", (int)ip->ip_tos);
	switch (ip->ip_tos & 0x3)
#else
	printf ("(tos 0x%x", (int)ip->tos);
	switch (ip->tos & 0x3)
#endif
	{
	case 1: printf (",ECT(1)"); break;
	case 2: printf (",ECT(0)"); break;
	case 3: printf (",CE"); break;
	default: break;
	}

/* time to live */
#ifdef __USE_BSD
	if (ip->ip_ttl >= 1) printf (", ttl %u", ip->ip_ttl);
#else
	if (ip->ttl >= 1) printf (", ttl %u", ip->ttl);
#endif

/* fragmentation */
#define IP_RDF	0x8000
#define IP_DF	0x4000
#define IP_MF	0x2000
#define IP_OFFMASK	0x1fff

	printf (", id %u, offset %u, flags [%s%s]",
#ifdef __USE_BSD
		g_ntohs(ip->ip_id),
#else
		g_ntohs(ip->id),
#endif
		(offset & 0x1fff) * 8,
		((offset & IP_DF) ? "DF" : ""),
		((offset & IP_MF) ? "+" : ""));
	printf (", length %" G_GSIZE_FORMAT, packet_length);

/* IP options */
#ifdef __USE_BSD
	if ((ip_header_length - sizeof(struct ip)) > 0) {
		printf (", options (");
		pgm_ipopt_print((gconstpointer)(ip + 1), ip_header_length - sizeof(struct ip));
		printf (" )");
	}
#else
	if ((ip_header_length - sizeof(struct iphdr)) > 0) {
		printf (", options (");
		pgm_ipopt_print((gconstpointer)(ip + 1), ip_header_length - sizeof(struct iphdr));
		printf (" )");
	}
#endif

/* packets that fail checksum will generally not be passed upstream except with rfc3828
 */
	int sum = pgm_inet_checksum(data, packet_length, 0);
	if (sum != 0) {
#ifdef __USE_BSD
		int ip_sum = g_ntohs(ip->ip_sum);
#else
		int ip_sum = g_ntohs(ip->check);
#endif
		printf (", bad cksum! %i", ip_sum);
	}

	printf (") ");

/* fragmentation offset, bit 0: 0, bit 1: do-not-fragment, bit 2: more-fragments */
	if ((offset & 0x1fff) != 0) {
		puts ("fragmented packet :/");
		return FALSE;
	}

/* PGM payload, header looks as follows:
 *
 *  0                   1                   2                   3
 *  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |         Source Port           |       Destination Port        |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |      Type     |    Options    |           Checksum            |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                        Global Source ID                   ... |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * | ...    Global Source ID       |           TSDU Length         |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * | Type specific data ...
 * +-+-+-+-+-+-+-+-+-+- ...
 */
	struct pgm_header* pgm_header = (struct pgm_header*)((guint8*)data + ip_header_length);
	gsize pgm_length = packet_length - ip_header_length;

	if (pgm_length < sizeof(pgm_header)) {
		puts ("bad packet size :(");
		return FALSE;
	}

	printf ("%s.%s > %s.%s: PGM\n",
#ifdef __USE_BSD
		pgm_gethostbyaddr((const struct in_addr*)&ip->ip_src), pgm_udpport_string(pgm_header->pgm_sport),
		pgm_gethostbyaddr((const struct in_addr*)&ip->ip_dst), pgm_udpport_string(pgm_header->pgm_dport));
#else
		pgm_gethostbyaddr((const struct in_addr*)&ip->saddr), pgm_udpport_string(pgm_header->pgm_sport),
		pgm_gethostbyaddr((const struct in_addr*)&ip->daddr), pgm_udpport_string(pgm_header->pgm_dport));
#endif

	printf ("type: %s [%i] (version=%i, reserved=%i)\n"
		"options: extensions=%s, network-significant=%s, parity packet=%s (variable size=%s)\n"
		"global source id: %i.%i.%i.%i.%i.%i\n"
		"tsdu length: %i\n",

		/* packet type */		/* packet version */			/* reserved = 0x0 */
		pgm_type_string(pgm_header->pgm_type & 0xf),
		(pgm_header->pgm_type & 0xf),	((pgm_header->pgm_type & 0xc0) >> 6),	((pgm_header->pgm_type & 0x30) >> 4),

/* bit 0 set => one or more option extensions are present */
		((pgm_header->pgm_options & (0x1 << 7)) ? "true" : "false"),
/* bit 1 set => one or more options are network-significant */
			((pgm_header->pgm_options & (0x1 << 6)) ? "true" : "false"),
/* bit 7 set => parity packet (OPT_PARITY) */
			((pgm_header->pgm_options & (0x1 << 0)) ? "true" : "false"),
/* bit 6 set => parity packet for variable packet sizes  (OPT_VAR_PKTLEN) */
			((pgm_header->pgm_options & (0x1 << 1)) ? "true" : "false"),

		pgm_header->pgm_gsi[0], pgm_header->pgm_gsi[1], pgm_header->pgm_gsi[2], pgm_header->pgm_gsi[3], pgm_header->pgm_gsi[4], pgm_header->pgm_gsi[5],
		g_ntohs(pgm_header->pgm_tsdu_length));

	if (pgm_header->pgm_checksum)
	{
		sum = pgm_header->pgm_checksum;
		pgm_header->pgm_checksum = 0;
		int pgm_sum = pgm_csum_fold (pgm_csum_partial((const char*)pgm_header, pgm_length, 0));
		if (pgm_sum != sum) {
			printf ("PGM checksum incorrect, packet %x calculated %x  :(\n", sum, pgm_sum);
			return FALSE;
		}
	} else {
		puts ("No PGM checksum :O");
	}

/* now decode PGM packet types */
	gpointer pgm_data = pgm_header + 1;
	gssize pgm_data_length = (gssize)pgm_length - sizeof(pgm_header);		/* can equal zero for SPMR's */

	if (pgm_data_length < 0) {
		puts ("bad packet length :(");
		return FALSE;
	}

	gboolean err = FALSE;
	switch (pgm_header->pgm_type) {
	case PGM_SPM:	err = pgm_print_spm (pgm_header, pgm_data, (gsize)pgm_data_length); break;
	case PGM_POLL:	err = pgm_print_poll (pgm_header, pgm_data, (gsize)pgm_data_length); break;
	case PGM_POLR:	err = pgm_print_polr (pgm_header, pgm_data, (gsize)pgm_data_length); break;
	case PGM_ODATA:	err = pgm_print_odata (pgm_header, pgm_data, (gsize)pgm_data_length); break;
	case PGM_RDATA:	err = pgm_print_rdata (pgm_header, pgm_data, (gsize)pgm_data_length); break;
	case PGM_NAK:	err = pgm_print_nak (pgm_header, pgm_data, (gsize)pgm_data_length); break;
	case PGM_NNAK:	err = pgm_print_nnak (pgm_header, pgm_data, (gsize)pgm_data_length); break;
	case PGM_NCF:	err = pgm_print_ncf (pgm_header, pgm_data, (gsize)pgm_data_length); break;
	case PGM_SPMR:	err = pgm_print_spmr (pgm_header, pgm_data, (gsize)pgm_data_length); break;
	default:	puts ("unknown packet type :("); break;
	}

	return err;
}

/* 8.1.  Source Path Messages (SPM)
 *
 *  0                   1                   2                   3
 *  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                     SPM's Sequence Number                     |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                 Trailing Edge Sequence Number                 |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                 Leading Edge Sequence Number                  |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |            NLA AFI            |          Reserved             |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                            Path NLA                     ...   |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-...-+-+
 * | Option Extensions when present ...                            |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+- ... -+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *
 * NLA = Network Layer Address
 * NLA AFI = NLA Address Family Indicator: rfc 1700 (ADDRESS FAMILY NUMBERS)
 * => Path NLA = IP address of last network element
 */

#define PGM_MIN_SPM_SIZE	( sizeof(struct pgm_spm) )

int
pgm_verify_spm (
	G_GNUC_UNUSED struct pgm_header* header,
	gpointer		data,
	gsize			len
	)
{
	int retval = 0;

/* truncated packet */
	if (len < PGM_MIN_SPM_SIZE) {
		retval = -EINVAL;
		goto out;
	}

	struct pgm_spm* spm = (struct pgm_spm*)data;

	switch (g_ntohs(spm->spm_nla_afi)) {
	case AFI_IP6:
		if (len < sizeof(struct pgm_spm6)) {
			retval = -EINVAL;
		}

	case AFI_IP:
		break;

	default:
		retval = -EINVAL;
		break;
	}

out:
	return retval;
}

static gboolean
pgm_print_spm (
	struct pgm_header*	header,
	gpointer		data,
	gsize			len
	)
{
	printf ("SPM: ");

	if (len < PGM_MIN_SPM_SIZE) {
		puts ("packet truncated :(");
		return FALSE;
	}

	struct pgm_spm* spm = (struct pgm_spm*)data;
	struct pgm_spm6* spm6 = (struct pgm_spm6*)data;

	spm->spm_nla_afi = g_ntohs (spm->spm_nla_afi);

	printf ("sqn %lu trail %lu lead %lu nla-afi %u ",
		(gulong)g_ntohl(spm->spm_sqn),
		(gulong)g_ntohl(spm->spm_trail),
		(gulong)g_ntohl(spm->spm_lead),
		spm->spm_nla_afi);	/* address family indicator */

	char s[INET6_ADDRSTRLEN];
	switch (spm->spm_nla_afi) {
	case AFI_IP:
		inet_ntop ( AF_INET, &spm->spm_nla, s, sizeof (s) );
		data  = (guint8*)data + sizeof( struct pgm_spm );
		len  -= sizeof( struct pgm_spm );
		break;

	case AFI_IP6:
		if (len < sizeof (struct pgm_spm6)) {
			puts ("packet truncated :(");
			return FALSE;
		}

		inet_ntop ( AF_INET6, &spm6->spm6_nla, s, sizeof (s) );
		data  = (guint8*)data + sizeof( struct pgm_spm6 );
		len  -= sizeof( struct pgm_spm6 );
		break;

	default:
		printf ("unsupported afi");
		return FALSE;
	}

	printf ("%s", s);

/* option extensions */
	if (header->pgm_options & PGM_OPT_PRESENT &&
		pgm_print_options (data, len) < 0 )
	{
		return FALSE;
	}

	printf ("\n");
	return TRUE;
}

/* 14.7.1.  Poll Request
 *
 *  0                   1                   2                   3
 *  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                    POLL's Sequence Number                     |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |         POLL's Round          |       POLL's Sub-type         |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |            NLA AFI            |          Reserved             |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                            Path NLA                     ...   |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-...-+-+
 * |                  POLL's  Back-off Interval                    |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                        Random String                          |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                      Matching Bit-Mask                        |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * | Option Extensions when present ...                            |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+- ... -+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *
 * Sent to ODATA multicast group with IP Router Alert option.
 */

#define PGM_MIN_POLL_SIZE	( sizeof(struct pgm_poll) )

static gboolean
pgm_print_poll (
	struct pgm_header*	header,
	gpointer		data,
	gsize			len
	)
{
	printf ("POLL: ");

	if (len < PGM_MIN_POLL_SIZE) {
		puts ("packet truncated :(");
		return FALSE;
	}

	struct pgm_poll* poll4 = (struct pgm_poll*)data;
	struct pgm_poll6* poll6 = (struct pgm_poll6*)data;
	poll4->poll_nla_afi = g_ntohs (poll4->poll_nla_afi);

	printf ("sqn %lu round %u sub-type %u nla-afi %u ",
		(gulong)g_ntohl(poll4->poll_sqn),
		g_ntohs(poll4->poll_round),
		g_ntohs(poll4->poll_s_type),
		poll4->poll_nla_afi);	/* address family indicator */

	char s[INET6_ADDRSTRLEN];
	switch (poll4->poll_nla_afi) {
	case AFI_IP:
		inet_ntop ( AF_INET, &poll4->poll_nla, s, sizeof (s) );
		data  = (guint8*)data + sizeof( struct pgm_poll );
		len  -= sizeof( struct pgm_poll );
		printf ("%s", s);

/* back-off interval in microseconds */
		printf (" bo_ivl %u", poll4->poll_bo_ivl);

/* random string */
		printf (" rand [%c%c%c%c]",
			isprint (poll4->poll_rand[0]) ? poll4->poll_rand[0] : '.',
			isprint (poll4->poll_rand[1]) ? poll4->poll_rand[1] : '.',
			isprint (poll4->poll_rand[2]) ? poll4->poll_rand[2] : '.',
			isprint (poll4->poll_rand[3]) ? poll4->poll_rand[3] : '.' );

/* matching bit-mask */
		printf (" mask 0x%x", poll4->poll_mask);
		break;

	case AFI_IP6:
		if (len < sizeof (struct pgm_poll6)) {
			puts ("packet truncated :(");
			return FALSE;
		}

		inet_ntop ( AF_INET6, &poll6->poll6_nla, s, sizeof (s) );
		data  = (guint8*)data + sizeof( struct pgm_poll6 );
		len  -= sizeof( struct pgm_poll6 );
		printf ("%s", s);

/* back-off interval in microseconds */
		printf (" bo_ivl %u", poll6->poll6_bo_ivl);

/* random string */
		printf (" rand [%c%c%c%c]",
			isprint (poll6->poll6_rand[0]) ? poll6->poll6_rand[0] : '.',
			isprint (poll6->poll6_rand[1]) ? poll6->poll6_rand[1] : '.',
			isprint (poll6->poll6_rand[2]) ? poll6->poll6_rand[2] : '.',
			isprint (poll6->poll6_rand[3]) ? poll6->poll6_rand[3] : '.' );

/* matching bit-mask */
		printf (" mask 0x%x", poll6->poll6_mask);
		break;

	default:
		printf ("unsupported afi");
		return FALSE;
	}


/* option extensions */
	if (header->pgm_options & PGM_OPT_PRESENT &&
		pgm_print_options (data, len) < 0 )
	{
		return FALSE;
	}

	printf ("\n");
	return TRUE;
}

/* 14.7.2.  Poll Response
 *
 *  0                   1                   2                   3
 *  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                    POLR's Sequence Number                     |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |         POLR's Round          |           reserved            |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * | Option Extensions when present ...                            |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+- ... -+-+-+-+-+-+-+-+-+-+-+-+-+-+
 */

static gboolean
pgm_print_polr (
	struct pgm_header*	header,
	gpointer		data,
	gsize			len
	)
{
	printf ("POLR: ");

	if (len < sizeof(struct pgm_polr)) {
		puts ("packet truncated :(");
		return FALSE;
	}

	struct pgm_polr* polr = (struct pgm_polr*)data;

	printf("sqn %lu round %u",
		(gulong)g_ntohl(polr->polr_sqn),
		g_ntohs(polr->polr_round));

	data = (guint8*)data + sizeof(struct pgm_polr);
	len -= sizeof(struct pgm_polr);

/* option extensions */
	if (header->pgm_options & PGM_OPT_PRESENT &&
		pgm_print_options (data, len) < 0 )
	{
		return FALSE;
	}

	printf ("\n");
	return TRUE;
}

/* 8.2.  Data Packet
 *
 *  0                   1                   2                   3
 *  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                  Data Packet Sequence Number                  |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                 Trailing Edge Sequence Number                 |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * | Option Extensions when present ...                            |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+- ... -+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * | Data ...
 * +-+-+- ...
 */

static gboolean
pgm_print_odata (
	struct pgm_header*	header,
	gpointer		data,
	gsize			len
	)
{
	printf ("ODATA: ");

	if (len < sizeof(struct pgm_data)) {
		puts ("packet truncated :(");
		return FALSE;
	}

	struct pgm_data* odata = (struct pgm_data*)data;

	printf ("sqn %lu trail %lu [",
		(gulong)g_ntohl(odata->data_sqn),
		(gulong)g_ntohl(odata->data_trail));

/* option extensions */
	data = (guint8*)data + sizeof(struct pgm_data);
	len -= sizeof(struct pgm_data);

	char* payload = data;
	if (header->pgm_options & PGM_OPT_PRESENT)
	{
		gssize opt_len = pgm_print_options (data, len);
		if (opt_len < 0) {
			return FALSE;
		}
		payload	+= opt_len;
		len	-= opt_len;
	}

/* data */
	char* end = payload + g_ntohs (header->pgm_tsdu_length);
	while (payload < end) {
		if (isprint(*payload))
			putchar(*payload);
		else
			putchar('.');
		payload++;
	}

	printf ("]\n");
	return TRUE;
}

/* 8.2.  Repair Data
 */

static gboolean
pgm_print_rdata (
	struct pgm_header*	header,
	gpointer		data,
	gsize			len
	)
{
	printf ("RDATA: ");

	if (len < sizeof(struct pgm_data)) {
		puts ("packet truncated :(");
		return FALSE;
	}

	struct pgm_data* rdata = (struct pgm_data*)data;

	printf ("sqn %lu trail %lu [",
		(gulong)g_ntohl(rdata->data_sqn),
		(gulong)g_ntohl(rdata->data_trail));

/* option extensions */
	data = (guint8*)data + sizeof(struct pgm_data);
	len -= sizeof(struct pgm_data);

	char* payload = data;
	if (header->pgm_options & PGM_OPT_PRESENT)
	{
		gssize opt_len = pgm_print_options (data, len);
		if (opt_len < 0) {
			return FALSE;
		}
		data = (guint8*)data + opt_len;
		len -= opt_len;
	}

/* data */
	char* end = (char*)( (guint8*)data + len );
	while (payload < end) {
		if (isprint(*payload))
			putchar(*payload);
		else
			putchar('.');
		payload++;
	}

	printf ("]\n");
	return TRUE;
}

/* 8.3.  NAK
 *
 * Technically the AFI of the source and multicast group can be different
 * but that would be very wibbly wobbly.  One example is using a local DLR
 * with a IPv4 address to reduce NAK cost for recovery on wide IPv6
 * distribution.
 *
 *  0                   1                   2                   3
 *  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                   Requested Sequence Number                   |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |            NLA AFI            |          Reserved             |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                           Source NLA                    ...   |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-...-+-+
 * |            NLA AFI            |          Reserved             |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                      Multicast Group NLA                ...   |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-...-+-+
 * | Option Extensions when present ...
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+- ...
 */

#define PGM_MIN_NAK_SIZE	( sizeof(struct pgm_nak) )

int
pgm_verify_nak (
	G_GNUC_UNUSED struct pgm_header* header,
	gpointer		data,
	gsize			len
	)
{
	int retval = 0;

/* truncated packet */
	if (len < PGM_MIN_NAK_SIZE) {
		retval = -EINVAL;
		goto out;
	}

	struct pgm_nak* nak = (struct pgm_nak*)data;
	int nak_src_nla_afi = g_ntohs (nak->nak_src_nla_afi);
	int nak_grp_nla_afi = -1;

/* check source NLA: unicast address of the ODATA sender */
	switch (nak_src_nla_afi) {
	case AFI_IP:
		nak_grp_nla_afi = g_ntohs (nak->nak_grp_nla_afi);
		break;

	case AFI_IP6:
		nak_grp_nla_afi = g_ntohs (((struct pgm_nak6*)nak)->nak6_grp_nla_afi);
		break;

	default:
		retval = -EINVAL;
		goto out;
	}

/* check multicast group NLA */
	switch (nak_grp_nla_afi) {
	case AFI_IP6:
		switch (nak_src_nla_afi) {
/* IPv4 + IPv6 NLA */
		case AFI_IP:
			if (len < ( sizeof(struct pgm_nak) + sizeof(struct in6_addr) - sizeof(struct in_addr) )) {
				retval = -EINVAL;
			}
			break;

/* IPv6 + IPv6 NLA */
		case AFI_IP6:
			if (len < sizeof(struct pgm_nak6)) {
				retval = -EINVAL;
			}
			break;
		}

	case AFI_IP:
		break;

	default:
		retval = -EINVAL;
		break;
	}

out:
	return retval;
}

static gboolean
pgm_print_nak (
	struct pgm_header*	header,
	gpointer		data,
	gsize			len
	)
{
	printf ("NAK: ");

	if (len < PGM_MIN_NAK_SIZE) {
		puts ("packet truncated :(");
		return FALSE;
	}

	struct pgm_nak* nak = (struct pgm_nak*)data;
	struct pgm_nak6* nak6 = (struct pgm_nak6*)data;
	nak->nak_src_nla_afi = g_ntohs (nak->nak_src_nla_afi);

	printf ("sqn %lu src ", 
		(gulong)g_ntohl(nak->nak_sqn));

	char s[INET6_ADDRSTRLEN];

/* source nla */
	switch (nak->nak_src_nla_afi) {
	case AFI_IP:
		nak->nak_grp_nla_afi = g_ntohs (nak->nak_grp_nla_afi);
		if (nak->nak_grp_nla_afi != nak->nak_grp_nla_afi) {
			puts ("different source & group afi very wibbly wobbly :(");
			return FALSE;
		}

		inet_ntop ( AF_INET, &nak->nak_src_nla, s, sizeof(s) );
		data  = (guint8*)data + sizeof( struct pgm_nak );
		len  -= sizeof( struct pgm_nak );
		printf ("%s grp ", s);

		inet_ntop ( AF_INET, &nak->nak_grp_nla, s, sizeof(s) );
		printf ("%s", s);

	case AFI_IP6:
		if (len < sizeof (struct pgm_nak6)) {
			puts ("packet truncated :(");
			return FALSE;
		}

		nak6->nak6_grp_nla_afi = g_ntohs (nak6->nak6_grp_nla_afi);
		if (nak6->nak6_grp_nla_afi != nak6->nak6_grp_nla_afi) {
			puts ("different source & group afi very wibbly wobbly :(");
			return FALSE;
		}

		inet_ntop ( AF_INET6, &nak6->nak6_src_nla, s, sizeof(s) );
		data  = (guint8*)data + sizeof( struct pgm_nak6 );
		len  -= sizeof( struct pgm_nak6 );
		printf ("%s grp ", s);

		inet_ntop ( AF_INET6, &nak6->nak6_grp_nla, s, sizeof(s) );
		printf ("%s", s);

	default:
		printf ("unsupported afi");
		break;
	}


/* option extensions */
	if (header->pgm_options & PGM_OPT_PRESENT &&
		pgm_print_options (data, len) < 0 )
	{
		return FALSE;
	}

	printf ("\n");
	return TRUE;
}

/* 8.3.  N-NAK
 */

int
pgm_verify_nnak (
	struct pgm_header*	header,
	gpointer		data,
	gsize			len
	)
{
	return pgm_verify_nak (header, data, len);
}

static gboolean
pgm_print_nnak (
	G_GNUC_UNUSED struct pgm_header*	header,
	G_GNUC_UNUSED gpointer			data,
	gsize			len
	)
{
	printf ("N-NAK: ");

	if (len < sizeof(struct pgm_nak)) {
		puts ("packet truncated :(");
		return FALSE;
	}

//	struct pgm_nak* nnak = (struct pgm_nak*)data;

	return TRUE;
}

/* 8.3.  NCF
 */

int
pgm_verify_ncf (
	struct pgm_header*	header,
	gpointer		data,
	gsize			len
	)
{
	return pgm_verify_nak (header, data, len);
}

gboolean
pgm_print_ncf (
	G_GNUC_UNUSED struct pgm_header*	header,
	G_GNUC_UNUSED gpointer			data,
	gsize			len
	)
{
	printf ("NCF: ");

	if (len < sizeof(struct pgm_nak)) {
		puts ("packet truncated :(");
		return FALSE;
	}

//	struct pgm_nak* ncf = (struct pgm_nak*)data;

	return TRUE;
}

/* 13.6.  SPM Request
 *
 *  0                   1                   2                   3
 *  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * | Option Extensions when present ...
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+- ...
 */

int
pgm_verify_spmr (
	G_GNUC_UNUSED struct pgm_header*	header,
	G_GNUC_UNUSED gpointer			data,
	G_GNUC_UNUSED gsize			len
	)
{
	int retval = 0;

	return retval;
}

static gboolean
pgm_print_spmr (
	struct pgm_header*	header,
	gpointer		data,
	gsize			len
	)
{
	printf ("SPMR: ");

/* option extensions */
	if (header->pgm_options & PGM_OPT_PRESENT &&
		pgm_print_options (data, len) < 0 )
	{
		return FALSE;
	}

	printf ("\n");
	return TRUE;
}

/* Parse PGM options fields, alters contents of packet.
 * 
 * returns -1 on failure, or total length in octets of the option fields
 */

static gssize
pgm_print_options (
	gpointer		data,
	gsize			len
	)
{
	printf (" OPTIONS:");

	if (len < sizeof(struct pgm_opt_length)) {
		puts (" packet truncated :(");
		return -1;
	}

	struct pgm_opt_length* opt_len = (struct pgm_opt_length*)data;

	if (opt_len->opt_length != sizeof(struct pgm_opt_length)) {
		printf (" bad opt_length length %hhu\n", opt_len->opt_length);
		return -1;
	}

	opt_len->opt_total_length = g_ntohs (opt_len->opt_total_length);

	printf (" total len %" G_GUINT16_FORMAT " ", opt_len->opt_total_length);

	if (opt_len->opt_total_length < (sizeof(struct pgm_opt_length) + sizeof(struct pgm_opt_header)) || opt_len->opt_total_length > len) {
		puts ("bad total length");
		return -1;
	}

/* total length includes opt_length option */
	opt_len->opt_total_length -= sizeof(struct pgm_opt_length);
	struct pgm_opt_header* opt_header = (struct pgm_opt_header*)(opt_len + 1);

/* iterate through options (max 16) */
	int count = 16;
	while (opt_len->opt_total_length && count) {
		if (opt_len->opt_total_length < sizeof(struct pgm_opt_header) || opt_header->opt_length > opt_len->opt_total_length) {
			puts ("short on option data :o");
			return -1;
		}

		if (opt_header->opt_type & PGM_OPT_END) {
			printf ("OPT_END+");
		}

		switch (opt_header->opt_type & PGM_OPT_MASK) {
		case PGM_OPT_SYN:
			printf ("OPT_SYN ");
			break;

		case PGM_OPT_FIN:
			printf ("OPT_FIN ");
			break;

		case PGM_OPT_RST:
			printf ("OPT_RST ");
			break;

		case PGM_OPT_PARITY_PRM:
			printf ("OPT_PARITY_PRM ");
			break;

		case PGM_OPT_CURR_TGSIZE:
			printf ("OPT_CURR_TGSIZE ");
			break;

		default:
			printf ("OPT-%u{%u} ", opt_header->opt_type & PGM_OPT_MASK, opt_header->opt_length);
			break;
		}

		opt_len->opt_total_length -= opt_header->opt_length;
		opt_header = (struct pgm_opt_header*)((char*)opt_header + opt_header->opt_length);

		count--;
	}

	if (!count) {
		puts ("too many options found");
		return -1;
	}

	return ((guint8*)opt_header - (guint8*)data);
}

const char*
pgm_type_string (
	guint8		type
	)
{
	const char* c;

	switch (type) {
	case PGM_SPM:		c = "PGM_SPM"; break;
	case PGM_POLL:		c = "PGM_POLL"; break;
	case PGM_POLR:		c = "PGM_POLR"; break;
	case PGM_ODATA:		c = "PGM_ODATA"; break;
	case PGM_RDATA:		c = "PGM_RDATA"; break;
	case PGM_NAK:		c = "PGM_NAK"; break;
	case PGM_NNAK:		c = "PGM_NNAK"; break;
	case PGM_NCF:		c = "PGM_NCF"; break;
	case PGM_SPMR:		c = "PGM_SPMR"; break;
	default: c = "(unknown)"; break;
	}

	return c;
}

const char*
pgm_udpport_string (
	int		port
	)
{
	static GHashTable *services = NULL;

	if (!services) {
		services = g_hash_table_new (g_int_hash, g_int_equal);
	}

	gpointer service_string = g_hash_table_lookup (services, &port);
	if (service_string != NULL) {
		return service_string;
	}

	struct servent* se = getservbyport (port, "udp");
	if (se == NULL) {
		char buf[sizeof("00000")];
		snprintf(buf, sizeof(buf), "%i", g_ntohs(port));
		service_string = g_strdup(buf);
	} else {
		service_string = g_strdup(se->s_name);
	}
	g_hash_table_insert (services, &port, service_string);
	return service_string;
}

const char*
pgm_gethostbyaddr (
	const struct in_addr*	ap
	)
{
	static GHashTable *hosts = NULL;

	if (!hosts) {
		hosts = g_hash_table_new (g_str_hash, g_str_equal);
	}

	gpointer host_string = g_hash_table_lookup (hosts, ap);
	if (host_string != NULL) {
		return host_string;
	}

	struct hostent* he = gethostbyaddr(ap, sizeof(struct in_addr), AF_INET);
	if (he == NULL) {
		struct in_addr in;
		memcpy (&in, ap, sizeof(in));
		host_string = g_strdup(inet_ntoa(in));
	} else {
		host_string = g_strdup(he->h_name);
	}
	g_hash_table_insert (hosts, ap, host_string);
	return host_string;
}

void
pgm_ipopt_print (
	gconstpointer		ipopt,
	gsize			length
	)
{
	const char* op = ipopt;

	while (length)
	{
		char len = (*op == IPOPT_NOP || *op == IPOPT_EOL) ? 1 : op[1];
		switch (*op) {
		case IPOPT_EOL:		printf(" eol"); break;
		case IPOPT_NOP:		printf(" nop"); break;
		case IPOPT_RR:		printf(" rr"); break;	/* 1 route */
		case IPOPT_TS:		printf(" ts"); break;	/* 1 TS */
#if 0
		case IPOPT_SECURITY:	printf(" sec-level"); break;
		case IPOPT_LSRR:	printf(" lsrr"); break;	/* 1 route */
		case IPOPT_SATID:	printf(" satid"); break;
		case IPOPT_SSRR:	printf(" ssrr"); break;	/* 1 route */
#endif
		default:		printf(" %hhx{%hhd}", *op, len); break;
		}

		if (!len) {
			puts ("invalid IP opt length");
			return;
		}

		op += len;
		length -= len;
	}
}

/* eof */
