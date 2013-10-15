/* vim:ts=8:sts=4:sw=4:noai:noexpandtab
 * 
 * struct sockaddr functions independent of in or in6.
 *
 * Copyright (c) 2006-2009 Miru Limited.
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

#ifndef __PGM_SOCKADDR_H__
#define __PGM_SOCKADDR_H__

#include <errno.h>
#include <string.h>
#include <netinet/in.h>

#include <glib.h>


#ifndef __PGM_IF_H__
#	include <pgm/if.h>
#endif


/* glibc 2.3 on debian etch doesn't include this */
#ifndef IPV6_RECVPKTINFO
#	define IPV6_RECVPKTINFO		49
#endif

/* OpenSolaris differences */
#ifndef MCAST_MSFILTER
#	include <sys/ioctl.h>
#	define MCAST_MSFILTER		SIOCSMSFILTER
#endif
#ifndef SOL_IP
#	define SOL_IP			IPPROTO_IP
#endif
#ifndef SOL_IPV6
#	define SOL_IPV6			IPPROTO_IPV6
#endif
#ifndef MSG_CONFIRM
#	define MSG_CONFIRM		0
#endif
#ifndef IP_ROUTER_ALERT
#	include <netinet/ip.h>
#	define IP_ROUTER_ALERT		IPOPT_RTRALERT
#endif
#ifndef IPV6_ROUTER_ALERT
#	include <netinet/ip6.h>
#	define IPV6_ROUTER_ALERT	IP6OPT_ROUTER_ALERT
#endif
#ifndef IP_MAX_MEMBERSHIPS
#	define IP_MAX_MEMBERSHIPS	20
#endif


G_BEGIN_DECLS

#define pgm_sockaddr_family(src)	( ((const struct sockaddr*)(src))->sa_family )

#define pgm_sockaddr_port(src) \
	    ( pgm_sockaddr_family(src) == AF_INET ? \
		((const struct sockaddr_in*)(src))->sin_port : \
		((const struct sockaddr_in6*)(src))->sin6_port )

#define pgm_sockaddr_addr(src) \
	    ( pgm_sockaddr_family(src) == AF_INET ? \
		(const void*)&((const struct sockaddr_in*)(src))->sin_addr : \
		(const void*)&((const struct sockaddr_in6*)(src))->sin6_addr )

#define pgm_sockaddr_len(src) \
	    ( pgm_sockaddr_family(src) == AF_INET ? sizeof(struct sockaddr_in) : sizeof(struct sockaddr_in6) )

#define pgm_sockaddr_scope_id(src) \
	    ( pgm_sockaddr_family(src) == AF_INET ? 0 : ((const struct sockaddr_in6*)(src))->sin6_scope_id )

#define pgm_sockaddr_ntop(src,dst,cnt) \
	    ( inet_ntop(pgm_sockaddr_family(src), pgm_sockaddr_addr(src), (dst), (cnt)) )

static inline int pgm_sockaddr_is_addr_multicast (const struct sockaddr* s)
{
    int retval = 0;

    switch (s->sa_family) {
    case AF_INET:
	retval = IN_MULTICAST(g_ntohl( ((const struct sockaddr_in*)s)->sin_addr.s_addr ));
	break;

    case AF_INET6:
	retval = IN6_IS_ADDR_MULTICAST( &((const struct sockaddr_in6*)s)->sin6_addr );
	break;

    default:
	retval = -EINVAL;
	break;
    }

    return retval;
}

static inline int pgm_sockaddr_cmp (const struct sockaddr *a, const struct sockaddr *b)
{
    int retval = 0;

    if (a->sa_family != b->sa_family)
    {
	retval = a->sa_family < b->sa_family ? -1 : 1;
    }
    else
    {
	switch (a->sa_family) {
	case AF_INET:
	    if (((const struct sockaddr_in*)a)->sin_addr.s_addr != ((const struct sockaddr_in*)b)->sin_addr.s_addr)
	    {
		retval = ((const struct sockaddr_in*)a)->sin_addr.s_addr < ((const struct sockaddr_in*)b)->sin_addr.s_addr ? -1 : 1;
	    }
	    break;

	case AF_INET6:
	    retval = memcmp (&((const struct sockaddr_in6*)a)->sin6_addr, &((const struct sockaddr_in6*)b)->sin6_addr, sizeof(struct in6_addr));
	    break;

	default:
	    retval = -EINVAL;
	    break;
	}
    }

    return retval;
}

static inline int pgm_sockaddr_hdrincl (int s, int sa_family, gboolean v)
{
    int retval = 0;
    gint optval = v;

    switch (sa_family) {
    case AF_INET:
	retval = setsockopt (s, IPPROTO_IP, IP_HDRINCL, &optval, sizeof(optval));
	break;

    case AF_INET6:  /* method does not exist */
	break;

    default:
	retval = -EINVAL;
	break;
    }

    return retval;
}

static inline int pgm_sockaddr_pktinfo (int s, int sa_family, gboolean v)
{
    int retval = 0;
    gint optval = v;

    switch (sa_family) {
    case AF_INET:
	retval = setsockopt (s, IPPROTO_IP, IP_PKTINFO, &optval, sizeof(optval));
	break;

    case AF_INET6:
	retval = setsockopt (s, IPPROTO_IPV6, IPV6_RECVPKTINFO, &optval, sizeof(optval));
	break;

    default:
	retval = -EINVAL;
	break;
    }

    return retval;
}


static inline int pgm_sockaddr_router_alert (int s, int sa_family, gboolean v)
{
    int retval = 0;
    gint8 optval = v;

    switch (sa_family) {
    case AF_INET:
	retval = setsockopt (s, IPPROTO_IP, IP_ROUTER_ALERT, &optval, sizeof(optval));
	break;

    case AF_INET6:
	retval = setsockopt (s, IPPROTO_IPV6, IPV6_ROUTER_ALERT, &optval, sizeof(optval));
	break;

    default:
	retval = -EINVAL;
	break;
    }

    return retval;
}

static inline int pgm_sockaddr_tos (int s, int sa_family, int tos)
{
    int retval = 0;
    gint optval = tos;

    switch (sa_family) {
    case AF_INET:
	retval = setsockopt (s, IPPROTO_IP, IP_TOS, &optval, sizeof(optval));
	break;

    case AF_INET6:  /* TRAFFIC_CLASS not implemented */
	break;

    default:
	retval = -EINVAL;
	break;
    }

    return retval;
}

/* nb: IPV6_JOIN_GROUP == IPV6_ADD_MEMBERSHIP
 */
static inline int pgm_sockaddr_add_membership (int s, const struct group_source_req* gsr)
{
	int retval = 0;

	switch (pgm_sockaddr_family(&gsr->gsr_group)) {
	case AF_INET:
	{
/* Linux: ip_mreqn preferred, ip_mreq supported for compat */
	    struct ip_mreq mreq;
	    memset (&mreq, 0, sizeof(mreq));

	    mreq.imr_multiaddr.s_addr = ((const struct sockaddr_in*)&gsr->gsr_group)->sin_addr.s_addr;

	    struct sockaddr_in interface;
	    pgm_if_indextosockaddr (gsr->gsr_interface, AF_INET, (struct sockaddr*)&interface);
	    mreq.imr_interface.s_addr = interface.sin_addr.s_addr;

	    retval = setsockopt (s, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq));
	}
	break;

	case AF_INET6:
	{
		struct ipv6_mreq mreq6;
		memset (&mreq6, 0, sizeof(mreq6));

		mreq6.ipv6mr_multiaddr = ((const struct sockaddr_in6*)&gsr->gsr_group)->sin6_addr;
		mreq6.ipv6mr_interface = gsr->gsr_interface;

		retval = setsockopt (s, IPPROTO_IPV6, IPV6_JOIN_GROUP, &mreq6, sizeof(mreq6));
	}
	break;

	default:
		retval = -EINVAL;
		break;
	}

	return retval;
}

static inline int pgm_sockaddr_multicast_if (int s, const struct sockaddr* address, int ifindex)
{
	int retval = 0;

	switch (pgm_sockaddr_family(address)) {
	case AF_INET:
	{
		const struct in_addr interface = ((const struct sockaddr_in*)address)->sin_addr;
		retval = setsockopt (s, IPPROTO_IP, IP_MULTICAST_IF, &interface, sizeof(interface));
	}
	break;

	case AF_INET6:
	{
		retval = setsockopt (s, IPPROTO_IPV6, IPV6_MULTICAST_IF, &ifindex, sizeof(ifindex));
	}
	break;

	default:
		retval = -EINVAL;
		break;
	}

	return retval;
}

static inline int pgm_sockaddr_multicast_loop (int s, int sa_family, gboolean v)
{
    int retval = 0;

    switch (sa_family) {
    case AF_INET:
    {
    	gint8 optval = v;
	retval = setsockopt (s, IPPROTO_IP, IP_MULTICAST_LOOP, &optval, sizeof(optval));
	break;
    }

    case AF_INET6:
    {
	gint optval = v;
	retval = setsockopt (s, IPPROTO_IPV6, IPV6_MULTICAST_LOOP, &optval, sizeof(optval));
	break;
    }

    default:
	retval = -EINVAL;
	break;
    }

    return retval;
}

static inline int pgm_sockaddr_multicast_hops (int s, int sa_family, gint hops)
{
    int retval = 0;

    switch (sa_family) {
    case AF_INET:
    {
    	gint8 optval = hops;
	retval = setsockopt (s, IPPROTO_IP, IP_MULTICAST_TTL, &optval, sizeof(optval));
	break;
    }

    case AF_INET6:
    {
    	gint optval = hops;
	retval = setsockopt (s, IPPROTO_IPV6, IPV6_MULTICAST_HOPS, &optval, sizeof(optval));
	break;
    }

    default:
	retval = -EINVAL;
	break;
    }

    return retval;
}

G_END_DECLS

#endif /* __PGM_SOCKADDR_H__ */
