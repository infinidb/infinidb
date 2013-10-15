/* vim:ts=8:sts=8:sw=4:noai:noexpandtab
 *
 * network interface handling.
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

#include <ctype.h>
#include <errno.h>
#ifdef CONFIG_HAVE_GETIFADDRS
#	include <ifaddrs.h>
#endif
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <net/if.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <glib.h>

#include "pgm/if.h"
#include "pgm/sockaddr.h"

//#define IF_DEBUG

#ifndef IF_DEBUG
#define g_trace(...)		while (0)
#else
#define g_trace(...)		g_debug(__VA_ARGS__)
#endif



/* temporary structure to contain interface name whilst address family
 * has not been resolved.
 */
struct interface_req {
	char			ir_name[IF_NAMESIZE];
	unsigned int		ir_interface;		/* interface index */
	struct sockaddr_storage ir_addr;		/* interface address */
};

#ifdef CONFIG_HAVE_GETIFADDRS
#	define pgm_ifaddrs	ifaddrs
#else
struct pgm_ifaddrs
{
	struct pgm_ifaddrs *ifa_next;	/* Pointer to the next structure.  */

	char *ifa_name;			/* Name of this network interface.  */
	unsigned int ifa_flags;		/* Flags as from SIOCGIFFLAGS ioctl.  */

	struct sockaddr *ifa_addr;	/* Network address of this interface.  */
	struct sockaddr *ifa_netmask;	/* Netmask of this interface.  */
};

struct _pgm_ifaddrs
{
	struct pgm_ifaddrs	_ifa;
	char			_name[IF_NAMESIZE];
	struct sockaddr		_addr;
	struct sockaddr		_netmask;
};
#endif


/* globals */

#define IF_DEFAULT_GROUP	((in_addr_t) 0xefc00001) /* 239.192.0.1 */

/* ff08::1 */
#define IF6_DEFAULT_INIT { { { 0xff,8,0,0,0,0,0,0,0,0,0,0,0,0,0,1 } } }
const struct in6_addr if6_default_group_addr = IF6_DEFAULT_INIT;


#ifdef CONFIG_HAVE_GETHOSTBYNAME2
#	define pgm_gethostbyname2	gethostbyname2
#else
static struct hostent*
pgm_gethostbyname2 (
	const char*		name,
	int			af
	)
{
	int error_num;
	static struct hostent* ip = NULL;

	if (ip)
		freehostent (ip);
	return (ip = getipnodebyname (name, af, 0, &error_num));
}
#endif

#ifdef CONFIG_HAVE_GETIFADDRS
static inline int
pgm_if_getifaddrs (
	struct pgm_ifaddrs**	ifap
	)
{
	return getifaddrs ((struct ifaddrs**)ifap);
}

static inline void
pgm_if_freeifaddrs (
	struct pgm_ifaddrs*	ifa
	)
{
	freeifaddrs ((struct ifaddrs*)ifa);
}
#else

static int
pgm_if_getifaddrs (
	struct pgm_ifaddrs**	ifap
	)
{
	int sock = socket (AF_INET, SOCK_DGRAM, 0);
	if (sock < 0) {
		return -1;
	}

	int sock6 = socket (AF_INET6, SOCK_DGRAM, 0);
	if (sock6 < 0) {
		close (sock6);
		return -1;
	}

/* get count of interfaces */
	char buf[1024], buf6[1024];
	struct ifconf ifc, ifc6;

	ifc.ifc_buf = buf;
	ifc.ifc_len = sizeof(buf);
	if (ioctl (sock, SIOCGIFCONF, &ifc) < 0) {
		close (sock);
		close (sock6);
		return -1;
	}

	ifc6.ifc_buf = buf6;
	ifc6.ifc_len = sizeof(buf6);
	if (ioctl (sock6, SIOCGIFCONF, &ifc6) < 0) {
		close (sock);
		close (sock6);
		return -1;
	}

/* alloc a contiguous block for entire list */
	int n = (ifc.ifc_len + ifc6.ifc_len) / sizeof(struct ifreq);
	struct _pgm_ifaddrs* ifa = malloc (n * sizeof(struct _pgm_ifaddrs));
	memset (ifa, 0, n * sizeof(struct _pgm_ifaddrs));

/* foreach interface */
	struct ifreq *ifr  = ifc.ifc_req;
	struct ifreq *lifr = (struct ifreq *)&ifc.ifc_buf[ifc.ifc_len];
	struct _pgm_ifaddrs* ift = ifa;

	g_assert (IF_NAMESIZE >= sizeof(ifr->ifr_name));

	while (ifr < lifr)
	{
/* address */
		if (ioctl (sock, SIOCGIFADDR, ifr) != -1) {
			ift->_ifa.ifa_addr = &ift->_addr;
			memcpy (ift->_ifa.ifa_addr, &ifr->ifr_addr, sizeof(struct sockaddr));
		}

/* name */
		ift->_ifa.ifa_name = ift->_name;
		strncpy (ift->_ifa.ifa_name, ifr->ifr_name, sizeof(ifr->ifr_name));
		ift->_ifa.ifa_name[sizeof(ifr->ifr_name) - 1] = 0;

/* flags */
		if (ioctl (sock, SIOCGIFFLAGS, ifr) != -1) {
			ift->_ifa.ifa_flags = ifr->ifr_flags;
		}

/* netmask */
		if (ioctl (sock, SIOCGIFNETMASK, ifr) != -1) {
			ift->_ifa.ifa_netmask = &ift->_netmask;
#ifdef CONFIG_HAVE_IFR_NETMASK
			memcpy (ift->_ifa.ifa_netmask, &ifr->ifr_netmask, sizeof(struct sockaddr));
#else
			memcpy (ift->_ifa.ifa_netmask, &ifr->ifr_addr, sizeof(struct sockaddr));
#endif
		}

		++ifr;
		if (ifr < lifr) {
			ift->_ifa.ifa_next = (struct pgm_ifaddrs*)(ift + 1);
			ift = (struct _pgm_ifaddrs*)(ift->_ifa.ifa_next);
		}
	}

#ifdef CONFIG_HAVE_IPV6_SIOCGIFADDR
/* repeat for IPv6 */
	ifr  = ifc6.ifc_req;
	lifr = (struct ifreq *)&ifc6.ifc_buf[ifc6.ifc_len];

	while (ifr < lifr)
	{
		if (ift != ifa) {
			ift->_ifa.ifa_next = (struct pgm_ifaddrs*)(ift + 1);
			ift = (struct _pgm_ifaddrs*)(ift->_ifa.ifa_next);
		}

/* address, note this does not work on Linux as struct ifreq is too small for an IPv6 address */
		if (ioctl (sock6, SIOCGIFADDR, ifr) != -1) {
			ift->_ifa.ifa_addr = &ift->_addr;
			memcpy (ift->_ifa.ifa_addr, &ifr->ifr_addr, sizeof(struct sockaddr));
		}

/* name */
		ift->_ifa.ifa_name = ift->_name;
		strncpy (ift->_ifa.ifa_name, ifr->ifr_name, sizeof(ifr->ifr_name));
		ift->_ifa.ifa_name[sizeof(ifr->ifr_name) - 1] = 0;

/* flags */
		if (ioctl (sock6, SIOCGIFFLAGS, ifr) != -1) {
			ift->_ifa.ifa_flags = ifr->ifr_flags;
		}

/* netmask */
		if (ioctl (sock6, SIOCGIFNETMASK, ifr) != -1) {
#ifdef CONFIG_HAVE_IFR_NETMASK
			ift->_ifa.ifa_netmask = &ift->_netmask;
			memcpy (ift->_ifa.ifa_netmask, &ifr->ifr_netmask, sizeof(struct sockaddr));
#else
			ift->_ifa.ifa_netmask = &ift->_addr;
			memcpy (ift->_ifa.ifa_netmask, &ifr->ifr_addr, sizeof(struct sockaddr));
#endif
		}

		++ifr;
	}
#endif

	*ifap = (struct pgm_ifaddrs*)ifa;

	close (sock);
	close (sock6);
	return 0;
}

static void
pgm_if_freeifaddrs (
	struct pgm_ifaddrs*	ifa
	)
{
	free (ifa);
}

#endif /* CONFIG_HAVE_GETIFADDRS */

/* return node primary address on multi-address family interfaces.
 *
 * returns > 0 on success, or -1 on error and sets errno appropriately,
 * 			   or -2 on NS lookup error and sets h_errno appropriately.
 */

int
pgm_if_getnodeaddr (
	int			af,	/* requested address family, AF_INET, or AF_INET6 */
	struct sockaddr*	addr,
	socklen_t		cnt	/* size of address pointed to by addr */
	)
{
	g_return_val_if_fail (af == AF_INET || af == AF_INET6, -EINVAL);
	g_return_val_if_fail (NULL != addr, -EINVAL);

	char hostname[NI_MAXHOST + 1];
	struct hostent* he;

	gethostname (hostname, sizeof(hostname));

	if (AF_INET == af)
	{
		g_return_val_if_fail (cnt >= sizeof(struct sockaddr_in), -EINVAL);
		he = gethostbyname (hostname);
		if (NULL == he) {
			g_trace ("gethostbyname failed on local hostname: %s", hstrerror (h_errno));
			return -2;
		}
		((struct sockaddr_in*)addr)->sin_family = af;
		((struct sockaddr_in*)addr)->sin_addr.s_addr = ((struct in_addr*)(he->h_addr_list[0]))->s_addr;
		cnt = sizeof(struct sockaddr_in);
	}
	else
	{
		g_return_val_if_fail (cnt >= sizeof(struct sockaddr_in6), -EINVAL);
		he = pgm_gethostbyname2 (hostname, AF_INET6);
		if (NULL == he)
		{
/* try link scope via IPv4 nodename */
			he = gethostbyname (hostname);
			if (NULL == he)
			{
				g_trace ("gethostbyname2 and gethostbyname failed on local hostname: %s", hstrerror (h_errno));
				return -2;
			}

			struct pgm_ifaddrs *ifap, *ifa, *ifa6;
			int e = pgm_if_getifaddrs (&ifap);
			if (e < 0) {
				g_trace ("pgm_if_getifaddrs failed when trying to resolve link scope interfaces");
				return -1;
			}

/* hunt for IPv4 interface */
			for (ifa = ifap; ifa; ifa = ifa->ifa_next)
			{
				if (AF_INET != ifa->ifa_addr->sa_family) {
					continue;
				}
				if (((struct sockaddr_in *)ifa->ifa_addr)->sin_addr.s_addr == ((struct in_addr*)(he->h_addr_list[0]))->s_addr)
				{
					goto ipv4_found;
				}
			}
			g_trace ("node IPv4 interface not found!");
			pgm_if_freeifaddrs (ifap);
			errno = ENONET;
			return -1;
ipv4_found:

/* hunt for IPv6 interface */
			for (ifa6 = ifap; ifa6; ifa6 = ifa6->ifa_next)
			{
				if (AF_INET6 != ifa6->ifa_addr->sa_family) {
					continue;
				}
				if (0 == strcmp(ifa->ifa_name, ifa6->ifa_name))
				{
					goto ipv6_found;
				}
			}
			g_trace ("node IPv6 interface not found!");
			pgm_if_freeifaddrs (ifap);
			errno = ENONET;
			return -1;
ipv6_found:
			*(struct in6_addr*)(he->h_addr_list[0]) = ((struct sockaddr_in6 *)ifa6->ifa_addr)->sin6_addr;
			pgm_if_freeifaddrs (ifap);
		}
		((struct sockaddr_in6*)addr)->sin6_family = af;
		((struct sockaddr_in6*)addr)->sin6_addr = *(struct in6_addr*)(he->h_addr_list[0]);
		cnt = sizeof(struct sockaddr_in6);
	}

	return cnt;
}

/* recommended address space for multicast:
 * rfc4607, rfc3180, rfc2365
 *
 * avoid 5 high-order bit overlap.
 *
 * loopback:  ffx1::/16
 * segment:   ffx2::/16
 * glop:      238/8
 * mysterious admin:  239/8,   ffx6::/16
 * site:      239.252-255/16,  ffx5::/16
 * org:       239.192/14,      ffx8::/16
 *
 * internets: 224.0.1.0-238.255.255.255,  ffxe::/16
 */


/* dump all interfaces to console.
 *
 * note that interface indexes are only in regard to the link layer and hence
 * no 1-1 mapping between adapter name to index back to address.
 */
int
pgm_if_print_all (void)
{
	struct pgm_ifaddrs *ifap, *ifa;

	int e = pgm_if_getifaddrs (&ifap);
	if (e < 0) {
		perror("pgm_if_getifaddrs");
		return -1;
	}

	for (ifa = ifap; ifa; ifa = ifa->ifa_next)
	{
		int i = if_nametoindex(ifa->ifa_name);
		char rname[IF_NAMESIZE * 2 + 3];
		char b[IF_NAMESIZE * 2 + 3];

		if_indextoname(i, rname);
		sprintf (b, "%s (%s)", ifa->ifa_name, rname);

		if ( ifa->ifa_addr->sa_family != AF_INET && 
			ifa->ifa_addr->sa_family != AF_INET6)
		{
			g_message ("#%d name %-15.15s ---- %-46.46s status %s loop %s b/c %s m/c %s",
				i,
				b,
				"",
			ifa->ifa_flags & IFF_UP ? "UP  " : "DOWN",
			ifa->ifa_flags & IFF_LOOPBACK ? "YES" : "NO ",
			ifa->ifa_flags & IFF_BROADCAST ? "YES" : "NO ",
			ifa->ifa_flags & IFF_MULTICAST ? "YES" : "NO "
			);
			continue;
		}

		char s[INET6_ADDRSTRLEN];
		getnameinfo (ifa->ifa_addr, pgm_sockaddr_len(ifa->ifa_addr),
			     s, sizeof(s),
			     NULL, 0,
			     NI_NUMERICHOST);
		g_message ("#%d name %-15.15s IPv%i %-46.46s scope %u status %s loop %s b/c %s m/c %s",
			i,
			b,
			ifa->ifa_addr->sa_family == AF_INET ? 4 : 6,
			s,
			pgm_sockaddr_scope_id(ifa->ifa_addr),
			ifa->ifa_flags & IFF_UP ? "UP  " : "DOWN",
			ifa->ifa_flags & IFF_LOOPBACK ? "YES" : "NO ",
			ifa->ifa_flags & IFF_BROADCAST ? "YES" : "NO ",
			ifa->ifa_flags & IFF_MULTICAST ? "YES" : "NO "
			);
	}

	pgm_if_freeifaddrs (ifap);
	return 0;
}

/* interfaces indexes refer to the link layer, we want to find the internet layer address.
 * the big problem is that multiple IPv6 addresses can be bound to one link - called scopes.
 * we can just pick the first scope and let IP routing handle the rest.
 */
int
pgm_if_indextosockaddr(
	unsigned int		ifindex,
	int			iffamily,
	struct sockaddr*	ifsa
        )
{
	if (0 == ifindex)		/* any interface or address */
	{
		ifsa->sa_family = iffamily;
		switch (iffamily) {
		case AF_INET:
			((struct sockaddr_in*)ifsa)->sin_addr.s_addr = INADDR_ANY;
			break;

		case AF_INET6:
			((struct sockaddr_in6*)ifsa)->sin6_addr = in6addr_any;
			break;
		}
		return 0;
	}
	else
	{
		struct pgm_ifaddrs *ifap, *ifa;
		int e = pgm_if_getifaddrs (&ifap);
		if (e < 0) {
			g_trace ("pgm_if_getifaddrs failed when trying to resolve link scope interfaces");
			return -1;
		}

		for (ifa = ifap; ifa; ifa = ifa->ifa_next)
		{
			if ( ifa->ifa_addr->sa_family != iffamily )
				continue;

			unsigned i = if_nametoindex(ifa->ifa_name);
			if (i == ifindex)
			{
				memcpy (ifsa, ifa->ifa_addr, pgm_sockaddr_len(ifa->ifa_addr));
				pgm_if_freeifaddrs (ifap);
				return 0;
			}
		}

		pgm_if_freeifaddrs (ifap);
	}

	return -1;
}

/* 127		=> 127.0.0.0
 * 127.1/8	=> 127.0.0.0
 */

int
pgm_if_inet_network (
	const char* s,
	struct in_addr* in
	)
{
	g_return_val_if_fail (s != NULL, -EINVAL);

	g_trace ("if_inet_network (\"%s\")", s);
	in->s_addr = INADDR_ANY;

	const char *p = s;
	const char *e = p + strlen(s);
	int val = 0;
	int shift = 24;

	while (p <= e)
	{
		if (isdigit(*p)) {
			val = 10 * val + (*p - '0');
		} else if (*p == '.' || *p == 0) {
			if (val > 0xff) {
				in->s_addr = INADDR_NONE;
				return -1;
			}

//g_trace ("elem %i", val);
			
			in->s_addr |= val << shift;
			val = 0;
			shift -= 8;
			if (shift < 0 && *p != 0) {
				in->s_addr = INADDR_NONE;
				return -1;
			}

		} else if (*p == '/') {
			if (val > 0xff) {
				in->s_addr = INADDR_NONE;
				return -1;
			}
//g_trace ("elem %i", val);
			in->s_addr |= val << shift;
			p++; val = 0;
			while (p < e)
			{
				if (isdigit(*p)) {
					val = 10 * val + (*p - '0');
				} else {
					in->s_addr = INADDR_NONE;
					return -1;
				}
				p++;
			}
			if (val == 0 || val > 32) {
				in->s_addr = INADDR_NONE;
				return -1;
			}
//g_trace ("bit mask %i", val);

/* zero out host bits */
			in->s_addr = htonl(in->s_addr);
			while (val < 32) {
//g_trace ("s_addr=%s &= ~(1 << %i)", inet_ntoa(*in), val);
				in->s_addr &= ~(1 << val++);
			}
			return 0;
		
		} else if (*p == 'x' || *p == 'X') {	/* skip number, e.g. 1.x.x.x */
			if (val > 0) {	
				in->s_addr = INADDR_NONE;
				return -1;
			}
			
		} else {
			in->s_addr = INADDR_NONE;
			return -1;
		}
		p++;
	}

	in->s_addr = htonl(in->s_addr);

	return 0;
}

/* ::1/128	=> 0:0:0:0:0:0:0:1
 * ::1          => 0:0:0:0:0:0:0:1
 * ::1.2.3.4	=> 0:0:0:0:1.2.3.4
 */

int
pgm_if_inet6_network (
	const char* s,		/* NULL terminated */
	struct in6_addr* in6
	)
{
	g_return_val_if_fail (s != NULL, -EINVAL);

	g_trace ("if_inet6_network (\"%s\")", s);

/* inet_pton cannot parse IPv6 addresses with subnet declarations, so
 * chop them off.
 */
	char s2[INET6_ADDRSTRLEN];
	const char *p = s;
	char* p2 = s2;
	const char *e = p + strlen(s);
	while (*p) {
		if (*p == '/') break;
		*p2++ = *p++;
	}
	if (p == e) {
		if (inet_pton (AF_INET6, s, in6)) return 0;
		g_trace ("inet_pton failed");
		memcpy (in6, &in6addr_any, sizeof(in6addr_any));
		return -1;
	}

	*p2 = 0;
//	g_trace ("net part %s", s2);
	if (!inet_pton (AF_INET6, s2, in6)) {
		g_trace ("inet_pton failed parsing network part %s", s2);
		memcpy (in6, &in6addr_any, sizeof(in6addr_any));
		return -1;
	}

#ifdef IF_DEBUG
	char sdebug[INET6_ADDRSTRLEN];
	g_trace ("IPv6 network address: %s", inet_ntop(AF_INET6, in6, sdebug, sizeof(sdebug)));
#endif

	p++;
	int val = 0;
	while (p < e)
	{
		if (isdigit(*p)) {
			val = 10 * val + (*p - '0');
		} else {
			g_trace ("failed parsing subnet size due to character '%c'", *p);
			memcpy (in6, &in6addr_any, sizeof(in6addr_any));
			return -1;
		}
		p++;
	}
	if (val == 0 || val > 128) {
		g_trace ("subnet size invalid (%d)", val);
		memcpy (in6, &in6addr_any, sizeof(in6addr_any));
		return -1;
	}
	g_trace ("subnet size %i", val);

/* zero out host bits */
	while (val < 128) {
		int byte_index = val / 8;
		int bit_index  = val % 8;

		in6->s6_addr[byte_index] &= ~(1 << bit_index);
		val++;
	}

	g_trace ("effective IPv6 network address after subnet mask: %s", inet_ntop(AF_INET6, in6, s2, sizeof(s2)));

	return 0;
}

/* parse if name/address
 *
 * e.g.  eth0
 *       1.2.3.4
 *       1.2
 *       abcd::
 *       <hostname>
 *       <nss network name>
 *
 * special addresses should be ignored:
 *
 * local physical link: 169.254.0.0/16, fe80::/64
 * broadcast: 255.255.255.255
 * multicast: 224.0.0.0/4 (224.0.0.0 to 239.255.255.255), ff00::/8
 *
 * We could use if_nametoindex() but we might as well check that the interface is
 * actually UP and capable of multicast traffic.
 *
 * returns 0 on success, -EINVAL on invalid input, -ENODEV on interface not found,
 * -EXDEV if multicast address instead of interface found.
 */

static int
pgm_if_parse_interface (
	const char*		s,		/* NULL terminated */
	int			ai_family,	/* AF_UNSPEC | AF_INET | AF_INET6 */
	struct interface_req*	interface	/* location to write interface details to */
	)
{
	g_return_val_if_fail (s != NULL, -EINVAL);
	g_return_val_if_fail (interface != NULL, -EINVAL);

	g_trace ("if_parse_interface (\"%s\", %s [%i])", 
		s, 
		ai_family == AF_UNSPEC ? "AF_UNSPEC" :
			( ai_family == AF_INET ? "AF_INET" :
				( ai_family == AF_INET6 ? "AF_INET6" : "UNKNOWN" )
			),
		ai_family);

	int retval = 0;
	struct pgm_ifaddrs *ifap, *ifa;

	int e = pgm_if_getifaddrs (&ifap);
	if (e < 0) {
		g_critical ("pgm_if_getifaddrs failed: %s", strerror(e));
		return -EINVAL;
	}

/* search for interface name */
	int interface_matches = 0;
	for (ifa = ifap; ifa; ifa = ifa->ifa_next)
	{
/* ignore raw entries */
#ifdef CONFIG_HAVE_GETIFADDRS
		if ( ifa->ifa_addr->sa_family == AF_PACKET ) continue;
#endif

		if (	ifa->ifa_addr->sa_family != AF_INET && 
			ifa->ifa_addr->sa_family != AF_INET6	)
		{
/* warn if not an IP interface */
			if ( strcmp (s, ifa->ifa_name ) == 0 )
			{
				g_trace ("%s: not an IP interface, sa_family=0x%x", ifa->ifa_name, ifa->ifa_addr->sa_family);

				retval = -EINVAL;
				goto out;
			}

/* just ignore other non-IP interfaces */
			continue;
		}

/* we have an IP interface, check its name, IP and network addresses */
		if ( strcmp (s, ifa->ifa_name ) == 0 )
		{
			int i = if_nametoindex (ifa->ifa_name);
			if (i > 0)
			{
				if (ai_family == AF_UNSPEC) {
					g_trace ("match on interface #%i, %s %s [%i]",
							i, ifa->ifa_name,
							ifa->ifa_addr->sa_family == AF_INET ? "AF_INET" : "AF_INET6",
							ifa->ifa_addr->sa_family);
				} else if (ai_family == ifa->ifa_addr->sa_family) {
					g_trace ("match on interface #%i, %s", i, ifa->ifa_name);
				} else {
					continue;
				}

				interface_matches++;
				strcpy (interface->ir_name, ifa->ifa_name);
				interface->ir_interface = i;
				memcpy (&interface->ir_addr, ifa->ifa_addr, pgm_sockaddr_len(ifa->ifa_addr));

/* check for multiple interfaces */
				continue;
			}

			g_trace("failed lookup via if_nametoindex(\"%s\")", ifa->ifa_name);
			retval = -EINVAL;
			goto out;
		}
	}

	if (interface_matches == 1) {
		retval = 0;
		goto out;
	} else if (interface_matches > 1) {
		g_trace ("multiple interfaces match");
		retval = -ERANGE;
		goto out;
	}

/* check if a valid ipv4 or ipv6 address */
	struct sockaddr_storage addr;
	int valid_ipv4 = 0, valid_ipv6 = 0;
	int valid_net4 = 0, valid_net6 = 0;

	if (inet_pton (AF_INET, s, &((struct sockaddr_in*)&addr)->sin_addr))
	{
		((struct sockaddr_in*)&addr)->sin_family = AF_INET;
		valid_ipv4 = 1;
		if (IN_MULTICAST(g_ntohl(((struct sockaddr_in*)&addr)->sin_addr.s_addr)))
		{
			g_trace ("found IPv4 multicast address instead of interface");
			retval = -EXDEV;
			goto out;
		}
	}
	else if (inet_pton (AF_INET6, s, &((struct sockaddr_in6*)&addr)->sin6_addr))
	{
		((struct sockaddr_in6*)&addr)->sin6_family = AF_INET6;
		valid_ipv6 = 1;
		if (IN6_IS_ADDR_MULTICAST(&((struct sockaddr_in6*)&addr)->sin6_addr))
		{
			g_trace ("found IPv6 multicast address instead of interface");
			retval = -EXDEV;
			goto out;
		}
	}

/* IPv6 friendly version??? */
	struct in_addr in;
#if 0
	in.s_addr = inet_network (s);
	if (in.s_addr != -1) {
		g_trace ("network address calculated: %s", inet_ntoa (in));
	}
#else
	e = pgm_if_inet_network (s, &in);
	if (e != -1) {
		g_trace ("IPv4 network address calculated: %s", inet_ntoa (in));
		valid_net4 = 1;
	}

	struct in6_addr in6;
	e = pgm_if_inet6_network (s, &in6);
	if (e != -1) {
#ifdef IF_DEBUG
		char sdebug[INET6_ADDRSTRLEN];
		g_trace ("IPv6 network address calculated: %s", inet_ntop(AF_INET6, &in6, sdebug, sizeof(sdebug)));
#endif
		valid_net6 = 1;
	}

#endif

	if (! (valid_ipv4 || valid_ipv6 || valid_net4 || valid_net6) )
	{

/* check IP NSS networks for a network name */
		struct netent* ne = getnetbyname (s);
		if (ne) {
			switch (ne->n_addrtype) {
			case AF_INET:
			{
				g_trace ("found IPv4 network by NSS: %s", ne->n_name);
				in.s_addr = g_htonl(ne->n_net);
				g_trace ("address %s", inet_ntoa (in));
				valid_net4 = 1;
				if (IN_MULTICAST(g_ntohl(in.s_addr)))
				{
					g_trace ("NSS erroneously contains IPv4 multicast group instead of network address.");
					retval = -EINVAL;
					goto out;
				}
			}
			break;

			case AF_INET6:
			{
				g_trace ("found IPv6 network by NSS: %s", ne->n_name);
				memcpy (&in6,
					&ne->n_net,
					sizeof(struct in6_addr));
#ifdef IF_DEBUG
				char sdebug[INET6_ADDRSTRLEN];
				g_trace ("address %s", inet_ntop (ne->n_addrtype, &in6, sdebug, sizeof(sdebug)));
#endif
				valid_net6 = 1;
				if (IN6_IS_ADDR_MULTICAST(&in6))
				{
					g_trace ("NSS erroneously contains IPv6 multicast group instead of network address.");
					retval = -EINVAL;
					goto out;
				}
			}
			break;

			default:
				g_trace ("unknown network address type %i from getnetbyname().", ne->n_addrtype);
				break;
			}
		}
	}

	if (! (valid_ipv4 || valid_ipv6 || valid_net4 || valid_net6) )
	{
		g_trace ("cannot find valid node or network address, trying DNS resolution on entity.");

/* DNS resolve to see if valid hostname */
		struct addrinfo hints;
		struct addrinfo *res = NULL;

		memset (&hints, 0, sizeof(hints));
		hints.ai_family = ai_family;
/*		hints.ai_protocol = IPPROTO_PGM; */
/*		hints.ai.socktype = SOCK_RAW; */
		hints.ai_flags  = AI_ADDRCONFIG | AI_CANONNAME; /* AI_V4MAPPED is probably stupid here */
		int err = getaddrinfo (s, NULL, &hints, &res);

		if (!err) {
			switch (res->ai_family) {
			case AF_INET:
				((struct sockaddr_in*)&addr)->sin_addr = ((struct sockaddr_in*)(res->ai_addr))->sin_addr;
				((struct sockaddr_in*)&addr)->sin_family = res->ai_family;
				valid_ipv4 = 1;
				g_trace ("entity resolved as a IPv4 address");
				if (IN_MULTICAST(g_ntohl(((struct sockaddr_in*)&addr)->sin_addr.s_addr)))
				{
					g_trace ("entity is a IPv4 multicast DNS name.");
					retval = -EXDEV;
					goto out;
				}
				break;

			case AF_INET6:
				((struct sockaddr_in6*)&addr)->sin6_addr = ((struct sockaddr_in6*)(res->ai_addr))->sin6_addr;
				((struct sockaddr_in6*)&addr)->sin6_family = res->ai_family;
				valid_ipv6 = 1;
				g_trace ("entity resolved as a IPv6 address");
				if (IN6_IS_ADDR_MULTICAST(&((struct sockaddr_in6*)&addr)->sin6_addr))
				{
					g_trace ("found IPv6 multicast address instead of interface");
					retval = -EXDEV;
					goto out;
				}
				break;

			default:
				g_trace ("getaddrinfo returned %i on ai_family", res->ai_family);
				g_assert_not_reached();
			}

			freeaddrinfo (res);
		} else {
			return -EINVAL;
		}
	}

/* iterate through interface list again to match ip or net address */
	if (valid_ipv4 || valid_ipv6 || valid_net4 || valid_net6)
	{
		g_trace ("searching for matching interface");
		for (ifa = ifap; ifa; ifa = ifa->ifa_next)
		{
			switch (ifa->ifa_addr->sa_family) {
			case AF_INET:
				if (valid_ipv4 &&
					((struct sockaddr_in*)ifa->ifa_addr)->sin_addr.s_addr == ((struct sockaddr_in*)&addr)->sin_addr.s_addr )
				{
					g_trace ("IPv4 address on %i:%s",
							if_nametoindex (ifa->ifa_name),
							ifa->ifa_name );

/* copy interface ip address */
					strcpy (interface->ir_name, ifa->ifa_name);
					interface->ir_interface = if_nametoindex (ifa->ifa_name);
					((struct sockaddr*)&interface->ir_addr)->sa_family = ifa->ifa_addr->sa_family;
					((struct sockaddr_in*)&interface->ir_addr)->sin_port = 0;
					((struct sockaddr_in*)&interface->ir_addr)->sin_addr.s_addr = ((struct sockaddr_in*)ifa->ifa_addr)->sin_addr.s_addr;
					retval = 0;
					goto out;
				}

/* check network address */
				if (valid_net4)
				{
					struct in_addr netaddr;
					netaddr.s_addr = ((struct sockaddr_in*)ifa->ifa_netmask)->sin_addr.s_addr &
							((struct sockaddr_in*)ifa->ifa_addr)->sin_addr.s_addr;

					if (in.s_addr == netaddr.s_addr)
					{
						g_trace ("IPv4 net address on %i:%s",
							if_nametoindex (ifa->ifa_name),
							ifa->ifa_name );

						strcpy (interface->ir_name, ifa->ifa_name);
						interface->ir_interface = if_nametoindex (ifa->ifa_name);
						((struct sockaddr*)&interface->ir_addr)->sa_family = AF_INET;
						((struct sockaddr_in*)&interface->ir_addr)->sin_port = 0;
						((struct sockaddr_in*)&interface->ir_addr)->sin_addr.s_addr = ((struct sockaddr_in*)ifa->ifa_addr)->sin_addr.s_addr;
						retval = 0;
						goto out;
					}
				}
				break;

			case AF_INET6:
				if (valid_ipv6 &&
					memcmp (&((struct sockaddr_in6*)ifa->ifa_addr)->sin6_addr,
						&((struct sockaddr_in6*)&addr)->sin6_addr,
						sizeof(struct in6_addr)) == 0)
				{
					g_trace ("IPv6 address on %i:%s",
							if_nametoindex (ifa->ifa_name),
							ifa->ifa_name );

					strcpy (interface->ir_name, ifa->ifa_name);
					interface->ir_interface = if_nametoindex (ifa->ifa_name);
					((struct sockaddr*)&interface->ir_addr)->sa_family = AF_INET6;
					((struct sockaddr_in6*)&interface->ir_addr)->sin6_port = 0;
					((struct sockaddr_in6*)&interface->ir_addr)->sin6_flowinfo = 0;
					((struct sockaddr_in6*)&interface->ir_addr)->sin6_addr = ((struct sockaddr_in6*)ifa->ifa_addr)->sin6_addr;
					((struct sockaddr_in6*)&interface->ir_addr)->sin6_scope_id = 0;
					retval = 0;
					goto out;
				}

/* check network address */
				if (valid_net6)
				{
					struct in6_addr ipaddr6  = ((struct sockaddr_in6*)ifa->ifa_addr)->sin6_addr;
					struct in6_addr netaddr6 = ((struct sockaddr_in6*)ifa->ifa_netmask)->sin6_addr;

					int invalid = 0;
					for (int i = 0; i < 16; i++)
					{
						if ((netaddr6.s6_addr[i] & ipaddr6.s6_addr[i]) != in6.s6_addr[i])
						{
							invalid = 1;
							break;
						}
					}
					if (!invalid)
					{
						g_trace ("IPv6 net address on %i:%s",
							if_nametoindex (ifa->ifa_name),
							ifa->ifa_name );

						strcpy (interface->ir_name, ifa->ifa_name);
						interface->ir_interface = if_nametoindex (ifa->ifa_name);
						((struct sockaddr*)&interface->ir_addr)->sa_family = AF_INET6;
						((struct sockaddr_in6*)&interface->ir_addr)->sin6_port = 0;
						((struct sockaddr_in6*)&interface->ir_addr)->sin6_flowinfo = 0;
						((struct sockaddr_in6*)&interface->ir_addr)->sin6_addr = ((struct sockaddr_in6*)ifa->ifa_addr)->sin6_addr;
						((struct sockaddr_in6*)&interface->ir_addr)->sin6_scope_id = 0;
						retval = 0;
						goto out;
					}
				}
				break;

			default: continue;
			}
		}
		g_trace ("no matching interfaces found!");
		retval = -ENODEV;
	}
	else
	{
		retval = -EINVAL;
	}

out:

/* cleanup after pgm_getifaddrs() */
	pgm_if_freeifaddrs (ifap);

	return retval;
}

/* parse one multicast address, conflict resolution of multiple address families of DNS multicast names is
 * deferred to libc.
 *
 * reserved addresses may flag warnings:
 *
 * 224.0.0.0/24 for local network control
 * 224.0.1/24 for internetwork control
 * 169.254.255.255, ff02::1 all local nodes on segment
 * ff02::2 all routers
 * ff05::1 all nodes
 * ff0x::fb multicast DNS
 * ff0x::108 NIS
 * ff05::1:3 DHCP
 */

static int
pgm_if_parse_multicast (
	const char*		s,		/* NULL terminated */
	int			ai_family,	/* AF_UNSPEC | AF_INET | AF_INET6 */
	struct sockaddr*	addr		/* pointer to sockaddr_storage for writing */
	)
{
	g_return_val_if_fail (s != NULL, -EINVAL);

	g_trace ("if_parse_multicast (\"%s\", %s [%i])", 
		s, 
		ai_family == AF_UNSPEC ? "AF_UNSPEC" :
			( ai_family == AF_INET ? "AF_INET" :
				( ai_family == AF_INET6 ? "AF_INET6" : "UNKNOWN" )
			),
		ai_family);

	int retval = 0;

/* IPv4 address */
	if (inet_pton (AF_INET, s, &((struct sockaddr_in*)addr)->sin_addr))
	{
		addr->sa_family = AF_INET;
		if (IN_MULTICAST(ntohl(((struct sockaddr_in*)addr)->sin_addr.s_addr)))
		{
			g_trace ("IPv4 multicast: %s", s);
		}
		else
		{
			g_trace ("IPv4 unicast: %s", s);
			retval = -EINVAL;
		}
	}
	else if (inet_pton (AF_INET6, s, &((struct sockaddr_in6*)addr)->sin6_addr))
	{
		addr->sa_family = AF_INET6;

		if (IN6_IS_ADDR_MULTICAST(&((struct sockaddr_in6*)addr)->sin6_addr))
		{
			g_trace ("IPv6 multicast: %s", s);
		}
		else
		{
			g_trace ("IPv6 unicast: %s", s);
			retval = -EINVAL;
		}
	}
	else
	{
/* try to resolve the name instead */
		struct addrinfo hints;
		struct addrinfo *res = NULL;

		memset (&hints, 0, sizeof(hints));
		hints.ai_family = ai_family;
/*		hints.ai_protocol = IPPROTO_PGM; */
/*		hints.ai.socktype = SOCK_RAW; */
		hints.ai_flags  = AI_ADDRCONFIG | AI_CANONNAME; /* AI_V4MAPPED is probably stupid here */
		int err = getaddrinfo (s, NULL, &hints, &res);

		if (!err) {
#ifdef IF_DEBUG
			char s2[INET6_ADDRSTRLEN];
			g_trace ("DNS hostname: (A) %s address %s",
				res->ai_canonname,
				inet_ntop (res->ai_family, 
						res->ai_family == AF_INET ?
							(const void*)&((struct sockaddr_in*)(res->ai_addr))->sin_addr :
							(const void*)&((struct sockaddr_in6*)(res->ai_addr))->sin6_addr,
						s2, sizeof(s2)) );
#endif

			if (res->ai_family == AF_INET)
			{
				addr->sa_family = AF_INET;

				if (IN_MULTICAST(ntohl(((struct sockaddr_in*)(res->ai_addr))->sin_addr.s_addr)))
				{
					g_trace ("IPv4 multicast");
					((struct sockaddr_in*)addr)->sin_addr.s_addr = 
						((struct sockaddr_in*)(res->ai_addr))->sin_addr.s_addr;
				}
				else
				{
					g_trace ("IPv4 unicast");
					retval = -EINVAL;
				}
			}
			else
			{
				addr->sa_family = AF_INET6;

				if (IN6_IS_ADDR_MULTICAST(&((struct sockaddr_in6*)res->ai_addr)->sin6_addr))
				{
					g_trace ("IPv6 multicast");

					((struct sockaddr_in6*)addr)->sin6_port = 0;
					((struct sockaddr_in6*)addr)->sin6_flowinfo = 0;
					memcpy (&((struct sockaddr_in6*)addr)->sin6_addr,
						&((struct sockaddr_in6*)(res->ai_addr))->sin6_addr,
						sizeof(struct in6_addr));
					((struct sockaddr_in6*)addr)->sin6_scope_id = 0;
				}
				else
				{
					g_trace ("IPv6 unicast");
					retval = -EINVAL;
				}
			}
		}
		else
		{
			g_trace ("DNS resolution failed.");
			retval = -EINVAL;
		}

		freeaddrinfo (res);
	}

	return retval;
}

/* parse an interface entity from a network parameter.
 *
 * family can be unspecified - AF_UNSPEC, can return interfaces with the unspecified
 * address family
 *
 * examples:  "eth0"
 * 	      "hme0,hme1"
 * 	      "qe0,qe1,qe2"
 * 	      "qe0,qe2,qe2"	=> valid even though duplicate interface name
 *
 * returns 0 on success with device_list containing double linked list of devices as
 * sockaddr/idx pairs.  returns -ERANGE, device multicast group address family with be
 * AF_UNSPEC when multiple matching adapters have been discovered.  returns -EINVAL on
 * invalid input, -ENODEV if a device could not be found, and -EXDEV if a multicast group
 * is resolved instead of a node address.
 *
 * memory ownership of linked list is passed to caller and must be freed with g_free
 * and the g_list_free* api.
 */

static
int
pgm_if_parse_entity_interface (
	const char*		s,		/* NULL terminated */
	int			ai_family,	/* AF_UNSPEC | AF_INET | AF_INET6 */
	GList**			interface_list	/* <struct interface_req*> */
	)
{
	g_assert (g_list_length(*interface_list) == 0);

	int retval = 0;
	struct interface_req* ir;

/* the empty entity, returns in_addr_any for both receive and send interfaces */
	if (NULL == s)
	{
		g_trace ("interface = (nul)");

		ir = g_new0(struct interface_req, 1);
		((struct sockaddr*)&ir->ir_addr)->sa_family = ai_family;
		*interface_list = g_list_append (*interface_list, ir);
		goto out;
	}

/* check interface name length limit */
	gchar** tokens = g_strsplit (s, ",", 10);
	int j = 0;
	while (tokens && tokens[j])
	{
		ir = g_new(struct interface_req, 1);

		retval = pgm_if_parse_interface (tokens[j], ai_family, ir);
/* mark multiple interfaces for later decision based on group families */
		if (retval == -ERANGE)
		{
			((struct sockaddr*)&ir->ir_addr)->sa_family = AF_UNSPEC;
		}
/* bail out on first interface with an error */
		else if (retval != 0)
		{
			g_free (ir);
			g_strfreev (tokens);
			goto out;
		}

		*interface_list = g_list_append (*interface_list, ir);
		++j;
	}

	g_strfreev (tokens);

out:
	return retval;
}

#if 0
static
int 
strcnt (
	const char *s,
	const char c
	)
{
	int tokens = 0;

	if (s)
	{
		do {
			if (*s == c)
				++tokens;
		} while (*s++ != '\0');
	}

	return tokens;
}
#endif

/* parse a receive multicast group entity.  can contain more than one multicast group to
 * support asymmetric fan-out.
 *
 * if group is ambiguous, i.e. empty or a name mapping then the address family of the matching
 * interface is queried.  if the interface is also ambiguous, i.e. empty interface and receive group
 * then the hostname will be used to determine the default node address family.  if the hosts
 * node name resolves both IPv4 and IPv6 address families then the first matching value is taken.
 *
 * e.g. "239.192.0.1"
 * 	"239.192.0.100,239.192.0.101"
 *
 * unspecified address family interfaces are forced to AF_INET or AF_INET6.
 */

static
int
pgm_if_parse_entity_receive (
	const char*		s,		/* NULL terminated */
	int			ai_family,	/* AF_UNSPEC | AF_INET | AF_INET6 */
	GList**			interface_list,	/* <struct interface_req*> */
	GList**			recv_list	/* <struct group_source_req*> */
	)
{
	g_assert (g_list_length(*interface_list) == 1);		/* TODO: support multiple interfaces */
	g_assert (g_list_length(*recv_list) == 0);

	int retval = 0;
	struct group_source_req* gsr;
	struct interface_req* interface = (struct interface_req*)g_memdup((*interface_list)->data, sizeof(struct interface_req));

/* the empty entity */
	if (NULL == s)
	{
		g_trace ("receive group = (nul)");

/* default receive object */
		gsr = g_new0(struct group_source_req, 1);
		gsr->gsr_interface = interface->ir_interface;
		((struct sockaddr*)&gsr->gsr_group)->sa_family = ai_family;

/* if using unspec default group check the interface for address family
 */
		if (AF_UNSPEC == ((struct sockaddr*)&gsr->gsr_group)->sa_family)
		{
			if (AF_UNSPEC == ((struct sockaddr*)&interface->ir_addr)->sa_family)
			{
				g_trace ("Cannot determine address family to use from group or source, resolving nodename.");

/* find the default address family for this node using the hostname
 */
				char hostname[NI_MAXHOST + 1];
				struct addrinfo hints;
				struct addrinfo *res = NULL;
				gethostname (hostname, sizeof(hostname));
				memset (&hints, 0, sizeof(hints));
				hints.ai_family = AF_UNSPEC;
				hints.ai_flags  = AI_ADDRCONFIG | AI_CANONNAME;
				retval = getaddrinfo (hostname, NULL, &hints, &res);
				if (0 > retval)
				{
					g_trace ("Cannot resolve nodename");
					g_free (gsr);
					retval = -ENODEV;
					goto out;
				}

				g_trace ("Assuming address family %s from node address.",
					res->ai_family == AF_INET ? "AF_INET" :
						( res->ai_family == AF_INET6 ? "AF_INET6" : "UNKNOWN" ));
				((struct sockaddr*)&gsr->gsr_group)->sa_family = res->ai_family;
				freeaddrinfo (res);

/* was an interface actually specified */
				if (interface->ir_name[0] != '\0')
				{
					struct interface_req ir;
					g_trace ("Re-resolving interface name using nodename address family.");
					retval = pgm_if_parse_interface (interface->ir_name, ((struct sockaddr*)&gsr->gsr_group)->sa_family, &ir);
					if (0 > retval)
					{
						g_trace ("Address family of interface \"%s\" does not match default node interface", 
								interface->ir_name);
						g_free (gsr);
						retval = -ENODEV;
						goto out;
					}

					gsr->gsr_interface = ir.ir_interface;
					memcpy(&interface->ir_addr, &ir.ir_addr, pgm_sockaddr_len(&ir.ir_addr));
				}
			}
			else
			{
/* use source address family for multicast group */
				((struct sockaddr*)&gsr->gsr_group)->sa_family = ((struct sockaddr*)&interface->ir_addr)->sa_family;
			}
		}


		g_assert (AF_UNSPEC != ((struct sockaddr*)&gsr->gsr_group)->sa_family);
		if (AF_UNSPEC != ((struct sockaddr*)&interface->ir_addr)->sa_family)
		{
			g_assert (((struct sockaddr*)&gsr->gsr_group)->sa_family == ((struct sockaddr*)&interface->ir_addr)->sa_family);
		}
		else
		{
/* check if we can now resolve the source interface by address family of the receive group */
			if (interface->ir_name[0] != '\0')
			{
				struct interface_req ir;
				g_trace ("Re-resolving interface name using the detected receive group address family.");
				retval = pgm_if_parse_interface (interface->ir_name, ((struct sockaddr*)&gsr->gsr_group)->sa_family, &ir);
				if (-ERANGE == retval)
				{
					if (AF_INET == ((struct sockaddr*)&gsr->gsr_group)->sa_family)
					{
						g_trace ("System configuration error, multiple interfaces found matching the name \"%s\".", interface->ir_name);
					}
					else
					{
						g_trace ("Multiple interfaces match the name \"%s\", if multiple IPv6 scopes are defined please specify the scope by IP or network address instead of name.", interface->ir_name);
					}
					g_free (gsr);
					goto out;
				}
				else if (0 > retval)
				{
					g_trace ("Address family of interface \"%s\" does not match the detected receive group", interface->ir_name);
					g_free (gsr);
					goto out;
				}

				gsr->gsr_interface = ir.ir_interface;
			}
		}

/* copy default PGM multicast group */
		switch (((struct sockaddr*)&gsr->gsr_group)->sa_family) {
		case AF_INET6:
			memcpy (&((struct sockaddr_in6*)&gsr->gsr_group)->sin6_addr,
				&if6_default_group_addr,
				sizeof(if6_default_group_addr));
			break;

		case AF_INET:
			((struct sockaddr_in*)&gsr->gsr_group)->sin_addr.s_addr = htonl(IF_DEFAULT_GROUP);
			break;
	
		default:
			g_assert_not_reached();
		}

#ifdef IF_DEBUG
		char s2[INET6_ADDRSTRLEN];
		g_trace ("Assigning default receive group address %s.",
				inet_ntop (pgm_sockaddr_family(&gsr->gsr_group), pgm_sockaddr_addr(&gsr->gsr_group),
						s2, sizeof(s2)) );
#endif
		g_assert(0 == retval);

/* ASM: source = group */
		memcpy(&gsr->gsr_source, &gsr->gsr_group, pgm_sockaddr_len(&gsr->gsr_group));

		*recv_list = g_list_append (*recv_list, gsr);
		goto out;
	}

/* parse one or more multicast receive groups.
 */

	int j = 0;	
	gchar** tokens = g_strsplit (s, ",", 10);
	while (tokens && tokens[j])
	{
/* default receive object */
		gsr = g_new0(struct group_source_req, 1);
		gsr->gsr_interface = interface->ir_interface;
		((struct sockaddr*)&gsr->gsr_group)->sa_family = ai_family;

		if (AF_UNSPEC == ((struct sockaddr*)&gsr->gsr_group)->sa_family)
		{
			if (AF_UNSPEC == ((struct sockaddr*)&interface->ir_addr)->sa_family)
			{
				g_trace ("Multiple address family resolution of multicast group deferred to libc.");
			}
			else
			{
				g_trace ("Hinting multicast group resolution with interface address family.");
				((struct sockaddr*)&gsr->gsr_group)->sa_family = ((struct sockaddr*)&interface->ir_addr)->sa_family;
			}
		}

		retval = pgm_if_parse_multicast (tokens[j], ((struct sockaddr*)&gsr->gsr_group)->sa_family, (struct sockaddr*)&gsr->gsr_group);
		if (retval != 0) {
			g_strfreev (tokens);
			g_free (gsr);
			goto out;
		}

/* check if we can now resolve the source interface by address family of the receive group */
		if (AF_UNSPEC == ((struct sockaddr*)&interface->ir_addr)->sa_family)
		{
			if (interface->ir_name[0] != '\0')
			{
				struct interface_req ir;
				g_trace ("Re-resolving interface name using receive multicast group address family.");
				retval = pgm_if_parse_interface (interface->ir_name, ((struct sockaddr*)&gsr->gsr_group)->sa_family, &ir);
				if (-ERANGE == retval)
				{
					if (AF_INET == ((struct sockaddr*)&gsr->gsr_group)->sa_family)
					{
						g_trace ("System configuration error, multiple interfaces found matching the name \"%s\".", interface->ir_name);
					}
					else
					{
						g_trace ("Multiple interfaces match the name \"%s\", if multiple IPv6 scopes are defined please specify the scope by IP or network address instead of name.", interface->ir_name);
					}
					g_free (gsr);
					goto out;
				}
				else if (0 > retval)
				{
					g_trace ("Address family of interface \"%s\" does not match specified receive multicast group",
							interface->ir_name);
					g_free (gsr);
					retval = -EINVAL;
					goto out;
				}

				gsr->gsr_interface = ir.ir_interface;
			}
		}

/* ASM: source = group */
		memcpy(&gsr->gsr_source, &gsr->gsr_group, pgm_sockaddr_len(&gsr->gsr_group));

		*recv_list = g_list_append (*recv_list, gsr);
		++j;
	}

	g_strfreev (tokens);

out:
	g_free (interface);
	return retval;
}

static
int
pgm_if_parse_entity_send (
	const char*		s,		/* null = empty entity */
	int			ai_family,	/* AF_UNSPEC | AF_INET | AF_INET6 */
	GList**			interface_list,	/* <struct interface_req*> */
	GList**			recv_list,	/* <struct group_source_req*> */
	GList**			send_list	/* <struct group_source_req*> */
	)
{
	g_assert (g_list_length(*interface_list) == 1);		/* TODO: support multiple interfaces */
	g_assert (g_list_length(*recv_list) > 0);
	g_assert (g_list_length(*send_list) == 0);

	int retval = 0;
	struct group_source_req* gsr;
	struct interface_req* interface = (struct interface_req*)(*interface_list)->data;

	if (s == NULL)
	{
		g_trace ("send group = (nul)");

		if (g_list_length(*recv_list) > 1)
		{
			g_trace ("Send group needs to be explicitly defined when requesting multiple receive groups.");
			retval = -EINVAL;
			goto out;
		}

		g_trace ("Send entity defaults to receive entity settings.");
		gsr = g_memdup ((*recv_list)->data, sizeof(struct group_source_req));
		*send_list = g_list_append (*send_list, gsr);
		goto out;
	}

/* default send object */
	gsr = g_new0(struct group_source_req, 1);
	gsr->gsr_interface = interface->ir_interface;

	retval = pgm_if_parse_multicast (s, ai_family, (struct sockaddr*)&gsr->gsr_group);
	if (0 != retval) {
		g_free (gsr);
		goto out;
	}

/* check if we can now resolve the source interface by address family of the send group */
	if (AF_UNSPEC == ((struct sockaddr*)&interface->ir_addr)->sa_family)
	{
		if (interface->ir_name[0] != '\0')
		{
			struct interface_req ir;
			g_trace ("Re-resolving interface name using send multicast group address family.");
			retval = pgm_if_parse_interface (interface->ir_name, ((struct sockaddr*)&gsr->gsr_group)->sa_family, &ir);
			if (0 > retval)
			{
				g_trace ("Address family of interface \"%s\" does not match specified send multicast group",
						interface->ir_name);
				return -EINVAL;
			}

			gsr->gsr_interface = ir.ir_interface;
		}
	}

/* ASM: source = group */
	memcpy(&gsr->gsr_source, &gsr->gsr_group, pgm_sockaddr_len(&gsr->gsr_group));

	*send_list = g_list_append (*send_list, gsr);

out:
	return retval;
}

/* parse network parameter
 *
 * interface list; receive multicast group list; send multicast group
 *
 * TODO: reply with linked list of devices & groups.
 * TODO: split receive/send interfaces, ensure matching interface to multicast group.
 *
 * TODO: create function to determine whether this host is default IPv4 or IPv6 and use that instead of IPv4.
 */

#define IS_HOSTNAME(x) ( 				/* RFC 952 */ \
				isalnum(x) || \
				((x) == '-') || \
				((x) == '.') \
			)
#define IS_IP(x) ( \
				isdigit(x) || \
				((x) == '.') || \
				((x) == '/') \
			)
#define IS_IP6(x) ( \
				isxdigit(x) || \
				((x) == ':') || \
				((x) == '/') || \
				((x) == '.') \
			)
/* e.g. fe80::1%eth0.620    vlan tag,
 *      fe80::1%eth0:0      IP alias
 *      fe80::1%qe0_0       Solaris link name
 *
 * The Linux kernel generally doesn't care too much, but everything else falls apart with
 * random characters in interface names.  Hyphen is a popular problematic character.
 */
#define IS_IP6_WITH_ZONE(x) ( \
				IS_IP6(x) || \
				((x) == '%') || \
				isalpha(x) || \
				((x) == '_') \
			    )
#define IS_NETPARAM(x) ( \
				((x) == ',') || \
				((x) == ';') \
			)

static
int
pgm_if_parse_network (
	const char*		s,			/* NULL terminated */
	int			ai_family,		/* AF_UNSPEC | AF_INET | AF_INET6 */
	GList**			recv_list,		/* <struct group_source_req*> */
	GList**			send_list		/* <struct group_source_req*> */
	)
{
	g_trace ("if_parse_network (\"%s\", %s [%i], [receive list], [send list])", 
		s, 
		ai_family == AF_UNSPEC ? "AF_UNSPEC" :
			( ai_family == AF_INET ? "AF_INET" :
				( ai_family == AF_INET6 ? "AF_INET6" : "UNKNOWN" )
			),
		ai_family);

	const char *p = s;
	const char *e = p + strlen(s);
	enum { ENTITY_INTERFACE, ENTITY_RECEIVE, ENTITY_SEND, ENTITY_ERROR } ec = ENTITY_INTERFACE;
	const char *b = p;		/* begin of entity */
	int retval = 0;

	GList* source_list = NULL;

	while (p < e)
	{
		if (!IS_HOSTNAME(*p) && !IS_IP(*p) && !IS_IP6_WITH_ZONE(*p) && !IS_NETPARAM(*p))
		{
			g_trace ("invalid character 0x%x", *p);
			retval = -EINVAL;
			goto out;
		}

		if (*p == ';')		/* end of entity */
		{
			if (b == p)	/* empty entity */
			{
				g_trace ("empty entity");
				switch (ec++) {
				case ENTITY_INTERFACE:
					retval = pgm_if_parse_entity_interface (NULL, ai_family, &source_list);
					break;

				case ENTITY_RECEIVE:
					retval = pgm_if_parse_entity_receive (NULL, ai_family, &source_list, recv_list);
					break;

				case ENTITY_SEND:
					retval = pgm_if_parse_entity_send (NULL, ai_family, &source_list, recv_list, send_list);
					break;

				default:
					g_assert_not_reached();
					break;
				}

				if (retval != 0) goto out;

				b = ++p;
				continue;
			}

			/* entity from b to p-1 */
			char entity[1024];
			strncpy (entity, b, sizeof(entity));

//			char *entity = malloc (p - b + 1);
//			strncpy (entity, b, p - b);

			entity[p - b] = 0;

			g_trace ("entity:0 '%s'", entity);
			switch (ec++) {
			case ENTITY_INTERFACE:
				retval = pgm_if_parse_entity_interface (entity, ai_family, &source_list);
/* fall through on multicast */
				if (-EXDEV != retval)
				{
					if (!(retval == 0 || retval == -ERANGE)) goto out;

/* FIXME: too many interfaces */
					if (g_list_length(source_list) > 1) {
						retval = -EINVAL;
						goto out;
					}
					break;
				}
				retval = pgm_if_parse_entity_interface (NULL, ai_family, &source_list);
				if (!(retval == 0 || retval == -ERANGE)) goto out;
				ec++;

			case ENTITY_RECEIVE:
				retval = pgm_if_parse_entity_receive (entity, ai_family, &source_list, recv_list);
				if (retval != 0) goto out;
				break;

			case ENTITY_SEND:
				retval = pgm_if_parse_entity_send (entity, ai_family, &source_list, recv_list, send_list);
				if (retval != 0) goto out;
				break;

			default:
				g_assert_not_reached();
				break;
			}
				
//			free (entity);

			b = ++p;
			continue;
		}

		p++;
	}

	if (b < e) {
		g_trace ("entity:1 '%s'", b);
		switch (ec++) {
		case ENTITY_INTERFACE:
			retval = pgm_if_parse_entity_interface (b, ai_family, &source_list);
/* fall through on multicast */
			if (-EXDEV != retval)
			{
				if (!(retval == 0 || retval == -ERANGE)) goto out;

/* FIXME: too many interfaces */
					if (g_list_length(source_list) > 1) {
						retval = -EINVAL;
						goto out;
					}
				break;
			}
			retval = pgm_if_parse_entity_interface (NULL, ai_family, &source_list);
			if (!(retval == 0 || retval == -ERANGE)) goto out;
			ec++;

		case ENTITY_RECEIVE:
			retval = pgm_if_parse_entity_receive (b, ai_family, &source_list, recv_list);
			if (retval != 0) goto out;
			break;

		case ENTITY_SEND:
			retval = pgm_if_parse_entity_send (b, ai_family, &source_list, recv_list, send_list);
			if (retval != 0) goto out;
			break;

		default:
			g_assert_not_reached();
			break;
		}
	}

	while (ec <= ENTITY_SEND)
	{
		g_trace ("assumed entity");
		switch (ec++) {
		case ENTITY_INTERFACE:
			retval = pgm_if_parse_entity_interface (NULL, ai_family, &source_list);
			break;

		case ENTITY_RECEIVE:
			retval = pgm_if_parse_entity_receive (NULL, ai_family, &source_list, recv_list);
			break;

		case ENTITY_SEND:
			retval = pgm_if_parse_entity_send (NULL, ai_family, &source_list, recv_list, send_list);
			break;

		default:
			g_assert_not_reached();
			break;
		}

		if (retval != 0) goto out;
	}

/* cleanup source interface list */
	while (source_list) {
		g_free (source_list->data);
		source_list = g_list_delete_link (source_list, source_list);
	}

	return retval;

out:
	while (source_list) {
		g_free (source_list->data);
		source_list = g_list_delete_link (source_list, source_list);
	}
	while (*recv_list) {
		g_free ((*recv_list)->data);
		*recv_list = g_list_delete_link (*recv_list, *recv_list);
	}
	while (*send_list) {
		g_free ((*send_list)->data);
		*send_list = g_list_delete_link (*send_list, *send_list);
	}

	return retval;
}

/* create group_source_req as used by pgm_transport_create which specify port, address & interface.
 *
 * gsr_source is copied from gsr_group for ASM, caller needs to populate gsr_source for SSM.
 */

int
pgm_if_parse_transport (
	const char*			s,
	int				ai_family,	/* AF_UNSPEC | AF_INET | AF_INET6 */
	struct group_source_req*	recv_gsr,
	gsize*				recv_len,	/* length of incoming gsr and filled in returning */
	struct group_source_req*	send_gsr
	)
{
	g_return_val_if_fail (s != NULL, -EINVAL);
	g_return_val_if_fail (ai_family == AF_UNSPEC || ai_family == AF_INET || ai_family == AF_INET6, -EINVAL);
	g_return_val_if_fail (recv_gsr != NULL, -EINVAL);
	g_return_val_if_fail (recv_len != NULL, -EINVAL);
	g_return_val_if_fail (*recv_len > 0, -EINVAL);
	g_return_val_if_fail (send_gsr != NULL, -EINVAL);

	g_trace ("if_parse_transport (\"%s\", %s [%i], ..., %" G_GSIZE_FORMAT ")", 
		s, 
		ai_family == AF_UNSPEC ? "AF_UNSPEC" :
			( ai_family == AF_INET ? "AF_INET" :
				( ai_family == AF_INET6 ? "AF_INET6" : "UNKNOWN" )
			),
		ai_family, *recv_len);
	int retval = 0;

/* resolve network string */
	GList* recv_gsr_list = NULL;	/* <struct group_source_req*> */
	GList* send_gsr_list = NULL;	/* <struct group_source_req*> */

	retval = pgm_if_parse_network (s, ai_family, &recv_gsr_list, &send_gsr_list);
	if (retval == 0)
	{
		if (g_list_length(recv_gsr_list) > *recv_len)
		{
			g_trace ("Receive group list too long for provided buffer.");
			retval = -ENOMEM;
			goto out;
		}

		if (g_list_length(send_gsr_list) > 1)
		{
			g_trace ("Send group list too long.");
			retval = -ENOMEM;
			goto out;
		}

/* receive side
 */
		gsize i;
		for (i = 0; i < *recv_len; i++)
		{
			if (recv_gsr_list == NULL) break;

			memcpy (&recv_gsr[i], recv_gsr_list->data, sizeof(struct group_source_req));
			g_free (recv_gsr_list->data);
			recv_gsr_list = g_list_delete_link (recv_gsr_list, recv_gsr_list);
		}

/* store count of copied buffers */
		*recv_len = i;

/* send side
 */
		memcpy (send_gsr, send_gsr_list->data, sizeof(struct group_source_req));
	}

out:
	while (recv_gsr_list) {
		g_free (recv_gsr_list->data);
		recv_gsr_list = g_list_delete_link (recv_gsr_list, recv_gsr_list);
	}
	while (send_gsr_list) {
		g_free (send_gsr_list->data);
		send_gsr_list = g_list_delete_link (send_gsr_list, send_gsr_list);
	}

	return retval;
}

/* eof */
