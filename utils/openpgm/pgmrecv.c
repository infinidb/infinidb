/* vim:ts=8:sts=8:sw=4:noai:noexpandtab
 *
 * Simple receiver using the PGM transport, based on enonblocksyncrecvmsgv :/
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
#include <netdb.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <netinet/in.h>
#ifdef CONFIG_HAVE_EPOLL
#	include <sys/epoll.h>
#endif
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <arpa/inet.h>

#include <glib.h>

#include "pgm/pgm.h"
#include "pgm/backtrace.h"
#include "pgm/log.h"
#ifdef CONFIG_WITH_HTTP
#	include "pgm/http.h"
#endif
#ifdef CONFIG_WITH_SNMP
#	include "pgm/snmp.h"
#endif

/* typedefs */

/* globals */

#ifndef SC_IOV_MAX
#ifdef _SC_IOV_MAX
#	define SC_IOV_MAX	_SC_IOV_MAX
#else
#	SC_IOV_MAX and _SC_IOV_MAX undefined too, please fix.
#endif
#endif


static int g_port = 7500;
static const char* g_network = "";
static const char* g_source = "";
static gboolean g_multicast_loop = FALSE;
static int g_udp_encap_port = 0;

static int g_max_tpdu = 1500;
static int g_sqns = 100;

static pgm_transport_t* g_transport = NULL;
static GThread* g_thread = NULL;
static GMainLoop* g_loop = NULL;
static gboolean g_quit = FALSE;

static void on_signal (int);
static gboolean on_startup (gpointer);
static gboolean on_mark (gpointer);

static gpointer receiver_thread (gpointer);
static int on_msgv (pgm_msgv_t*, guint, gpointer);

G_GNUC_NORETURN static void
usage (
	const char*	bin
	)
{
	fprintf (stderr, "Usage: %s [options]\n", bin);
	fprintf (stderr, "  -n <network>    : Multicast group or unicast IP address\n");
	fprintf (stderr, "  -a <ip address> : Source unicast IP address\n");
	fprintf (stderr, "  -s <port>       : IP port\n");
	fprintf (stderr, "  -p <port>       : Encapsulate PGM in UDP on IP port\n");
	fprintf (stderr, "  -l              : Enable multicast loopback and address sharing\n");
#ifdef CONFIG_WITH_HTTP
	fprintf (stderr, "  -t              : Enable HTTP administrative interface\n");
#endif
#ifdef CONFIG_WITH_SNMP
	fprintf (stderr, "  -x              : Enable SNMP interface\n");
#endif
	exit (1);
}

int
main (
	int		argc,
	char*		argv[]
	)
{
	pgm_transport_list = NULL;

	g_message ("pgmrecv");
#ifdef CONFIG_WITH_HTTP
	gboolean enable_http = FALSE;
#endif
#ifdef CONFIG_WITH_SNMP
	gboolean enable_snmpx = FALSE;
#endif

/* parse program arguments */
	const char* binary_name = strrchr (argv[0], '/');
	int c;
	while ((c = getopt (argc, argv, "a:s:n:p:lh"
#ifdef CONFIG_WITH_HTTP
					"t"
#endif
#ifdef CONFIG_WITH_SNMP
					"x"
#endif
					)) != -1)
	{
		switch (c) {
		case 'n':	g_network = optarg; break;
		case 'a':	g_source = optarg; break;
		case 's':	g_port = atoi (optarg); break;
		case 'p':	g_udp_encap_port = atoi (optarg); break;

		case 'l':	g_multicast_loop = TRUE; break;
#ifdef CONFIG_WITH_HTTP
		case 't':	enable_http = TRUE; break;
#endif
#ifdef CONFIG_WITH_SNMP
		case 'x':	enable_snmpx = TRUE; break;
#endif

		case 'h':
		case '?': usage (binary_name);
		}
	}

	log_init();
	pgm_init();

#ifdef CONFIG_WITH_HTTP
	if (enable_http)
		pgm_http_init(PGM_HTTP_DEFAULT_SERVER_PORT);
#endif
#ifdef CONFIG_WITH_SNMP
	if (enable_snmpx)
		pgm_snmp_init();
#endif

	g_loop = g_main_loop_new (NULL, FALSE);

/* setup signal handlers */
	signal(SIGSEGV, on_sigsegv);
	pgm_signal_install(SIGINT, on_signal);
	pgm_signal_install(SIGTERM, on_signal);
	pgm_signal_install(SIGHUP, SIG_IGN);

/* delayed startup */
	g_message ("scheduling startup.");
	g_timeout_add (0, (GSourceFunc)on_startup, NULL);

/* dispatch loop */
	g_message ("entering main event loop ... ");
	g_main_loop_run (g_loop);

	g_message ("event loop terminated, cleaning up.");

/* cleanup */
	g_quit = TRUE;
	g_thread_join (g_thread);

	g_main_loop_unref(g_loop);
	g_loop = NULL;

	if (g_transport) {
		g_message ("destroying transport.");

		pgm_transport_destroy (g_transport, TRUE);
		g_transport = NULL;
	}

#ifdef CONFIG_WITH_HTTP
	if (enable_http)
		pgm_http_shutdown();
#endif
#ifdef CONFIG_WITH_SNMP
	if (enable_snmpx)
		pgm_snmp_shutdown();
#endif

	g_message ("finished.");
	return 0;
}

static void
on_signal (
	G_GNUC_UNUSED int signum
	)
{
	g_message ("on_signal");

	g_main_loop_quit(g_loop);
}

static gboolean
on_startup (
	G_GNUC_UNUSED gpointer data
	)
{
	g_message ("startup.");
	g_message ("create transport.");

	pgm_gsi_t gsi;
	int e = pgm_create_md5_gsi (&gsi);
	g_assert (e == 0);

	struct group_source_req recv_gsr, send_gsr;
	gsize recv_len = 1;
	e = pgm_if_parse_transport (g_network, AF_UNSPEC, &recv_gsr, &recv_len, &send_gsr);
	g_assert (e == 0);
	g_assert (recv_len == 1);

	if (g_source[0]) {
		((struct sockaddr_in*)&recv_gsr.gsr_source)->sin_addr.s_addr = inet_addr(g_source);
	}

	if (g_udp_encap_port) {
		((struct sockaddr_in*)&send_gsr.gsr_group)->sin_port = g_htons (g_udp_encap_port);
		((struct sockaddr_in*)&recv_gsr.gsr_group)->sin_port = g_htons (g_udp_encap_port);
	}

	pgm_transport_list = NULL;
	e = pgm_transport_create (&g_transport, &gsi, 0, g_port, &recv_gsr, 1, &send_gsr);
	g_assert (e == 0);

	pgm_transport_set_recv_only (g_transport, FALSE);
	pgm_transport_set_max_tpdu (g_transport, g_max_tpdu);
	pgm_transport_set_rxw_sqns (g_transport, g_sqns);
	pgm_transport_set_multicast_loop (g_transport, g_multicast_loop);
	pgm_transport_set_hops (g_transport, 16);
	pgm_transport_set_peer_expiry (g_transport, pgm_secs(300));
	pgm_transport_set_spmr_expiry (g_transport, pgm_msecs(250));
	pgm_transport_set_nak_bo_ivl (g_transport, pgm_msecs(50));
	pgm_transport_set_nak_rpt_ivl (g_transport, pgm_secs(2));
	pgm_transport_set_nak_rdata_ivl (g_transport, pgm_secs(2));
	pgm_transport_set_nak_data_retries (g_transport, 50);
	pgm_transport_set_nak_ncf_retries (g_transport, 50);

	e = pgm_transport_bind (g_transport);
	if (e < 0) {
		if      (e == -1)
			g_critical ("pgm_transport_bind failed errno %i: \"%s\"", errno, strerror(errno));
		else if (e == -2)
			g_critical ("pgm_transport_bind failed h_errno %i: \"%s\"", h_errno, hstrerror(h_errno));
		else
			g_critical ("pgm_transport_bind failed e %i", e);
		g_main_loop_quit(g_loop);
		return FALSE;
	}
	g_assert (e == 0);

/* create receiver thread */
	GError* err;
	g_thread = g_thread_create_full (receiver_thread,
					g_transport,
					0,
					TRUE,
					TRUE,
					G_THREAD_PRIORITY_HIGH,
					&err);
	if (!g_thread) {
		g_critical ("g_thread_create_full failed errno %i: \"%s\"", err->code, err->message);
		g_main_loop_quit(g_loop);
		return FALSE;
	}

/* period timer to indicate some form of life */
// TODO: Gnome 2.14: replace with g_timeout_add_seconds()
	g_timeout_add(10 * 1000, (GSourceFunc)on_mark, NULL);

	g_message ("startup complete.");
	return FALSE;
}

/* idle log notification
 */

static gboolean
on_mark (
	G_GNUC_UNUSED gpointer data
	)
{
	g_message ("-- MARK --");
	return TRUE;
}

static gpointer
receiver_thread (
	gpointer	data
	)
{
	pgm_transport_t* transport = (pgm_transport_t*)data;
	long iov_max = sysconf( SC_IOV_MAX );
	pgm_msgv_t msgv[iov_max];

#ifdef CONFIG_HAVE_EPOLL
	int efd = epoll_create (IP_MAX_MEMBERSHIPS);
	if (efd < 0) {
		g_error ("epoll_create failed errno %i: \"%s\"", errno, strerror(errno));
		g_main_loop_quit(g_loop);
		return NULL;
	}

	int retval = pgm_transport_epoll_ctl (g_transport, efd, EPOLL_CTL_ADD, EPOLLIN);
	if (retval < 0) {
		g_error ("pgm_epoll_ctl failed errno %i: \"%s\"", errno, strerror(errno));
		g_main_loop_quit(g_loop);
		return NULL;
	}
#else
	int n_fds = 2;
	struct pollfd fds[ n_fds ];
	
#endif /* !CONFIG_HAVE_EPOLL */

	do {
		gssize len = pgm_transport_recvmsgv (transport, msgv, iov_max, MSG_DONTWAIT /* non-blocking */);
		if (len >= 0)
		{
			on_msgv (msgv, len, NULL);
		}
		else if (errno == EAGAIN)	/* len == -1, an error occured */
		{
#ifdef CONFIG_HAVE_EPOLL
			struct epoll_event events[1];	/* wait for maximum 1 event */
			epoll_wait (efd, events, G_N_ELEMENTS(events), 1000 /* ms */);
#else
			memset (fds, 0, sizeof(fds));
			pgm_transport_poll_info (g_transport, fds, &n_fds, POLLIN);
			poll (fds, n_fds, 1000 /* ms */);
#endif /* !CONFIG_HAVE_EPOLL */
		}
		else if (errno == ECONNRESET)
		{
			pgm_sock_err_t* pgm_sock_err = (pgm_sock_err_t*)msgv[0].msgv_iov->iov_base;
			g_warning ("pgm socket lost %" G_GUINT32_FORMAT " packets detected from %s",
					pgm_sock_err->lost_count,
					pgm_print_tsi(&pgm_sock_err->tsi));
			continue;
		} 
		else if (errno == ENOTCONN)		/* socket(s) closed */
		{
			g_error ("pgm socket closed.");
			g_main_loop_quit(g_loop);
			break;
		}
		else
		{
			g_error ("pgm socket failed errno %i: \"%s\"", errno, strerror(errno));
			g_main_loop_quit(g_loop);
			break;
		}
	} while (!g_quit);

#ifdef CONFIG_HAVE_EPOLL
	close (efd);
#endif
	return NULL;
}

static int
on_msgv (
	pgm_msgv_t*	msgv,		/* an array of msgvs */
	guint		len,
	G_GNUC_UNUSED gpointer	user_data
	)
{
        g_message ("(%i bytes)",
                        len);

        guint i = 0;
        while (len)
        {
                struct iovec* msgv_iov = msgv->msgv_iov;

		guint apdu_len = 0;
		struct iovec* p = msgv_iov;
		for (guint j = 0; j < msgv->msgv_iovlen; j++) {	/* # elements */
			apdu_len += p->iov_len;
			p++;
		}

/* truncate to first fragment to make GLib printing happy */
		char buf[1024], tsi[PGM_TSISTRLEN];
		snprintf (buf, sizeof(buf), "%s", (char*)msgv_iov->iov_base);
		pgm_print_tsi_r (msgv->msgv_tsi, tsi, sizeof(tsi));
		if (msgv->msgv_iovlen > 1) {
			g_message ("\t%u: \"%s\" ... (%u bytes from %s)", ++i, buf, apdu_len, tsi);
		} else {
			g_message ("\t%u: \"%s\" (%u bytes from %s)", ++i, buf, apdu_len, tsi);
		}

		len -= apdu_len;
                msgv++;
        }

	return 0;
}

/* eof */
