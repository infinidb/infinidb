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


/* typedefs */

/* globals */

#ifndef SC_IOV_MAX
#ifdef _SC_IOV_MAX
#	define SC_IOV_MAX	_SC_IOV_MAX
#else
#	SC_IOV_MAX and _SC_IOV_MAX undefined too, please fix.
#endif
#endif

#include <iostream>

// #include <boost/thread.hpp>
// using namespace boost;

#include "messagequeue.h"
#include "bytestream.h"
using namespace messageqcpp;

#include "consts.h"
#include "threadpool.h"
using namespace threadpool;

#include "timeset.h"
using namespace broadcast;
using namespace std;

static int g_port = 7500;
static const char* g_network = "";
static const char* g_source = "";
static gboolean g_multicast_loop = FALSE;
static int g_udp_encap_port = 0;

// static int g_max_tpdu = 1500;
// static int g_sqns = 100;

static pgm_transport_t* g_transport = NULL;
static bool g_quit = FALSE;

static void on_signal (int);
static bool startup ();

static int on_msgv (pgm_msgv_t*, int);
void readStats(uint, double, double);
const string recvMsg("recv");
const string procMsg("proc");
const int lenSize = sizeof(ByteStream::octbyte);
uint64_t sentBytes = 0;

TimeSet ts;


struct ReceiverThread
{
	pgm_transport_t* transport;

	void operator()()
	{
		long iov_max = sysconf( SC_IOV_MAX );
		pgm_msgv_t msgv[iov_max];
	
		int n_fds = 2;
		struct pollfd fds[ n_fds ];	
		uint total_len = 0;	

		do {
			int len = pgm_transport_recvmsgv (transport, msgv, iov_max, MSG_WAITALL /*blocking */);
			if (len >= 0)
			{
				ts.startTimer(procMsg);
				on_msgv (msgv, len);
				ts.holdTimer(procMsg);
				if (!len) 
				{
					readStats(total_len, ts.totalTime(recvMsg), ts.totalTime(procMsg));
					total_len = 0;
					ts.clear();
				}
				else
					if (lenSize != len)	total_len += len;
			}
			else if (errno == EAGAIN)	/* len == -1, an error occured */
			{
				memset (fds, 0, sizeof(fds));
				pgm_transport_poll_info (g_transport, fds, &n_fds, POLLIN);
				poll (fds, n_fds, 1000 /* ms */);
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
				g_quit = true;
				break;
			}
			else
			{
				g_error ("pgm socket failed errno %i: \"%s\"", errno, strerror(errno));
				g_quit = true;
				break;
			}

		} while (true);
	}
};

void readStats(uint len, double recvtime, double msgtime)
{
	double time = recvtime + msgtime;
	if (time)
	{
		std::cout << "Read " << len  << " bytes in " << (time) << " seconds (" <<	(len / (time) / 1024) << " KB/s) or " <<  (len / (time) / PrimSize / 1024) << " Kblockops/s " ;
	}
	else 
		std::cout << "Read " << len << " bytes in " << (time) << " seconds ";
	std::cout << " Transport recvtime: " << recvtime << " process msgtime " << msgtime << std::endl;
}

static void usage (const char*	bin)
{
	fprintf (stderr, "Usage: %s [options]\n", bin);
	fprintf (stderr, "  -n <network>    : Multicast group or unicast IP address\n");
	fprintf (stderr, "  -a <ip address> : Source unicast IP address\n");
	fprintf (stderr, "  -s <port>       : IP port\n");
	fprintf (stderr, "  -p <port>       : Encapsulate PGM in UDP on IP port\n");
	fprintf (stderr, "  -l              : Enable multicast loopback and address sharing\n");
	fprintf (stderr, "  -r rcvbuf       : Set rcvbuf size\n");
	fprintf (stderr, "  -q sqns         : Set send window size (in sequence numbers)\n");

	exit (1);
}

int main (int	argc, char*	argv[]	)
{
	pgm_transport_list = NULL;

	g_message ("pgmrecv");

	g_rcvbuf = 126976;
	g_sqns = 1000;

/* parse program arguments */
	const char* binary_name = strrchr (argv[0], '/');
	int c;
	while ((c = getopt (argc, argv, "a:s:n:p:r:q:lh")) != -1)
	{
		switch (c) {
		case 'n':	g_network = optarg; break;
		case 'a':	g_source = optarg; break;
		case 's':	g_port = atoi (optarg); break;
		case 'p':	g_udp_encap_port = atoi (optarg); break;

		case 'l':	g_multicast_loop = TRUE; break;
		case 'r':	g_rcvbuf = atoi (optarg); break;
		case 'q':	g_sqns = atoi (optarg); break;
		case 'h':
		case '?': usage (binary_name);
		}
	}

	log_init();
	pgm_init();


/* setup signal handlers */
	signal(SIGSEGV, on_sigsegv);
	signal(SIGINT, on_signal);
	signal(SIGTERM, on_signal);

/* delayed startup */
	g_message ("scheduling startup.");

/* dispatch loop */
	g_message ("entering main event loop ... ");
	startup();

	ReceiverThread rt;
    	ThreadPool tp(1, 10);    
    	while (!g_quit)
    	{
        	rt.transport = g_transport;
        	tp.invoke(rt);
    	}

	g_message ("event loop terminated, cleaning up.");

/* cleanup */

	if (g_transport) {
		g_message ("destroying transport.");

		pgm_transport_destroy (g_transport, TRUE);
		g_transport = NULL;
	}

	g_message ("finished.");
	return 0;
}

static void on_signal (G_GNUC_UNUSED int signum	)
{
	g_message ("on_signal");
	if (g_transport) {
		g_message ("destroying transport.");

		pgm_transport_destroy (g_transport, TRUE);
		g_transport = NULL;
	}
	g_message ("finished.");
	g_quit = true;
	exit(0);	
}

static bool startup ()
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

	// RJD
	pgm_transport_set_rcvbuf (g_transport, g_rcvbuf);

	e = pgm_transport_bind (g_transport);
	if (e < 0) {
		if      (e == -1)
			g_critical ("pgm_transport_bind failed errno %i: \"%s\"", errno, strerror(errno));
		else if (e == -2)
			g_critical ("pgm_transport_bind failed h_errno %i: \"%s\"", h_errno, hstrerror(h_errno));
		else
			g_critical ("pgm_transport_bind failed e %i", e);
		return FALSE;
	}
	g_assert (e == 0);

	g_message ("startup complete.");
	return FALSE;
}

ByteStream::octbyte process_block(const ByteStream& bs)
{
	const ByteStream::byte* b = bs.buf();
	const size_t l = bs.length() / sizeof(ByteStream::octbyte);
	const ByteStream::octbyte* o = reinterpret_cast<const ByteStream::octbyte*>(b);
	ByteStream::octbyte total = 0;
	for (size_t i = 0; i < l; i++, o++)
	{
		if (*o == 0xdeadbeefbadc0ffeLL)
			total++;
		if (*o == 0x1001LL)
			total++;
		if (*o == 0x2002LL)
			total++;
	}
	return total;
}

static int on_msgv (pgm_msgv_t*	msgv,	/* an array of msgvs */	int len)
{
//         g_message ("(%i bytes)", len);
	static ByteStream indata;
	bool startStream = false;
	if (lenSize == len)
	{
		ts.startTimer(recvMsg);
		startStream = true;
		indata.reset();
	}
	else if (0 == len)
	{
		ts.stopTimer(recvMsg);
		if (indata.length() != sentBytes)
			cout << "ERROR: sentBytes " << sentBytes << ", receivedBytes " << indata.length() << ", difference " << sentBytes - indata.length() << endl;
		return 0;
	}

        while (len)
        {
                struct iovec* msgv_iov = msgv->msgv_iov;

		guint apdu_len = 0;
		struct iovec* p = msgv_iov;

		if (startStream)
		{
			sentBytes = *(ByteStream::octbyte*)p->iov_base;
			apdu_len += p->iov_len;
		}
		else	
		for (guint j = 0; j < msgv->msgv_iovlen; j++) 
		{	/* # elements */
			apdu_len += p->iov_len;
			indata.append((ByteStream::byte*)p->iov_base, p->iov_len);
			p++;
		}
		
		len -= apdu_len;
                msgv++;
        }
// 	std::cout << "Indata length " << indata.length() << std::endl;

	return 0;
}

/* eof */
