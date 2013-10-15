/* vim:ts=8:sts=8:sw=4:noai:noexpandtab
 *
 * Simple sender using the PGM transport.
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
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <arpa/inet.h>

#include <glib.h>

#include <pgm/pgm.h>
#include <pgm/backtrace.h>
#include <pgm/log.h>


#include "messagequeue.h"
#include "bytestream.h"
using namespace messageqcpp;

#include "consts.h"
#include <iostream>
/* typedefs */

/* globals */

static int g_port = 7500;
static const char* g_network = "";
static bool g_multicast_loop = FALSE;
static int g_udp_encap_port = 0;

// static int g_max_tpdu = 1500;
int g_max_rte = 400*1000;
// static int g_sqns = 100;

static gboolean g_fec = FALSE;
static int g_k = 64;
static int g_n = 255;

static pgm_transport_t* g_transport = NULL;

static bool create_transport (void);

// how many blocks to back into a message
uint g_blocks = 1;
// how many messages to send (total blockops sent = blocks * messages)
uint g_messages = 100; //250000;
uint createData(ByteStream& obs, ByteStream& psbs);
uint createData(ByteStream& obs);

static void usage (const char* bin)
{
	fprintf (stderr, "Usage: %s [options] message\n", bin);
	fprintf (stderr, "  -b <blocks >    : Number of blocks to send\n");
	fprintf (stderr, "  -m <messages>   : Number of messages to send\n");
	fprintf (stderr, "  -t <max rate>   : max rate factor * 400\n");
	fprintf (stderr, "  -n <network>    : Multicast group or unicast IP address\n");
	fprintf (stderr, "  -s <port>       : IP port\n");
	fprintf (stderr, "  -p <port>       : Encapsulate PGM in UDP on IP port\n");
	fprintf (stderr, "  -r <rate>       : Regulate to rate bytes per second\n");
	fprintf (stderr, "  -f <type>       : Enable FEC with either proactive or ondemand parity\n");
	fprintf (stderr, "  -k <k>          : Configure Reed-Solomon code (n, k)\n");
	fprintf (stderr, "  -g <n>\n");
	fprintf (stderr, "  -w sndbuf       : Set sndbuf size\n");
	fprintf (stderr, "  -q sqns         : Set send window size (in sequence numbers)\n");
	fprintf (stderr, "  -l              : Enable multicast loopback and address sharing\n");
	exit (1);
}

int main (int	argc,	char   *argv[]	)
{
	g_sndbuf = 126976;
	g_sqns = 1000;

/* parse program arguments */
	const char* binary_name = strrchr (argv[0], '/');
	int c;
	while ((c = getopt (argc, argv, "b:m:t:s:n:p:r:f:k:g:w:q:lh")) != -1)
	{
		switch (c) {
		case 'b':	g_blocks = atoi(optarg); break;
		case 'm':	g_messages = atoi(optarg); break;
		case 't':	g_max_rte = atoi(optarg) * 400; break;

		case 'n':	g_network = optarg; break;
		case 's':	g_port = atoi (optarg); break;
		case 'p':	g_udp_encap_port = atoi (optarg); break;
		case 'r':	g_max_rte = atoi (optarg); break;

		case 'f':	g_fec = TRUE; break;
		case 'k':	g_k = atoi (optarg); break;
		case 'g':	g_n = atoi (optarg); break;
		case 'w':	g_sndbuf = atoi (optarg); break;
		case 'q':	g_sqns = atoi (optarg); break;

		case 'l':	g_multicast_loop = TRUE; break;

		case 'h':
		case '?': usage (binary_name);
		}
	}

	if (g_fec && ( !g_k || !g_n )) {
		puts ("Invalid Reed-Solomon parameters.");
		usage (binary_name);
	}

	log_init ();
	pgm_init ();

/* setup signal handlers */
	signal (SIGSEGV, on_sigsegv);
	signal (SIGHUP, SIG_IGN);

	ByteStream obs;
	ByteStream psbs;
	uint w = createData(obs, psbs);
	ByteStream::octbyte bytes_written = 0;
	time_t start_time = 0;
	if (!create_transport ())
	{
		start_time = time(0);
// Send total length for error check and to start timer on receive side
		gssize e = pgm_transport_send (g_transport, psbs.buf(), psbs.length(), 0);
		if (e < 0) 
			g_warning ("pgm_transport_send datasize failed.");
		for (uint i = 0; i < g_messages; ++i)
		{
			gsize e = pgm_transport_send (g_transport, obs.buf(), obs.length(), 0);
		        if (e < 0) 
				g_warning ("pgm_transport_send data failed.");

			bytes_written +=w;
		}
	}
	obs.reset();
//Send 0 length to indicate transmission finished.
	gssize e = pgm_transport_send (g_transport, &obs, 0, 0);
 	if (e < 0) 
		g_warning ("pgm_transport_send failed.");
	
	double elapsed_time = time(0) - start_time;

/* cleanup */
	if (g_transport) 
	{
		pgm_transport_destroy (g_transport, TRUE);
		g_transport = NULL;
	}

	std::cout << "pgmsend wrote " << bytes_written << " bytes with " << g_messages << " messages of " << w << " bytes in " << elapsed_time << " seconds ";
	if (elapsed_time)
		std::cout <<  bytes_written/elapsed_time/1024 << " Kbytes/second\n";
	else
		std::cout << std::endl;

	return 0;
}


uint createData(ByteStream& obs)
{
	srand(time(0));
	uint endloop = PrimSize * g_blocks / sizeof(ByteStream::octbyte);
	for (uint u = 0; u < endloop; u++)
	{
		ByteStream::octbyte rc = rand();
		obs << rc;
	}

	ByteStream::octbyte w = obs.length();
	idbassert(w == (PrimSize * g_blocks));
	return w;
}

uint createData(ByteStream& obs, ByteStream& psbs)
{
	srand(time(0));
	uint endloop = PrimSize * g_blocks / sizeof(ByteStream::octbyte);
	for (uint u = 0; u < endloop; u++)
	{
		ByteStream::octbyte rc = rand();
		obs << rc;
	}

	ByteStream::octbyte p = obs.length() * g_messages;
	psbs << p;

	ByteStream::octbyte w = obs.length();
	idbassert(w == (PrimSize * g_blocks));
	return w;
}

static bool create_transport ()
{
	pgm_gsi_t gsi;

	int e = pgm_create_md5_gsi (&gsi);
	g_assert (e == 0);

	struct group_source_req recv_gsr, send_gsr;
	gsize recv_len = 1;
	e = pgm_if_parse_transport (g_network, AF_UNSPEC, &recv_gsr, &recv_len, &send_gsr);
	g_assert (e == 0);
	g_assert (recv_len == 1);

	if (g_udp_encap_port) {
		((struct sockaddr_in*)&send_gsr.gsr_group)->sin_port = g_htons (g_udp_encap_port);
		((struct sockaddr_in*)&recv_gsr.gsr_group)->sin_port = g_htons (g_udp_encap_port);
	}

	e = pgm_transport_create (&g_transport, &gsi, 0, g_port, &recv_gsr, 1, &send_gsr);
	g_assert (e == 0);

	pgm_transport_set_send_only (g_transport, TRUE);
	pgm_transport_set_max_tpdu (g_transport, g_max_tpdu);
	if (g_sqns != 1000)
	    pgm_transport_set_txw_sqns (g_transport, g_sqns);
	else
	    pgm_transport_set_txw_max_rte (g_transport, g_max_rte);
	pgm_transport_set_multicast_loop (g_transport, g_multicast_loop);
	pgm_transport_set_hops (g_transport, 16);
	pgm_transport_set_ambient_spm (g_transport, pgm_secs(30));
	guint spm_heartbeat[] = { pgm_msecs(100), pgm_msecs(100), pgm_msecs(100), pgm_msecs(100), pgm_msecs(1300), pgm_secs(7), pgm_secs(16), pgm_secs(25), pgm_secs(30) };
	pgm_transport_set_heartbeat_spm (g_transport, spm_heartbeat, G_N_ELEMENTS(spm_heartbeat));

	pgm_transport_set_sndbuf(g_transport, g_sndbuf);

	if (g_fec) {
		pgm_transport_set_fec (g_transport, 0, TRUE, TRUE, g_n, g_k);
	}

	e = pgm_transport_bind (g_transport);
	if (e < 0) {
		if      (e == -1)
			g_critical ("pgm_transport_bind failed errno %i: \"%s\"", errno, strerror(errno));
		else if (e == -2)
			g_critical ("pgm_transport_bind failed h_errno %i: \"%s\"", h_errno, hstrerror(h_errno));
		else
			g_critical ("pgm_transport_bind failed e %i", e);
		return TRUE;
	}
	g_assert (e == 0);

	return FALSE;
}

/* eof */
