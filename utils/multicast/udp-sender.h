/* Copyright (C) 2014 InfiniDB, Inc.

   This program is free software; you can redistribute it and/or
   modify it under the terms of the GNU General Public License
   as published by the Free Software Foundation; version 2 of
   the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. */

#ifndef UDP_SENDER_H
#define UDP_SENDER_H

#include "udp-sender.h"
#include "udpcast.h"
#include "participants.h"
#include "statistics.h"
#include "socklib.h"

extern FILE *udpc_log;

struct fifo;

#define openFile udpc_openFile
#define openPipe udpcs_openPipe
#define localReader udpc_localReader
#define doSend udpc_doSend

int openFile(struct disk_config *config);
int openPipe(struct disk_config *config, int in, int *pid);
int localReader(struct fifo *fifo, int in);

#define BCAST_DATA(s, msg) \
	doSend(s, &msg, sizeof(msg), &net_config->dataMcastAddr)


/**
 * "switched network" mode: server already starts sending next slice before
 * first one is acknowledged. Do not use on old coax networks
 */
#define FLAG_SN    0x0001

/**
 * "not switched network" mode: network is known not to be switched
 */
#define FLAG_NOTSN    0x0002

/**
 * Asynchronous mode: do not any confirmation at all from clients.
 * Useful in situations where no return channel is available
 */
#define FLAG_ASYNC 0x0004


/**
 * Point-to-point transmission mode: use unicast in the (frequent)
 * special case where there is only one receiver.
 */
#define FLAG_POINTOPOINT 0x0008


/**
 * Do automatic rate limitation by monitoring socket's send buffer
 * size. Not very useful, as this still doesn't protect against the
 * switch dropping packets because its queue (which might be slightly slower)
 * overruns
 */
#ifndef WINDOWS
#define FLAG_AUTORATE 0x0008
#endif

#ifdef BB_FEATURE_UDPCAST_FEC
/**
 * Forward error correction
 */
#define FLAG_FEC 0x0010
#endif

/**
 * Use broadcast rather than multicast (useful for cards that don't support
 * multicast correctly
 */
#define FLAG_BCAST 0x0020

/**
 * Never use point-to-point, even if only one receiver
 */
#define FLAG_NOPOINTOPOINT 0x0040


/*
 * Don't ask for keyboard input on sender end.
 */
#define FLAG_NOKBD 0x0080

/**
 * Streaming mode: allow receiver to join a running transmission
 */
#define FLAG_STREAMING 0x0100

#endif
