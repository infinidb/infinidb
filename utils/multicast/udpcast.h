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

#ifndef UDPCAST_H
#define UDPCAST_H

#include "socklib.h"
#include <sys/time.h>
#include <stdio.h>
#include <pthread.h>

#define BITS_PER_INT (sizeof(int) * 8)
#define BITS_PER_CHAR 8


#define MAP_ZERO(l, map) (memset(map, 0, ((l) + BITS_PER_INT - 1)/ BIT_PER_INT))
#define BZERO(data) (memset((void *)&data, 0, sizeof(data)))


#define RDATABUFSIZE (2*(MAX_SLICE_SIZE + 1)* MAX_BLOCK_SIZE)

#define DATABUFSIZE (RDATABUFSIZE + 4096 - RDATABUFSIZE % 4096)

int udpc_writeSize(void);
int udpc_largeReadSize(void);
int udpc_smallReadSize(void);
int udpc_makeDataBuffer(int blocksize);
int udpc_parseCommand(char *pipeName, char **arg);

int udpc_printLongNum(unsigned long long x);
int udpc_waitForProcess(int pid, const char *message);

struct disk_config {
    int origOutFile;
    const char *fileName;
    char *pipeName;
    int flags;

    struct timeval stats_last_printed;
};

#define MAX_GOVERNORS 10

struct net_config {
    net_if_t *net_if; /* Network interface (eth0, isdn0, etc.) on which to
		       * multicast */
    int portBase; /* Port base */
    int blockSize;
    int sliceSize;
    struct sockaddr_in controlMcastAddr;
    struct sockaddr_in dataMcastAddr;
    const char *mcastRdv;
    int ttl;
    int nrGovernors;
    struct rateGovernor_t *rateGovernor[MAX_GOVERNORS];
    void *rateGovernorData[MAX_GOVERNORS];
    /*int async;*/
    /*int pointopoint;*/
    struct timeval ref_tv;

    enum discovery {
	DSC_DOUBLING,
	DSC_REDUCING
    } discovery;

    /* int autoRate; do queue watching using TIOCOUTQ, to avoid overruns */

    int flags; /* non-capability command line flags */
    int capabilities;

    int min_slice_size;
    int default_slice_size;
    int max_slice_size;
    unsigned int rcvbuf;

    int rexmit_hello_interval; /* retransmission interval between hello's.
				* If 0, hello message won't be retransmitted
				*/
    int autostart; /* autostart after that many retransmits */

    int requestedBufSize; /* requested receiver buffer */

    /* sender-specific parameters */
    int min_receivers;
    int max_receivers_wait;
    int min_receivers_wait;

    int retriesUntilDrop;

    /* receiver-specif parameters */
    int exitWait; /* How many milliseconds to wait on program exit */

    int startTimeout; /* Timeout at start */

    /* FEC config */
#ifdef BB_FEATURE_UDPCAST_FEC
    int fec_redundancy; /* how much fec blocks are added per group */
    int fec_stripesize; /* size of FEC group */
    int fec_stripes; /* number of FEC stripes per slice */
#endif

    int rehelloOffset; /* how far before end will rehello packet will
			  be retransmitted */
};

struct stat_config {
    FILE *log; /* Log file for statistics */
    long bwPeriod; /* How often are bandwidth estimations logged? */

    int statPeriod;
    int printUncompressedPos;
};

#define NR_CLIENT_SOCKS 4

struct client_config {
    int socks[NR_CLIENT_SOCKS];
    struct sockaddr_in serverAddr;
    int clientNumber;
    int isStarted;
    pthread_t thread;
    int sender_is_newgen;
};

void *rgInitGovernor(struct net_config *cfg, struct rateGovernor_t *gov);
void rgParseRateGovernor(struct net_config *net_config, char *rg);
void rgWaitAll(struct net_config *cfg, int sock, in_addr_t ip, int size);
void rgShutdownAll(struct net_config *cfg);

/**
 * Answers whether given fd is seekable
 */
int udpc_shouldPrintUncompressedPos(int deflt, int fd, int pipe);

#define MAX_SLICE_SIZE 1024

#define DEFLT_STAT_PERIOD 500000

#ifndef DEBUG
# define DEBUG 0
#endif

#endif
