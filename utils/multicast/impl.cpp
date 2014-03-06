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

// $Id: impl.cpp 3495 2013-01-21 14:09:51Z rdempsey $

/* This code is based on udpcast-20090830. Most of the source code in that release
   contains no copyright or licensing notices at all. The exception is fec.c, which
   is not used here. The udpcast website, http://udpcast.linux.lu/, implies that
   the source is covered under GPL. */

#include <net/if.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/ioctl.h>
#include <cerrno>
#include <pthread.h>
using namespace std;

#include "bytestream.h"
using namespace messageqcpp;

#include "udp-sender.h"
#include "udpc-protoc.h"
#include "util.h"
#include "mc_fifo.h"

#include "impl.h"

struct  participantsDb {
    int nrParticipants;
    
    struct clientDesc {
	struct sockaddr_in addr;
	int used;
	int capabilities;
	unsigned int rcvbuf;
    } clientTable[MAX_CLIENTS];
};
struct produconsum {
    unsigned int size;
    volatile unsigned int produced;
    unsigned int consumed;
    volatile int atEnd;
    pthread_mutex_t mutex;
    volatile int consumerIsWaiting;
    pthread_cond_t cond;
    const char *name;
};

struct stats {
    int fd;
    struct timeval lastPrinted;
    long statPeriod;
    int printUncompressedPos;
};

struct sender_stats {
    FILE *log;    
    unsigned long long totalBytes;
    unsigned long long retransmissions;
    int clNo;
    unsigned long periodBytes;
    struct timeval periodStart;
    long bwPeriod;
    struct stats s;
};

struct receiver_stats {
    struct timeval tv_start;
    int bytesOrig;
    long long totalBytes;
    int timerStarted;
    struct stats s;
};

#define SLICEMAGIC 0x41424344

typedef struct slice {
    int base; /* base address of slice in buffer */
    int sliceNo;
    int bytes; /* bytes in slice */
    int nextBlock; /* index of next buffer to be transmitted */
    enum slice_state { 
	SLICE_FREE, /* free slice, and in the queue of free slices */
	SLICE_NEW, /* newly allocated. FEC calculation and first 
		    * transmission */
	SLICE_XMITTED, /* transmitted */
	SLICE_ACKED, /* acknowledged (if applicable) */
	SLICE_PRE_FREE, /* no longer used, but not returned to queue */
	SLICE_RECEIVING,
	SLICE_DONE,
    };
    volatile enum slice_state state;
    char rxmitMap[MAX_SLICE_SIZE / BITS_PER_CHAR]; 
    /* blocks to be retransmitted */

    char isXmittedMap[MAX_SLICE_SIZE / BITS_PER_CHAR]; 
   /* blocks which have already been retransmitted during this round*/

    int rxmitId; /* used to distinguish among several retransmission 
		  * requests, so that we can easily discard answers to "old"
		  * requests */

    /* This structure is used to keep track of clients who answered, and
     * to make the reqack message
     */
    struct reqackBm {
	struct reqack ra;
	char readySet[MAX_CLIENTS / BITS_PER_CHAR]; /* who is already ok? */
    } sl_reqack;

    char answeredSet[MAX_CLIENTS / BITS_PER_CHAR]; /* who answered at all? */

    int nrReady; /* number of participants who are ready */
    int nrAnswered; /* number of participants who answered; */
    int needRxmit; /* does this need retransmission? */
    int lastGoodBlock; /* last good block of slice (i.e. last block having not
			* needed retransmission */

    int lastReqack; /* last req ack sent (debug) */
#ifdef BB_FEATURE_UDPCAST_FEC
    unsigned char *fec_data;
#endif
    int magic;
    int blocksTransferred; /* blocks transferred during this slice */
    int dataBlocksTransferred; /* data blocks transferred during this slice */
    struct retransmit retransmit;
    int freePos; /* where the next data part will be stored to */
    int bytesKnown; /* is number of bytes known yet? */
    short missing_data_blocks[MAX_FEC_INTERLEAVE]; 
} *slice_t;

#define QUEUE_SIZE 256

struct returnChannel {
    pthread_t thread; /* message receiving thread */
    int rcvSock; /* socket on which we receive the messages */
    produconsum_t incoming; /* where to enqueue incoming messages */
    produconsum_t freeSpace; /* free space */
    struct {
	int clNo; /* client number */
	union message msg; /* its message */
    } q[QUEUE_SIZE];
    struct net_config *config;
    participantsDb_t participantsDb;
};
#define NR_SLICES 2

typedef struct senderState {
    struct returnChannel rc;
    struct fifo *fifo;

    struct net_config *config;
    sender_stats_t stats;
    int socket;
    
    struct slice slices[NR_SLICES];

    produconsum_t free_slices_pc;

    unsigned char *fec_data;
    pthread_t fec_thread;
    produconsum_t fec_data_pc;
} *sender_state_t;

struct clientState {
    struct fifo *fifo;
    struct client_config *client_config;
    struct net_config *net_config;
    union serverDataMsg Msg;

    struct msghdr data_hdr;

    /* pre-prepared messages */
    struct iovec data_iov[2];

    struct slice *currentSlice;
    int currentSliceNo;
    receiver_stats_t stats;
    
    produconsum_t free_slices_pc;
    struct slice slices[NR_SLICES];

    /* Completely received slices */
    int receivedPtr;
    int receivedSliceNo;

#ifdef BB_FEATURE_UDPCAST_FEC
    int use_fec; /* do we use forward error correction ? */
#endif
    produconsum_t fec_data_pc;
    struct slice *fec_slices[NR_SLICES];
    pthread_t fec_thread;

    /* A reservoir of free blocks for FEC */
    produconsum_t freeBlocks_pc;
    unsigned char **blockAddresses; /* adresses of blocks in local queue */

    unsigned char **localBlockAddresses;
				/* local blocks: freed FEC blocks after we
				 * have received the corresponding data */
    int localPos;

    unsigned char *blockData;
    unsigned char *nextBlock;

    int endReached; /* end of transmission reached:
		       0: transmission in progress
		       2: network transmission _and_ FEC 
		          processing finished 
		    */

    int netEndReached; /* In case of a FEC transmission; network
			* transmission finished. This is needed to avoid
			* a race condition, where the receiver thread would
			* already prepare to wait for more data, at the same
			* time that the FEC would set endReached. To avoid
			* this, we do a select without timeout before
			* receiving the last few packets, so that if the
			* race condition strikes, we have a way to protect
			* against
			*/

    int selectedFd;

    int promptPrinted;  /* Has "Press any key..." prompt already been printed */

#ifdef BB_FEATURE_UDPCAST_FEC
    fec_code_t fec_code;
#endif
};

#define S_UCAST socks[0]
#define S_BCAST socks[1]
#define S_MCAST_CTRL socks[2]
#define S_MCAST_DATA socks[3]

#define SSEND(x) SEND(client_config->S_UCAST, x, client_config->serverAddr)

/**
 * Receiver will passively listen to sender. Works best if sender runs
 * in async mode
 */
#define FLAG_PASSIVE 0x0010


/**
 * Do not write file synchronously
 */
#define FLAG_NOSYNC 0x0040

/*
 * Don't ask for keyboard input on receiver end.
 */
#define FLAG_NOKBD 0x0080

/**
 * Do write file synchronously
 */
#define FLAG_SYNC 0x0100

namespace
{
int udpc_isFullDuplex(int s, const char *ifname) {

#ifdef ETHTOOL_GLINK
  struct ifreq ifr;
  struct ethtool_cmd ecmd;

  ecmd.cmd = ETHTOOL_GSET;

  strncpy(ifr.ifr_name, ifname, sizeof(ifr.ifr_name)-1);
  ifr.ifr_data = (char *) &ecmd;

  if(ioctl(s, SIOCETHTOOL, &ifr) == -1) {
    /* Operation not supported */
    return -1;
  } else {
    return ecmd.duplex;
  }
#else
  return -1;
#endif
}

#define getSinAddr(addr) (((struct sockaddr_in *) addr)->sin_addr)

int udpc_ipIsZero(struct sockaddr_in *ip) {
    return getSinAddr(ip).s_addr == 0;
}

int hasLink(int s, const char *ifname) {

#ifdef ETHTOOL_GLINK
  struct ifreq ifr;
  struct ethtool_value edata;

  edata.cmd = ETHTOOL_GLINK;

  strncpy(ifr.ifr_name, ifname, sizeof(ifr.ifr_name)-1);
  ifr.ifr_data = (char *) &edata;

  if(ioctl(s, SIOCETHTOOL, &ifr) == -1) {
    /* Operation not supported */
    return -1;
  } else {
    return edata.data;
  }
#else
  return -1;
#endif
}

#define INET_ATON(a,i) inet_aton(a,i)

int udpc_doSend(int s, void *message, size_t len, struct sockaddr_in *to) {
/*    flprintf("sent: %08x %d\n", *(int*) message, len);*/
#ifdef LOSSTEST
    loseSendPacket();
#endif
    return sendto(s, message, len, 0, (struct sockaddr*) to, sizeof(*to));
}

void udpc_copyToMessage(unsigned char *dst, struct sockaddr_in *src) {    
    memcpy(dst, (char *) &((struct sockaddr_in *)src)->sin_addr,
	   sizeof(struct in_addr));
}

void udpc_sendHello(struct net_config *net_config, int sock,
	       int streaming) {
//cerr << "sending hello..." << endl;
    struct hello hello;
    /* send hello message */
    if(streaming)
	hello.opCode = htons(CMD_HELLO_STREAMING);
    else
	hello.opCode = htons(CMD_HELLO);
    hello.reserved = 0;
    hello.capabilities = htonl(net_config->capabilities);
    udpc_copyToMessage(hello.mcastAddr,&net_config->dataMcastAddr);
    hello.blockSize = htons(net_config->blockSize);
	//TODO: FIXME
    //rgWaitAll(net_config, sock, net_config->controlMcastAddr.sin_addr.s_addr, sizeof(hello));
    BCAST_CONTROL(sock, hello);
}

char *udpc_getIpString(struct sockaddr_in *addr, char *buffer) {
    long iaddr = htonl(getSinAddr(addr).s_addr);
    sprintf(buffer,"%ld.%ld.%ld.%ld", 
	    (iaddr >> 24) & 0xff,
	    (iaddr >> 16) & 0xff,
	    (iaddr >>  8) & 0xff,
	    iaddr & 0xff);
    return buffer;
}

net_if_t *udpc_getNetIf(const char *wanted) {
#ifndef __MINGW32__
	struct ifreq ibuf[100];
	struct ifreq *ifrp, *ifend, *chosen;
	struct ifconf ifc;
	int s;
#else /* __MINGW32__ */
	int i;

	int etherNo=-1;
	int wantedEtherNo=-2; /* Wanted ethernet interface */

	MIB_IPADDRTABLE *iptab=NULL;
	MIB_IFTABLE *iftab=NULL;

	MIB_IPADDRROW *iprow, *chosen=NULL;
	MIB_IFROW *chosenIf=NULL;
	WORD wVersionRequested; /* Version of Winsock to load */
	WSADATA wsaData; /* Winsock implementation details */
	ULONG a;

	int r;
#endif /* __MINGW32__ */

	int lastGoodness=0;
	struct in_addr wantedAddress;
	int isAddress=0;
	int wantedLen=0;
	net_if_t *net_if;

	if(wanted == NULL) {
	    wanted = getenv("IFNAME");
	}

	if(wanted && INET_ATON(wanted, &wantedAddress))
	    isAddress=1;
	else
	    wantedAddress.s_addr=0;

	if(wanted)
	    wantedLen=strlen(wanted);

	net_if = MALLOC(net_if_t);
			//TODO: FIXME
	//if(net_if == NULL)
	//    udpc_fatal(1, "Out of memory error");

#ifndef __MINGW32__

	s = socket(PF_INET, SOCK_DGRAM, 0);
	if (s < 0) {
	    perror("make socket");
	    exit(1);
	}	

	ifc.ifc_len = sizeof(ibuf);
	ifc.ifc_buf = (caddr_t) ibuf;

	if (ioctl(s, SIOCGIFCONF, (char *)&ifc) < 0 ||
            ifc.ifc_len < (signed int)sizeof(struct ifreq)) {
                perror("udpcast: SIOCGIFCONF: ");
                exit(1);
        }

	ifend = (struct ifreq *)((char *)ibuf + ifc.ifc_len);
	chosen=NULL;

	for (ifrp = ibuf ; ifrp < ifend;
#ifdef IFREQ_SIZE
	     ifrp = IFREQ_SIZE(*ifrp) + (char *)ifrp
#else
	     ifrp++
#endif       
	     ) {
	    unsigned long iaddr = getSinAddr(&ifrp->ifr_addr).s_addr;
	    int goodness;

	    if(ifrp->ifr_addr.sa_family != PF_INET)
		continue;
	    
	    if(wanted) {
		if(isAddress && iaddr == wantedAddress.s_addr) {
		    goodness=8;
		} else if(strcmp(wanted, ifrp->ifr_name) ==0) {
		    /* perfect match on interface name */
		    goodness=12;
		} else if(wanted != NULL && 
			  strncmp(wanted, ifrp->ifr_name, wantedLen) ==0) {
		    /* prefix match on interface name */
		    goodness=7;
		} else {
		    /* no match, try next */
		    continue;
		}
	    } else {
		if(iaddr == 0) {
		    /* disregard interfaces whose address is zero */
		    goodness = 1;
		} else if(iaddr == htonl(0x7f000001)) {
		    /* disregard localhost type devices */
		    goodness = 2;
		} else if(strcmp("eth0", ifrp->ifr_name) == 0 ||
			  strcmp("en0",  ifrp->ifr_name) == 0) {
		    /* prefer first ethernet interface */
		    goodness = 6;
		} else if(strncmp("eth0:", ifrp->ifr_name, 5) == 0) {
		    /* second choice: any secondary addresses of first ethernet */
		    goodness = 5;
		} else if(strncmp("eth", ifrp->ifr_name, 3) == 0 ||
			  strncmp("en",  ifrp->ifr_name, 2) == 0) {
		    /* and, if not available, any other ethernet device */
		    goodness = 4;
		} else {
		    goodness = 3;
		}
	    }

	    if(hasLink(s, ifrp->ifr_name))
	      /* Good or unknown link status privileged over known 
	       * disconnected */
	      goodness += 3;

	    /* If all else is the same, prefer interfaces that 
	     * have broadcast */
	    goodness = goodness * 2;
	    if(goodness >= lastGoodness) {
		/* Privilege broadcast-enabled interfaces */
		if(ioctl(s,  SIOCGIFBRDADDR, ifrp) < 0)
		{
			//TODO: FIXME
		    //udpc_fatal(-1, "Error getting broadcast address for %s: %s", ifrp->ifr_name, strerror(errno));
		}
		if(getSinAddr(&ifrp->ifr_ifru.ifru_broadaddr).s_addr)
		    goodness++;
	    }

	    if(goodness > lastGoodness) {
		chosen = ifrp;
		lastGoodness = goodness;
		net_if->addr.s_addr = iaddr;
	    }
	}


	if(!chosen) {
	    fprintf(stderr, "No suitable network interface found\n");
	    fprintf(stderr, "The following interfaces are available:\n");

	    for (ifrp = ibuf ; ifrp < ifend;
#ifdef IFREQ_SIZE
		 ifrp = IFREQ_SIZE(*ifrp) + (char *)ifrp
#else
		 ifrp++
#endif
		 ) {
		char buffer[16];

		if(ifrp->ifr_addr.sa_family != PF_INET)
		    continue;

		fprintf(stderr, "\t%s\t%s\n",
			ifrp->ifr_name, 
			udpc_getIpString((struct sockaddr_in *)&ifrp->ifr_addr, buffer));
	    }
	    exit(1);
	}

	net_if->name = strdup(chosen->ifr_name);

#ifdef HAVE_STRUCT_IP_MREQN_IMR_IFINDEX
	/* Index for multicast subscriptions */
	if(ioctl(s,  SIOCGIFINDEX, chosen) < 0)
	{
			//TODO: FIXME
	    //udpc_fatal(-1, "Error getting index for %s: %s", net_if->name, strerror(errno));
	}
	net_if->index = chosen->ifr_ifindex;
#endif

	/* Broadcast */
	if(ioctl(s,  SIOCGIFBRDADDR, chosen) < 0)
	{
			//TODO: FIXME
	    //udpc_fatal(-1, "Error getting broadcast address for %s: %s", net_if->name, strerror(errno));
	}
	net_if->bcast = getSinAddr(&chosen->ifr_ifru.ifru_broadaddr);

	close(s);

#else /* __MINGW32__ */

	/* WINSOCK initialization */	
	wVersionRequested = MAKEWORD(2, 0); /* Request Winsock v2.0 */
	if (WSAStartup(wVersionRequested, &wsaData) != 0) /* Load Winsock DLL */ {
	    fprintf(stderr,"WSAStartup() failed");
	    exit(1);
	}
	/* End WINSOCK initialization */
	

	a=0;
	r=GetIpAddrTable(iptab, &a, TRUE);
	iptab=malloc(a);
	r=GetIpAddrTable(iptab, &a, TRUE);

	a=0;
	r=GetIfTable(iftab, &a, TRUE);
	iftab=malloc(a);
	r=GetIfTable(iftab, &a, TRUE);

	if(wanted && !strncmp(wanted, "eth", 3) && wanted[3]) {
	    char *ptr;
	    int n = strtoul(wanted+3, &ptr, 10);
	    if(!*ptr)
		wantedEtherNo=n;
	}

	for(i=0; i<iptab->dwNumEntries; i++) {
	    int goodness=-1;
	    unsigned long iaddr;
	    int isEther=0;
	    MIB_IFROW *ifrow;

	    iprow = &iptab->table[i];
	    iaddr = iprow->dwAddr;

	    ifrow = getIfRow(iftab, iprow->dwIndex);

	    if(ifrow && ifrow->dwPhysAddrLen == 6 && iprow->dwBCastAddr) {
		isEther=1;
		etherNo++;
	    }

	    if(wanted) {
		if(isAddress && iaddr == wantedAddress.s_addr) {
		    goodness=8;
		} else if(isEther && wantedEtherNo == etherNo) {
			goodness=9;
		} else if(ifrow->dwPhysAddrLen) {
		    int j;
		    const char *ptr=wanted;
		    for(j=0; *ptr && j<ifrow->dwPhysAddrLen; j++) {
			int digit = strtoul(ptr, (char**)&ptr, 16);
			if(digit != ifrow->bPhysAddr[j])
			    break; /* Digit mismatch */
			if(*ptr == '-' || *ptr == ':') {
			    ptr++;
			}
		    }
		    if(!*ptr && j == ifrow->dwPhysAddrLen) {
			goodness=9;
		    }
		}
	    } else {
		if(iaddr == 0) {
		    /* disregard interfaces whose address is zero */
		    goodness = 1;
		} else if(iaddr == htonl(0x7f000001)) {
		    /* disregard localhost type devices */
		    goodness = 2;
		} else if(isEther) {
		    /* prefer ethernet */
		    goodness = 6;
		} else if(ifrow->dwPhysAddrLen) {
		    /* then prefer interfaces which have a physical address */
		    goodness = 4;
		} else {
		    goodness = 3;
		}
	    }

	    goodness = goodness * 2;
	    /* If all else is the same, prefer interfaces that 
	     * have broadcast */
	    if(goodness >= lastGoodness) {
		/* Privilege broadcast-enabled interfaces */
		if(iprow->dwBCastAddr)
		    goodness++;
	    }

	    if(goodness > lastGoodness) {
		chosen = iprow;
		chosenIf = ifrow;
		lastGoodness = goodness;
	    }
	}
	
	if(!chosen) {
	    fprintf(stderr, "No suitable network interface found%s%s\n", 
		    wanted ? " for " : "", wanted ? wanted : "");
	    fprintf(stderr, "The following interfaces are available:\n");

	    for(i=0; i<iptab->dwNumEntries; i++) {
		char buffer[16];
		struct sockaddr_in addr;
		MIB_IFROW *ifrow;
		char *name=NULL;
		iprow = &iptab->table[i];
		addr.sin_addr.s_addr = iprow->dwAddr;
		ifrow = getIfRow(iftab, iprow->dwIndex);
		name = fmtName(ifrow);
		fprintf(stderr, " %15s  %s\n",
			udpc_getIpString(&addr, buffer),
			name ? name : "");
		if(name)
		    free(name);
	    }
	    exit(1);
	}

	net_if->bcast.s_addr = net_if->addr.s_addr = chosen->dwAddr;
	if(chosen->dwBCastAddr)
	    net_if->bcast.s_addr |= ~chosen->dwMask;
	if(chosenIf) {
	    net_if->name = fmtName(chosenIf);
	} else {
	    net_if->name = "*";
	}
	free(iftab);
	free(iptab);
#endif /* __MINGW32__ */

	return net_if;
}

#define IP_MREQN ip_mreqn

int fillMreq(net_if_t *net_if, struct in_addr addr,
		    struct IP_MREQN *mreq) {
#ifdef HAVE_STRUCT_IP_MREQN_IMR_IFINDEX
    mreq->imr_ifindex = net_if->index;
    mreq->imr_address.s_addr = 0;
#else
    mreq->imr_interface = net_if->addr;
#endif
    mreq->imr_multiaddr = addr;

    return 0;
}

int mcastOp(int sock, net_if_t *net_if, struct in_addr addr,
		   int code, const char *message) {
    struct IP_MREQN mreq;
    int r;
    
    fillMreq(net_if, addr, &mreq);
    r = setsockopt(sock, SOL_IP, code, (char*)&mreq, sizeof(mreq));
    if(r < 0) {
	perror(message);
	exit(1);
    }
    return 0;
}

int udpc_setMcastDestination(int sock, net_if_t *net_if, struct sockaddr_in *addr) {
#ifdef WINDOWS
    int r;
    struct sockaddr_in interface_addr;
    struct in_addr if_addr;
    getMyAddress(net_if, &interface_addr);
    if_addr = getSinAddr(&interface_addr);
    r = setsockopt (sock, IPPROTO_IP, IP_MULTICAST_IF, 
		    (char *) &if_addr, sizeof(if_addr));
    if(r < 0)
	fatal(1, "Set multicast send interface");
    return 0;
#else
    /* IP_MULTICAST_IF not correctly supported on Cygwin */
    return mcastOp(sock, net_if, getSinAddr(addr), IP_MULTICAST_IF,
		   "Set multicast send interface");
#endif
}

int initSockAddress(addr_type_t addr_type,
			   net_if_t *net_if, 
			   in_addr_t ip,
			   unsigned short port, 
			   struct sockaddr_in *addr) {
    memset ((char *) addr, 0, sizeof(struct sockaddr_in));
    addr->sin_family = AF_INET;
    addr->sin_port = htons(port);

	//TODO: FIXME
    //if(!net_if && addr_type != ADDR_TYPE_MCAST)
	//udpc_fatal(1, "initSockAddr without ifname\n");

    switch(addr_type) {
    case ADDR_TYPE_UCAST:
	addr->sin_addr = net_if->addr;
	break;
    case ADDR_TYPE_BCAST:
	addr->sin_addr = net_if->bcast;
	break;
    case ADDR_TYPE_MCAST:
	addr->sin_addr.s_addr = ip;
	break;
    }
    return 0;
}

int mcastListen(int sock, net_if_t *net_if, struct sockaddr_in *addr) {
    return mcastOp(sock, net_if, getSinAddr(addr), IP_ADD_MEMBERSHIP,
		   "Subscribe to multicast group");
}

int udpc_makeSocket(addr_type_t addr_type, 
	       net_if_t *net_if, 
	       struct sockaddr_in *tmpl,
	       int port) {
    int ret, s;
    struct sockaddr_in myaddr;
    in_addr_t ip=0;

#ifdef WINDOWS
    static int lastSocket=-1;
    /* Very ugly hack, but hey!, this is for Windows */

    if(addr_type == ADDR_TYPE_MCAST) {
	mcastListen(lastSocket, net_if, tmpl);
	return -1;
    } else if(addr_type != ADDR_TYPE_UCAST)
	return -1;
#endif

    s = socket(PF_INET, SOCK_DGRAM, 0);
    if (s < 0) {
	perror("make socket");
	exit(1);
    }

    if(addr_type == ADDR_TYPE_MCAST && tmpl != NULL) {
	ip = tmpl->sin_addr.s_addr;
    }

    ret = initSockAddress(addr_type, net_if, ip, port, &myaddr);
	//TODO: FIXME
    //if(ret < 0)
//	udpc_fatal(1, "Could not get socket address fot %d/%s", 
//		   addr_type, net_if->name);
if (addr_type == ADDR_TYPE_BCAST)
{
//cerr << "for addr_type == ADDR_TYPE_BCAST, myaddr.sin_addr.s_addr = 0x" << hex << myaddr.sin_addr.s_addr << dec << endl;
}
    if(addr_type == ADDR_TYPE_BCAST && myaddr.sin_addr.s_addr == 0) {
      /* Attempting to bind to broadcast address on not-broadcast media ... */
      closesocket(s);
      return -1;
    }
    ret = bind(s, (struct sockaddr *) &myaddr, sizeof(myaddr));
	//TODO: FIXME
//    if (ret < 0) {
//	char buffer[16];
//	udpc_fatal(1, "bind socket to %s:%d (%s)\n",
//		   udpc_getIpString(&myaddr, buffer), 
//		   udpc_getPort(&myaddr),
//		   strerror(errno));
//    }

    if(addr_type == ADDR_TYPE_MCAST)
	mcastListen(s, net_if, &myaddr);
#ifdef WINDOWS
    lastSocket=s;
#endif
    return s;
}

int udpc_setSocketToBroadcast(int sock) {
    /* set the socket to broadcast */
    int p = 1;
    return setsockopt(sock, SOL_SOCKET, SO_BROADCAST, (char *)&p, sizeof(int));
}

int udpc_getBroadCastAddress(net_if_t *net_if, struct sockaddr_in *addr, 
			short port){
    int r= initSockAddress(ADDR_TYPE_BCAST, net_if, INADDR_ANY, port, addr);
    if(addr->sin_addr.s_addr == 0) {
      /* Quick hack to make it work for loopback */
      struct sockaddr_in ucast;
      initSockAddress(ADDR_TYPE_UCAST, net_if, INADDR_ANY, port, &ucast);

      if((ntohl(ucast.sin_addr.s_addr) & 0xff000000) == 0x7f000000)
	addr->sin_addr.s_addr = ucast.sin_addr.s_addr;
    }
    return r;
}

int safe_inet_aton(const char *address, struct in_addr *ip) {
    if(!INET_ATON(address, ip))
	{
	//TODO: FIXME
	//udpc_fatal(-1, "Bad address %s", address);
	}
    return 0;
}

int udpc_getMcastAllAddress(struct sockaddr_in *addr, const char *address,
		       short port){
    struct in_addr ip;
    int ret;

    if(address == NULL || address[0] == '\0')
	safe_inet_aton("224.0.0.1", &ip);
    else {
	if((ret=safe_inet_aton(address, &ip))<0)
	   return ret;
    }
    return initSockAddress(ADDR_TYPE_MCAST, NULL, ip.s_addr, port, addr);
}

void setPort(struct sockaddr_in *addr, unsigned short port) {
    ((struct sockaddr_in *) addr)->sin_port = htons(port);
}

int isMcastAddress(struct sockaddr_in *addr) {
    int ip = ntohl(addr->sin_addr.s_addr) >> 24;
    return ip >= 0xe0 && ip < 0xf0;
}

void udpc_clearIp(struct sockaddr_in *addr) {
    addr->sin_addr.s_addr = 0;
    addr->sin_family = AF_INET;
}

void udpc_setSendBuf(int sock, unsigned int bufsize) {
    if(setsockopt(sock, SOL_SOCKET, SO_SNDBUF, (char*)&bufsize, sizeof(bufsize))< 0)
	perror("Set send buffer");
}

int udpc_setTtl(int sock, int ttl) {
    /* set the socket to broadcast */
    return setsockopt(sock, SOL_IP, IP_MULTICAST_TTL, (char*)&ttl, sizeof(int));
}

int udpc_getMyAddress(net_if_t *net_if, struct sockaddr_in *addr) {
    return initSockAddress(ADDR_TYPE_UCAST, net_if, INADDR_ANY, 0, addr);
}

void udpc_getDefaultMcastAddress(net_if_t *net_if, struct sockaddr_in *mcast) {
    udpc_getMyAddress(net_if, mcast);
    mcast->sin_addr.s_addr &= htonl(0x07ffffff);
    mcast->sin_addr.s_addr |= htonl(0xe8000000);
}

void udpc_copyIpFrom(struct sockaddr_in *dst, struct sockaddr_in *src) {
    dst->sin_addr = src->sin_addr;
    dst->sin_family = src->sin_family;
}

int udpc_getSelectedSock(int *socks, int nr, fd_set *read_set) {
    int i;
    int maxFd;
    maxFd=-1;
    for(i=0; i<nr; i++) {
	if(socks[i] == -1)
	    continue;
	if(FD_ISSET(socks[i], read_set))
	    return socks[i];
    }
    return -1;
}

int udpc_ipIsEqual(struct sockaddr_in *left, struct sockaddr_in *right) {
    return getSinAddr(left).s_addr == getSinAddr(right).s_addr;
}

void udpc_closeSock(int *socks, int nr, int target) {
    int i;
    int sock = socks[target];

    socks[target]=-1;
    for(i=0; i<nr; i++)
	if(socks[i] == sock)
	    return;
    closesocket(sock);
}
int prepareForSelect(int *socks, int nr, fd_set *read_set) {
    int i;
    int maxFd;
    FD_ZERO(read_set);
    maxFd=-1;
    for(i=0; i<nr; i++) {
	if(socks[i] == -1)
	    continue;
	FD_SET(socks[i], read_set);
	if(socks[i] > maxFd)
	    maxFd = socks[i];
    }
    return maxFd;
}

int doReceive(int s, void *message, size_t len, 
	      struct sockaddr_in *from, int portBase) {
    socklen_t slen;
    int r;
    unsigned short port;

    slen = sizeof(*from);
#ifdef LOSSTEST
    loseRecvPacket(s);
#endif
    r = recvfrom(s, message, len, 0, (struct sockaddr *)from, &slen);
    if (r < 0)
	return r;
    port = ntohs(from->sin_port);
    if(port != RECEIVER_PORT(portBase) && port != SENDER_PORT(portBase)) {
	return -1;
    }
/*    flprintf("recv: %08x %d\n", *(int*) message, r);*/
    return r;
}

participantsDb_t udpc_makeParticipantsDb(void)
{
    return MALLOC(struct participantsDb);
}

int udpc_removeParticipant(struct participantsDb *db, int i) {
    if(db->clientTable[i].used) {
	db->clientTable[i].used = 0;
	db->nrParticipants--;
    }
    return 0;
}

int udpc_lookupParticipant(struct participantsDb *db, struct sockaddr_in *addr) {
    int i;
    for (i=0; i < MAX_CLIENTS; i++) {
	if (db->clientTable[i].used && 
	    udpc_ipIsEqual(&db->clientTable[i].addr, addr)) {
	    return i;
	}
    }
    return -1;
}

int udpc_nrParticipants(participantsDb_t db) {
    return db->nrParticipants;
}

int udpc_addParticipant(participantsDb_t db,
		   struct sockaddr_in *addr, 
		   int capabilities,
		   unsigned int rcvbuf,
		   int pointopoint) {
//cerr << "adding a participant..." << endl;
    int i;

    if((i = udpc_lookupParticipant(db, addr)) >= 0)
	return i;

    for (i=0; i < MAX_CLIENTS; i++) {
	if (!db->clientTable[i].used) {
	    db->clientTable[i].addr = *addr;
	    db->clientTable[i].used = 1;
	    db->clientTable[i].capabilities = capabilities;
	    db->clientTable[i].rcvbuf = rcvbuf;
	    db->nrParticipants++;

	    return i;
	} else if(pointopoint)
	    return -1;
    }

    return -1; /* no space left in participant's table */
}

int selectWithoutConsole(int maxFd, 
		      fd_set *read_set, struct timeval *tv) {
    int ret;

    ret = select(maxFd, read_set, NULL, NULL, tv);
    if(ret < 0)
	return -1;
    return ret;
}

#if 0
void sendHello(struct net_config *net_config, int sock,
	       int streaming) {
    struct hello hello;
    /* send hello message */
    if(streaming)
	hello.opCode = htons(CMD_HELLO_STREAMING);
    else
	hello.opCode = htons(CMD_HELLO);
    hello.reserved = 0;
    hello.capabilities = htonl(net_config->capabilities);
    udpc_copyToMessage(hello.mcastAddr,&net_config->dataMcastAddr);
    hello.blockSize = htons(net_config->blockSize);
    //rgWaitAll(net_config, sock, net_config->controlMcastAddr.sin_addr.s_addr, sizeof(hello));
    BCAST_CONTROL(sock, hello);
}
#endif

int checkClientWait(participantsDb_t db, 
			   struct net_config *net_config,
			   time_t *firstConnected)
{
    time_t now;
    if (!udpc_nrParticipants(db) || !firstConnected || !*firstConnected)
	return 0; /* do not start: no receivers */

    now = time(0);
    /*
     * If we have a max_client_wait, start the transfer after first client
     * connected + maxSendWait
     */
    if(net_config->max_receivers_wait &&
       (now >= *firstConnected + net_config->max_receivers_wait)) {
#ifdef USE_SYSLOG
	    syslog(LOG_INFO, "max wait[%d] passed: starting", 
			    net_config->max_receivers_wait );
#endif
	return 1; /* send-wait passed: start */
    }

    /*
     * Otherwise check to see if the minimum of clients
     *  have checked in.
     */
    else if (udpc_nrParticipants(db) >= net_config->min_receivers &&
	/*
	 *  If there are enough clients and there's a min wait time, we'll
	 *  wait around anyway until then.
	 *  Otherwise, we always transfer
	 */
	(!net_config->min_receivers_wait || 
	 now >= *firstConnected + net_config->min_receivers_wait)) {
#ifdef USE_SYSLOG
	    syslog(LOG_INFO, "min receivers[%d] reached: starting", 
			    net_config->min_receivers );
#endif
	    return 1;
    } else
	return 0;
}

int sendConnectionReply(participantsDb_t db,
			       int sock,
			       struct net_config *config,
			       struct sockaddr_in *client, 
			       int capabilities,
			       unsigned int rcvbuf) {
    struct connectReply reply;

    if(rcvbuf == 0)
	rcvbuf = 65536;

    if(capabilities & CAP_BIG_ENDIAN) {
	reply.opCode = htons(CMD_CONNECT_REPLY);
	reply.clNr = 
	    htonl(udpc_addParticipant(db,
				      client, 
				      capabilities,
				      rcvbuf,
				      config->flags & FLAG_POINTOPOINT));
	reply.blockSize = htonl(config->blockSize);
    } else {
	//TODO: FIXME
	//udpc_fatal(1, "Little endian protocol no longer supported");
    }
    reply.reserved = 0;

    if(config->flags & FLAG_POINTOPOINT) {
	udpc_copyIpFrom(&config->dataMcastAddr, client);
    }

    /* new parameters: always big endian */
    reply.capabilities = ntohl(config->capabilities);
    udpc_copyToMessage(reply.mcastAddr,&config->dataMcastAddr);
    /*reply.mcastAddress = mcastAddress;*/
    //rgWaitAll(config, sock, client->sin_addr.s_addr, sizeof(reply));
    if(SEND(sock, reply, *client) < 0) {
	perror("reply add new client");
	return -1;
    }
    return 0;
}

int mainDispatcher(int *fd, int nr,
			  participantsDb_t db,
			  struct net_config *net_config,
			  int *tries,
			  time_t *firstConnected)
{
    struct sockaddr_in client;
    union message fromClient;
    fd_set read_set;
    int ret;
    int msgLength;
    int startNow=0;
    int selected;

    if (firstConnected && !*firstConnected && udpc_nrParticipants(db)) {
	*firstConnected = time(0);
#ifdef USE_SYSLOG
        syslog(LOG_INFO,
	 "first connection: min wait[%d] secs - max wait[%d] - min clients[%d]",
	  net_config->min_receivers_wait, net_config->max_receivers_wait, 
	  net_config->min_receivers );
#endif
    }

    while(!startNow) {
	struct timeval tv;
	struct timeval *tvp;
	int nr_desc;

	int maxFd = prepareForSelect(fd, nr, &read_set);

	if(net_config->rexmit_hello_interval) {
	    tv.tv_usec = (net_config->rexmit_hello_interval % 1000)*1000;
	    tv.tv_sec = net_config->rexmit_hello_interval / 1000;
	    tvp = &tv;
	} else if(firstConnected && udpc_nrParticipants(db)) {
	    tv.tv_usec = 0;
	    tv.tv_sec = 2;
	    tvp = &tv;
	} else
	    tvp = 0;
	nr_desc = selectWithoutConsole(maxFd+1, &read_set, tvp);
	if(nr_desc < 0) {
	    perror("select");
	    return -1;
	}
	if(nr_desc > 0)
	    /* key pressed, or receiver activity */
	    break;

	if(net_config->rexmit_hello_interval) {
	    /* retransmit hello message */
	    udpc_sendHello(net_config, fd[0], 0);
	    (*tries)++;
	    if(net_config->autostart != 0 && *tries > net_config->autostart)
		startNow=1;
	}

	if(firstConnected)
	    startNow = 
		startNow || checkClientWait(db, net_config, firstConnected);
    }

    selected = udpc_getSelectedSock(fd, nr, &read_set);
    if(selected == -1)
	return startNow;

    BZERO(fromClient); /* Zero it out in order to cope with short messages
			* from older versions */

    msgLength = RECV(selected, fromClient, client, net_config->portBase);
    if(msgLength < 0) {
	perror("problem getting data from client");
	return 0; /* don't panic if we get weird messages */
    }

    if(net_config->flags & FLAG_ASYNC)
	return 0;

    switch(ntohs(fromClient.opCode)) {
	case CMD_CONNECT_REQ:
	    sendConnectionReply(db, fd[0],
				net_config,
				&client, 
				CAP_BIG_ENDIAN |
				ntohl(fromClient.connectReq.capabilities),
				ntohl(fromClient.connectReq.rcvbuf));
	    return startNow;
	case CMD_GO:
	    return 1;
	case CMD_DISCONNECT:
	    ret = udpc_lookupParticipant(db, &client);
	    if (ret >= 0)
		udpc_removeParticipant(db, ret);
	    return startNow;
	default:
	    break;
    }

    return startNow;
}

static int isPointToPoint(participantsDb_t db, int flags) {
    if(flags & FLAG_POINTOPOINT)
	return 1;
    if(flags & (FLAG_NOPOINTOPOINT | FLAG_ASYNC))
	return 0;
    return udpc_nrParticipants(db) == 1;
}

int getProducedAmount(produconsum_t pc) {
    unsigned int produced = pc->produced;
    unsigned int consumed = pc->consumed;
    if(produced < consumed)
	return produced + 2 * pc->size - consumed;
    else
	return produced - consumed;
}

int _consumeAny(produconsum_t pc, unsigned int minAmount,
		       struct timespec *ts) {
    unsigned int amount;
#if DEBUG
    flprintf("%s: Waiting for %d bytes (%d:%d)\n", 
	    pc->name, minAmount, pc->consumed, pc->produced);
#endif
    pc->consumerIsWaiting=1;
    amount = getProducedAmount(pc);
    if(amount >= minAmount || pc->atEnd) {	
	pc->consumerIsWaiting=0;
#if DEBUG
	flprintf("%s: got %d bytes\n",pc->name, amount);
#endif
	return amount;
    }
    pthread_mutex_lock(&pc->mutex);
    while((amount=getProducedAmount(pc)) < minAmount && !pc->atEnd) {
#if DEBUG
	flprintf("%s: ..Waiting for %d bytes (%d:%d)\n", 
		pc->name, minAmount, pc->consumed, pc->produced);
#endif
	if(ts == 0)
	    pthread_cond_wait(&pc->cond, &pc->mutex);
	else {
	    int r;
#if DEBUG
	    flprintf("Before timed wait\n");
#endif
	    r=pthread_cond_timedwait(&pc->cond, &pc->mutex, ts);
#if DEBUG
	    flprintf("After timed wait %d\n", r);
#endif
	    if(r == ETIMEDOUT) {
		amount=getProducedAmount(pc);
		break;
	    }
	}
    }
    pthread_mutex_unlock(&pc->mutex);
#if DEBUG
    flprintf("%s: Got them %d (for %d) %d\n", pc->name, 
	    amount, minAmount, pc->atEnd);
#endif
    pc->consumerIsWaiting=0;
    return amount;
}

produconsum_t pc_makeProduconsum(int size, const char *name)
{
    produconsum_t pc = MALLOC(struct produconsum);
    pc->size = size;
    pc->produced = 0;
    pc->consumed = 0;
    pc->atEnd = 0;
    pthread_mutex_init(&pc->mutex, NULL);
    pc->consumerIsWaiting = 0;
    pthread_cond_init(&pc->cond, NULL);
    pc->name = name;
    return pc;
}

int pc_consumeAnyWithTimeout(produconsum_t pc, struct timespec *ts)
{
    return _consumeAny(pc, 1, ts);
}

unsigned int pc_getProducerPosition(produconsum_t pc)
{
    return pc->produced % pc->size;
}

unsigned int pc_getWaiting(produconsum_t pc)
{
    return getProducedAmount(pc);
}

int pc_consumeAny(produconsum_t pc)
{
    return _consumeAny(pc, 1, 0);
}

int pc_consume(produconsum_t pc, int amount)
{
    return _consumeAny(pc, amount, 0);
}

void wakeConsumer(produconsum_t pc)
{
    if(pc->consumerIsWaiting) {
	pthread_mutex_lock(&pc->mutex);
	pthread_cond_signal(&pc->cond);
	pthread_mutex_unlock(&pc->mutex);
    }
}

void pc_produceEnd(produconsum_t pc)
{
    pc->atEnd = 1;
    wakeConsumer(pc);
}

int pc_consumed(produconsum_t pc, int amount)
{
    unsigned int consumed = pc->consumed;
    if(consumed >= 2*pc->size - amount) {
	consumed += amount - 2 *pc->size;
    } else {
	consumed += amount;
    }
    pc->consumed = consumed;
    return amount;
}

unsigned int pc_getConsumerPosition(produconsum_t pc)
{
    return pc->consumed % pc->size;
}

void pc_produce(produconsum_t pc, unsigned int amount)
{
    unsigned int produced = pc->produced;
    unsigned int consumed = pc->consumed;

    /* sanity checks:
     * 1. should not produce more than size
     * 2. do not pass consumed+size
     */
    if(amount > pc->size) {
	//TODO: FIXME
	//udpc_fatal(1, "Buffer overflow in produce %s: %d > %d \n", pc->name, amount, pc->size);
    }

    produced += amount;
    if(produced >= 2*pc->size)
	produced -= 2*pc->size;

    if(produced > consumed + pc->size ||
       (produced < consumed && produced > consumed - pc->size)) {
	//TODO: FIXME
	//udpc_fatal(1, "Buffer overflow in produce %s: %d > %d [%d] \n", pc->name, produced, consumed, pc->size);
    }

    pc->produced = produced;
    wakeConsumer(pc);
}

void udpc_initFifo(struct fifo *fifo, int blockSize)
{
    fifo->dataBufSize = blockSize * 4096;
    fifo->dataBuffer = (unsigned char*)malloc(fifo->dataBufSize+4096);
    fifo->dataBuffer += 4096 - (((unsigned long)fifo->dataBuffer) % 4096);

    /* Free memory queue is initially full */
    fifo->freeMemQueue = pc_makeProduconsum(fifo->dataBufSize, "free mem");
    pc_produce(fifo->freeMemQueue, fifo->dataBufSize);

    fifo->data = pc_makeProduconsum(fifo->dataBufSize, "receive");
}

THREAD_RETURN returnChannelMain(void *args) {
    struct returnChannel *returnChannel = (struct returnChannel *) args;

    while(1) {
	struct sockaddr_in from;
	int clNo;
	int pos = pc_getConsumerPosition(returnChannel->freeSpace);
	pc_consumeAny(returnChannel->freeSpace);

	RECV(returnChannel->rcvSock, 
	     returnChannel->q[pos].msg, from,
	     returnChannel->config->portBase);
	clNo = udpc_lookupParticipant(returnChannel->participantsDb, &from);
	if (clNo < 0) { 
	    /* packet from unknown provenance */
	    continue;
	}
	returnChannel->q[pos].clNo = clNo;
	pc_consumed(returnChannel->freeSpace, 1);
	pc_produce(returnChannel->incoming, 1);
    }
    return 0;
}

void initReturnChannel(struct returnChannel *returnChannel,
			      struct net_config *config,
			      int sock) {
    returnChannel->config = config;
    returnChannel->rcvSock = sock;
    returnChannel->freeSpace = pc_makeProduconsum(QUEUE_SIZE,"msg:free-queue");
    pc_produce(returnChannel->freeSpace, QUEUE_SIZE);
    returnChannel->incoming = pc_makeProduconsum(QUEUE_SIZE,"msg:incoming");

    pthread_create(&returnChannel->thread, NULL,
		   returnChannelMain, returnChannel);

}

void senderStatsAddBytes(sender_stats_t ss, long bytes) {
    if(ss != NULL) {
	ss->totalBytes += bytes;

	if(ss->bwPeriod) {
	    double tdiff, bw;
	    struct timeval tv;
	    gettimeofday(&tv, 0);
	    ss->periodBytes += bytes;
	    if(tv.tv_sec - ss->periodStart.tv_sec < ss->bwPeriod-1)
		return;
	    tdiff = (tv.tv_sec-ss->periodStart.tv_sec) * 1000000.0 +
		tv.tv_usec - ss->periodStart.tv_usec;
	    if(tdiff < ss->bwPeriod * 1000000.0)
		return;
	    bw = ss->periodBytes * 8.0 / tdiff;
	    ss->periodBytes=0;
	    ss->periodStart = tv;
	}
    }
}
int ackSlice(struct slice *slice, struct net_config *net_config,
		    struct fifo *fifo, sender_stats_t stats)
{
    if(slice->state == slice::SLICE_ACKED)
	/* already acked */
	return 0;
    if(!(net_config->flags & FLAG_SN)) {
	if(net_config->discovery == net_config::DSC_DOUBLING) {
		net_config->sliceSize += net_config->sliceSize / 4;
	   if(net_config->sliceSize >= net_config->max_slice_size) {
	       net_config->sliceSize = net_config->max_slice_size;
	       net_config->discovery = net_config::DSC_REDUCING;
	   }
       }
    }
    slice->state = slice::SLICE_ACKED;
    pc_produce(fifo->freeMemQueue, slice->bytes);

    /* Statistics */
    senderStatsAddBytes(stats, slice->bytes);

    /* End Statistics */

    return 0;
}

int isSliceAcked(struct slice *slice)
{
    if(slice->state == slice::SLICE_ACKED) {
	return 1;
    } else {
	return 0;
    }
}

int freeSlice(sender_state_t sendst, struct slice *slice) {
    int i;
    i = slice - sendst->slices;
    slice->state = slice::SLICE_PRE_FREE;
    while(1) {
	int pos = pc_getProducerPosition(sendst->free_slices_pc);
	if(sendst->slices[pos].state == slice::SLICE_PRE_FREE)
	    sendst->slices[pos].state = slice::SLICE_FREE;
	else
	    break;
	pc_produce(sendst->free_slices_pc, 1);
    }
    return 0;
}

int isSliceXmitted(struct slice *slice) {
    if(slice->state == slice::SLICE_XMITTED) {
	return 1;
    } else {
	return 0;
    }
}

int getSliceBlocks(struct slice *slice, struct net_config *net_config)
{
    return (slice->bytes + net_config->blockSize - 1) / net_config->blockSize;
}

int sendReqack(struct slice *slice, struct net_config *net_config,
		      struct fifo *fifo, sender_stats_t stats,
		      int sock)
{
    /* in async mode, just confirm slice... */
    if((net_config->flags & FLAG_ASYNC) && slice->bytes != 0) {
	ackSlice(slice, net_config, fifo, stats);
	return 0;
    }

    if((net_config->flags & FLAG_ASYNC)
#ifdef BB_FEATURE_UDPCAST_FEC
       && 
       (net_config->flags & FLAG_FEC)
#endif
       ) {
	return 0;
    }

    if(!(net_config->flags & FLAG_SN) && slice->rxmitId != 0) {
	int nrBlocks;
	nrBlocks = getSliceBlocks(slice, net_config);
	if(slice->lastGoodBlock != 0 && slice->lastGoodBlock < nrBlocks) {
	    net_config->discovery = net_config::DSC_REDUCING;
	    if (slice->lastGoodBlock < net_config->sliceSize / 2) {
		net_config->sliceSize = net_config->sliceSize / 2;
	    } else {
		net_config->sliceSize = slice->lastGoodBlock;
	    }
	    if(net_config->sliceSize < 32) {
		/* a minimum of 32 */
		net_config->sliceSize = 32;
	    }
	}
    }

    slice->lastGoodBlock = 0;
    slice->sl_reqack.ra.opCode = htons(CMD_REQACK);
    slice->sl_reqack.ra.sliceNo = htonl(slice->sliceNo);
    slice->sl_reqack.ra.bytes = htonl(slice->bytes);

    slice->sl_reqack.ra.reserved = 0;
    memcpy((void*)&slice->answeredSet,(void*)&slice->sl_reqack.readySet,
	   sizeof(slice->answeredSet));
    slice->nrAnswered = slice->nrReady;

    /* not everybody is ready yet */
    slice->needRxmit = 0;
    memset(slice->rxmitMap, 0, sizeof(slice->rxmitMap));
    memset(slice->isXmittedMap, 0, sizeof(slice->isXmittedMap));
    slice->sl_reqack.ra.rxmit = htonl(slice->rxmitId);
    
    //rgWaitAll(net_config, sock, net_config->dataMcastAddr.sin_addr.s_addr, sizeof(slice->sl_reqack));
    BCAST_DATA(sock, slice->sl_reqack);
    return 0;
}

struct slice *findSlice(struct slice *slice1,
			       struct slice *slice2,
			       int sliceNo)
{
    if(slice1 != NULL && slice1->sliceNo == sliceNo)
	return slice1;
    if(slice2 != NULL && slice2->sliceNo == sliceNo)
	return slice2;
    return NULL;
}

void markParticipantAnswered(slice_t slice, int clNo)
{
    if(BIT_ISSET(clNo, slice->answeredSet))
	/* client already has answered */
	return;
    slice->nrAnswered++;
    SET_BIT(clNo, slice->answeredSet);
}

int udpc_isParticipantValid(struct participantsDb *db, int i) {
    return db->clientTable[i].used;
}

void senderSetAnswered(sender_stats_t ss, int clNo) {
    if(ss != NULL)
	ss->clNo = clNo;
}

int handleOk(sender_state_t sendst,
		    struct slice *slice,
		    int clNo)
{
    if(slice == NULL)
	return 0;
    if(!udpc_isParticipantValid(sendst->rc.participantsDb, clNo)) {
	//udpc_flprintf("Invalid participant %d\n", clNo);
	return 0;
    }
    if (BIT_ISSET(clNo, slice->sl_reqack.readySet)) {
        /* client is already marked ready */
    } else {
	SET_BIT(clNo, slice->sl_reqack.readySet);
	slice->nrReady++;
	senderSetAnswered(sendst->stats, clNo);
	markParticipantAnswered(slice, clNo);
    }
    return 0;
}

int handleDisconnect1(struct slice *slice, int clNo)
{    
    if(slice != NULL) {
	if (BIT_ISSET(clNo, slice->sl_reqack.readySet)) {
	    /* avoid counting client both as left and ready */
	    CLR_BIT(clNo, slice->sl_reqack.readySet);
	    slice->nrReady--;
	}
	if (BIT_ISSET(clNo, slice->answeredSet)) {
	    slice->nrAnswered--;
	    CLR_BIT(clNo, slice->answeredSet);
	}
    }
    return 0;
}

int handleDisconnect(participantsDb_t db,
			    struct slice *slice1,
			    struct slice *slice2,
			    int clNo)
{
    handleDisconnect1(slice1, clNo);
    handleDisconnect1(slice2, clNo);
    udpc_removeParticipant(db, clNo);
    return 0;
}

int handleRetransmit(sender_state_t sendst,
			    struct slice *slice,
			    int clNo, unsigned char *map, int rxmit)
{
    unsigned int i;

    if(!udpc_isParticipantValid(sendst->rc.participantsDb, clNo)) {
	//udpc_flprintf("Invalid participant %d\n", clNo);
	return 0;
    }
    if(slice == NULL)
	return 0;    
    if (rxmit < slice->rxmitId) {
	/* late answer to previous Req Ack */
	return 0;
    }
    for(i=0; i <sizeof(slice->rxmitMap) / sizeof(char); i++) {
	slice->rxmitMap[i] |= ~map[i];
    }
    slice->needRxmit = 1;
    markParticipantAnswered(slice, clNo);
    return 0;
}
int handleNextMessage(sender_state_t sendst,
			     struct slice *xmitSlice,
			     struct slice *rexmitSlice)
{
    int pos = pc_getConsumerPosition(sendst->rc.incoming);
    union message *msg = &sendst->rc.q[pos].msg;
    int clNo = sendst->rc.q[pos].clNo;

    pc_consumeAny(sendst->rc.incoming);
    switch(ntohs(msg->opCode)) {
	case CMD_OK:
	    handleOk(sendst, 
		     findSlice(xmitSlice, rexmitSlice, ntohl(msg->ok.sliceNo)), clNo);
	    break;
	case CMD_DISCONNECT:
	    handleDisconnect(sendst->rc.participantsDb, 
			     xmitSlice, rexmitSlice, clNo);
	    break;	    
	case CMD_RETRANSMIT:
	    handleRetransmit(sendst,
			     findSlice(xmitSlice, rexmitSlice,
				       ntohl(msg->retransmit.sliceNo)),
			     clNo,
			     msg->retransmit.map,
			     msg->retransmit.rxmit);
	    break;
	default:
		//TODO: FIXME
	    //udpc_flprintf("Bad command %04x\n", (unsigned short) msg->opCode);
	    break;
    }
    pc_consumed(sendst->rc.incoming, 1);
    pc_produce(sendst->rc.freeSpace, 1);
    return 0;
}

int sendRawData(int sock,
		       struct net_config *config, 
		       char *header, int headerSize, 
		       unsigned char *data, int dataSize)
{
    struct iovec iov[2];
    struct msghdr hdr;
    int packetSize;
    int ret;
    
    iov[0].iov_base = header;
    iov[0].iov_len = headerSize;

    iov[1].iov_base = data;
    iov[1].iov_len = dataSize;

    hdr.msg_name = &config->dataMcastAddr;
    hdr.msg_namelen = sizeof(struct sockaddr_in);
    hdr.msg_iov = iov;
    hdr.msg_iovlen  = 2;
    initMsgHdr(&hdr);

    packetSize = dataSize + headerSize;
    //rgWaitAll(config, sock, config->dataMcastAddr.sin_addr.s_addr, packetSize);
    ret = sendmsg(sock, &hdr, 0);
    if (ret < 0) {
	//TODO: FIXME
    }

    return 0;
}
int transmitDataBlock(sender_state_t sendst, struct slice *slice, int i)
{
    struct fifo *fifo = sendst->fifo;
    struct net_config *config = sendst->config;
    struct dataBlock msg;

    idbassert(i < MAX_SLICE_SIZE);
    
    msg.opCode  = htons(CMD_DATA);
    msg.sliceNo = htonl(slice->sliceNo);
    msg.blockNo = htons(i);

    msg.reserved = 0;
    msg.reserved2 = 0;
    msg.bytes = htonl(slice->bytes);
    
    sendRawData(sendst->socket, config, 
		(char *) &msg, sizeof(msg),
		fifo->dataBuffer + 
		(slice->base + i * config->blockSize) % fifo->dataBufSize,
		config->blockSize);
    return 0;
}

void senderStatsAddRetransmissions(sender_stats_t ss, int retransmissions) {
    if(ss != NULL) {
	ss->retransmissions += retransmissions;
    }
}

int sendSlice(sender_state_t sendst, struct slice *slice,
		     int retransmitting)
{    
    struct net_config *config = sendst->config;

    int nrBlocks, i, rehello;
#ifdef BB_FEATURE_UDPCAST_FEC
    int fecBlocks;
#endif
    int retransmissions=0;

    if(retransmitting) {
	slice->nextBlock = 0;
	if(slice->state != slice::SLICE_XMITTED)
	    return 0;
    } else {
	if(slice->state != slice::SLICE_NEW)
	    return 0;
    }

    nrBlocks = getSliceBlocks(slice, config);
#ifdef BB_FEATURE_UDPCAST_FEC
    if((config->flags & FLAG_FEC) && !retransmitting) {
	fecBlocks = config->fec_redundancy * config->fec_stripes;
    } else {
	fecBlocks = 0;
    }
#endif

    if((sendst->config->flags & FLAG_STREAMING)) {
      rehello = nrBlocks - sendst->config->rehelloOffset;
      if(rehello < 0)
	rehello = 0;
    } else {
      rehello = -1;
    }

    /* transmit the data */
    for(i = slice->nextBlock; i < nrBlocks
#ifdef BB_FEATURE_UDPCAST_FEC
	  + fecBlocks
#endif
	  ; i++) {
	if(retransmitting) {
	    if(!BIT_ISSET(i, slice->rxmitMap) ||
	       BIT_ISSET(i, slice->isXmittedMap)) {
		/* if slice is not in retransmit list, or has _already_
		 * been retransmitted, skip it */
		if(i > slice->lastGoodBlock)
		    slice->lastGoodBlock = i;
		continue;
	    }
	    SET_BIT(i, slice->isXmittedMap);
	    retransmissions++;
	}

	if(i == rehello) {
	    udpc_sendHello(sendst->config, sendst->socket, 1);
	}

	if(i < nrBlocks)
	    transmitDataBlock(sendst, slice, i);
#ifdef BB_FEATURE_UDPCAST_FEC
	else
	    transmitFecBlock(sendst, slice, i - nrBlocks);
#endif
	if(!retransmitting && pc_getWaiting(sendst->rc.incoming)) {
	    i++;
	    break;
	}
    }

    if(retransmissions)
	senderStatsAddRetransmissions(sendst->stats, retransmissions);
    slice->nextBlock = i;
    if(i == nrBlocks
#ifdef BB_FEATURE_UDPCAST_FEC
       + fecBlocks
#endif
       ) {
	slice->needRxmit = 0;
	if(!retransmitting)
	    slice->state = slice::SLICE_XMITTED;
	return 2;
    }
    return 1;
}

int doRetransmissions(sender_state_t sendst,
			     struct slice *slice)
{
    if(slice->state == slice::SLICE_ACKED)
	return 0; /* nothing to do */

    /* FIXME: reduce slice size if needed */
    if(slice->needRxmit) {
	/* do some retransmissions */
	sendSlice(sendst, slice, 1);
    }
    return 0;
}

struct slice *makeSlice(sender_state_t sendst, int sliceNo) {
    struct net_config *config = sendst->config;
    struct fifo *fifo = sendst->fifo;
    int i;
    struct slice *slice=NULL;

    pc_consume(sendst->free_slices_pc, 1);
    i = pc_getConsumerPosition(sendst->free_slices_pc);
    slice = &sendst->slices[i];
    idbassert(slice->state == slice::SLICE_FREE);
    BZERO(*slice);
    pc_consumed(sendst->free_slices_pc, 1);

    slice->base = pc_getConsumerPosition(sendst->fifo->data);
    slice->sliceNo = sliceNo;
    slice->bytes = pc_consume(fifo->data, 10*config->blockSize);

    /* fixme: use current slice size here */
    if(slice->bytes > config->blockSize * config->sliceSize)
	slice->bytes = config->blockSize * config->sliceSize;
    pc_consumed(fifo->data, slice->bytes);
    slice->nextBlock = 0;
    slice->state = slice::SLICE_NEW;
    BZERO(slice->sl_reqack.readySet);
    slice->nrReady = 0;
#ifdef BB_FEATURE_UDPCAST_FEC
    slice->fec_data = sendst->fec_data + (i * config->fec_stripes * 
					  config->fec_redundancy *
					  config->blockSize);
#endif
    return slice;
}

void cancelReturnChannel(struct returnChannel *returnChannel) {
    /* No need to worry about the pthread_cond_wait in produconsum, because
     * at the point where we enter here (to cancel the thread), we are sure
     * that nobody else uses that produconsum any more
     */
    pthread_cancel(returnChannel->thread);
    pthread_join(returnChannel->thread, NULL);
}

THREAD_RETURN netSenderMain(void	*args0)
{
    sender_state_t sendst = (sender_state_t) args0;
    struct net_config *config = sendst->config;
    struct timeval tv;
    struct timespec ts;
    int atEnd = 0;
    int nrWaited=0;
    unsigned long waitAverage=10000; /* Exponential average of last wait times */

    struct slice *xmitSlice=NULL; /* slice being transmitted a first time */
    struct slice *rexmitSlice=NULL; /* slice being re-transmitted */
    int sliceNo = 0;

    /* transmit the data */
    if(config->default_slice_size == 0) {
#ifdef BB_FEATURE_UDPCAST_FEC
	if(config->flags & FLAG_FEC) {
	    config->sliceSize = 
		config->fec_stripesize * config->fec_stripes;
	} else
#endif
	  if(config->flags & FLAG_ASYNC)
	    config->sliceSize = 1024;
	else if (sendst->config->flags & FLAG_SN) {
	    sendst->config->sliceSize = 112;
	} else
	    sendst->config->sliceSize = 130;
	sendst->config->discovery = net_config::DSC_DOUBLING;
    } else {
	config->sliceSize = config->default_slice_size;
#ifdef BB_FEATURE_UDPCAST_FEC
	if((config->flags & FLAG_FEC) &&
	   (config->sliceSize > 128 * config->fec_stripes))
	    config->sliceSize = 128 * config->fec_stripes;
#endif
    }

#ifdef BB_FEATURE_UDPCAST_FEC
    if( (sendst->config->flags & FLAG_FEC) &&
	config->max_slice_size > config->fec_stripes * 128)
      config->max_slice_size = config->fec_stripes * 128;
#endif

    if(config->sliceSize > config->max_slice_size)
	config->sliceSize = config->max_slice_size;

    idbassert(config->sliceSize <= MAX_SLICE_SIZE);

    do {
	/* first, cleanup rexmit Slice if needed */

	if(rexmitSlice != NULL) {
	    if(rexmitSlice->nrReady == 
	       udpc_nrParticipants(sendst->rc.participantsDb)){
#if DEBUG
		flprintf("slice is ready\n");
#endif
		ackSlice(rexmitSlice, sendst->config, sendst->fifo, 
			 sendst->stats);
	    }
	    if(isSliceAcked(rexmitSlice)) {
		freeSlice(sendst, rexmitSlice);
		rexmitSlice = NULL;
	    }
	}

	/* then shift xmit slice to rexmit slot, if possible */
	if(rexmitSlice == NULL &&  xmitSlice != NULL && 
	   isSliceXmitted(xmitSlice)) {
	    rexmitSlice = xmitSlice;
	    xmitSlice = NULL;
	    sendReqack(rexmitSlice, sendst->config, sendst->fifo, sendst->stats,
		       sendst->socket);
	}

	/* handle any messages */
	if(pc_getWaiting(sendst->rc.incoming)) {
#if DEBUG
	    flprintf("Before message %d\n",
		    pc_getWaiting(sendst->rc.incoming));
#endif
	    handleNextMessage(sendst, xmitSlice, rexmitSlice);

	    /* restart at beginning of loop: we may have acked the rxmit
	     * slice, makeing it possible to shift the pipe */
	    continue;
	}

	/* do any needed retransmissions */
	if(rexmitSlice != NULL && rexmitSlice->needRxmit) {
	    doRetransmissions(sendst, rexmitSlice);
	    /* restart at beginning: new messages may have arrived during
	     * retransmission  */
	    continue;
	}

	/* if all participants answered, send req ack */
	if(rexmitSlice != NULL && 
	   rexmitSlice->nrAnswered == 
	   udpc_nrParticipants(sendst->rc.participantsDb)) {
	    rexmitSlice->rxmitId++;
	    sendReqack(rexmitSlice, sendst->config, sendst->fifo, sendst->stats,
		       sendst->socket);
	}

	if(xmitSlice == NULL && !atEnd) {
#if DEBUG
	    flprintf("SN=%d\n", sendst->config->flags & FLAG_SN);
#endif
	    if((sendst->config->flags & FLAG_SN) ||
	       rexmitSlice == NULL) {
#ifdef BB_FEATURE_UDPCAST_FEC
		if(sendst->config->flags & FLAG_FEC) {
		    int i;
		    pc_consume(sendst->fec_data_pc, 1);
		    i = pc_getConsumerPosition(sendst->fec_data_pc);
		    xmitSlice = &sendst->slices[i];
		    pc_consumed(sendst->fec_data_pc, 1);
		} else
#endif
		  {
		    xmitSlice = makeSlice(sendst, sliceNo++);
		}
		if(xmitSlice->bytes == 0)
		    atEnd = 1;
	    }
	}
	 
	if(xmitSlice != NULL && xmitSlice->state == slice::SLICE_NEW) {
	    sendSlice(sendst, xmitSlice, 0);
#if DEBUG
	    flprintf("%d Interrupted at %d/%d\n", xmitSlice->sliceNo, 
		     xmitSlice->nextBlock, 
		     getSliceBlocks(xmitSlice, sendst->config));
#endif
	    continue;
	}
	if(atEnd && rexmitSlice == NULL && xmitSlice == NULL)
	    break;

	if(sendst->config->flags & FLAG_ASYNC)
	    break;

#if DEBUG
	flprintf("Waiting for timeout...\n");
#endif
	gettimeofday(&tv, 0);
	ts.tv_sec = tv.tv_sec;
	ts.tv_nsec = (long int)((tv.tv_usec + 1.1*waitAverage) * 1000);

#ifdef WINDOWS
	/* Windows has a granularity of 1 millisecond in its timer. Take this
	 * into account here */
	#define GRANULARITY 1000000
	ts.tv_nsec += 3*GRANULARITY/2;
	ts.tv_nsec -= ts.tv_nsec % GRANULARITY;
#endif

#define BILLION 1000000000

	while(ts.tv_nsec >= BILLION) {
	    ts.tv_nsec -= BILLION;
	    ts.tv_sec++;
	}

	if(rexmitSlice->rxmitId > 10)
	    /* after tenth retransmission, wait minimum one second */
	    ts.tv_sec++;

	if(pc_consumeAnyWithTimeout(sendst->rc.incoming, &ts) != 0) {
#if DEBUG
	    flprintf("Have data\n");
#endif
	    {
		struct timeval tv2;
		unsigned long timeout;
		gettimeofday(&tv2, 0);
		timeout = 
		    (tv2.tv_sec - tv.tv_sec) * 1000000+
		    tv2.tv_usec - tv.tv_usec;
		if(nrWaited)
		    timeout += waitAverage;
		waitAverage += 9; /* compensate against rounding errors */
		waitAverage = (long unsigned int)((0.9 * waitAverage + 0.1 * timeout));
	    }
	    nrWaited = 0;
	    continue;
	}
	if(rexmitSlice == NULL) {
		//TODO: FIXME
	    //udpc_flprintf("Weird. Timeout and no rxmit slice");
	    break;
	}
	if(nrWaited > 5){
#ifndef WINDOWS
	    /* on Cygwin, we would get too many of those messages... */
	    nrWaited=0;
#endif
	}
	nrWaited++;
	if(rexmitSlice->rxmitId > config->retriesUntilDrop) {
	    int i;
            for(i=0; i < MAX_CLIENTS; i++) {
                if(udpc_isParticipantValid(sendst->rc.participantsDb, i) && 
		   !BIT_ISSET(i, rexmitSlice->sl_reqack.readySet)) {
                    udpc_removeParticipant(sendst->rc.participantsDb, i);
                    if(udpc_nrParticipants(sendst->rc.participantsDb) == 0)
			goto exit_main_loop;
                }
            }
	    continue;
	}
	rexmitSlice->rxmitId++;
	sendReqack(rexmitSlice, sendst->config, sendst->fifo, sendst->stats,
		   sendst->socket);
    } while(udpc_nrParticipants(sendst->rc.participantsDb)||
	    (config->flags & FLAG_ASYNC));
 exit_main_loop:
    cancelReturnChannel(&sendst->rc);
    pc_produceEnd(sendst->fifo->freeMemQueue);
    return 0;
}

int spawnNetSender(struct fifo *fifo,
		   int sock,
		   struct net_config *config,
		   participantsDb_t db)
{
    int i;

    sender_state_t sendst = MALLOC(struct senderState);
    sendst->fifo = fifo;
    sendst->socket = sock;
    sendst->config = config;
    //sendst->stats = stats;
#ifdef BB_FEATURE_UDPCAST_FEC
    if(sendst->config->flags & FLAG_FEC)
      sendst->fec_data =  xmalloc(NR_SLICES *
				  config->fec_stripes * 
				  config->fec_redundancy *
				  config->blockSize);
#endif
    sendst->rc.participantsDb = db;
    initReturnChannel(&sendst->rc, sendst->config, sendst->socket);

    sendst->free_slices_pc = pc_makeProduconsum(NR_SLICES, "free slices");
    pc_produce(sendst->free_slices_pc, NR_SLICES);
    for(i = 0; i <NR_SLICES; i++)
	sendst->slices[i].state = slice::SLICE_FREE;

#ifdef BB_FEATURE_UDPCAST_FEC
    if(sendst->config->flags & FLAG_FEC) {
	/* Free memory queue is initially full */
	fec_init();
	sendst->fec_data_pc = pc_makeProduconsum(NR_SLICES, "fec data");

	pthread_create(&sendst->fec_thread, NULL, fecMain, sendst);
    }
#endif

    pthread_create(&fifo->thread, NULL, netSenderMain, sendst);
    return 0;
}

struct sockaddr_in *udpc_getParticipantIp(participantsDb_t db, int i)
{
    return &db->clientTable[i].addr;
}

int udpc_getParticipantCapabilities(participantsDb_t db, int i)
{
    return db->clientTable[i].capabilities;
}

unsigned int udpc_getParticipantRcvBuf(participantsDb_t db, int i)
{
    return db->clientTable[i].rcvbuf;
}

int pc_consumeContiguousMinAmount(produconsum_t pc, int amount)
{
    int n = _consumeAny(pc, amount, 0);
    int l = pc->size - (pc->consumed % pc->size);
    if(n > l)
	n = l;
    return n;
    
}

#define BLOCKSIZE 4096

void localReader(struct fifo* fifo, const uint8_t* buf, uint32_t len)
{
//cerr << "starting to send " << len << " bytes" << endl;
    uint32_t offset = 0;
    while(1) {
	int pos = pc_getConsumerPosition(fifo->freeMemQueue);
	int bytes = 
	    pc_consumeContiguousMinAmount(fifo->freeMemQueue, BLOCKSIZE);
	if(bytes > (pos + bytes) % BLOCKSIZE)
	    bytes -= (pos + bytes) % BLOCKSIZE;

        if (offset + bytes > len) bytes = len - offset;

//cerr << "sending " << bytes << " bytes from bs..." << endl;
	//bytes = read(in, fifo->dataBuffer + pos, bytes);
	memcpy(fifo->dataBuffer + pos, buf + offset, bytes);
        offset += bytes;

	if (bytes == 0) {
	    /* the end */
	    pc_produceEnd(fifo->data);
	    break;
	} else {
	    pc_consumed(fifo->freeMemQueue, bytes);
	    pc_produce(fifo->data, bytes);
	}
    }
//cerr << "done sending" << endl;
}

unsigned int udpc_getRcvBuf(int sock) {
    unsigned int bufsize;
    socklen_t len=sizeof(int);
    if(getsockopt(sock, SOL_SOCKET, SO_RCVBUF, (char *)&bufsize, &len) < 0)
	return -1;
    return bufsize;
}

int sendConnectReq(struct client_config *client_config,
			  struct net_config *net_config,
			  int haveServerAddress) {
//cerr << "sending a connect request" << endl;
    struct connectReq connectReq;

    if(net_config->flags & FLAG_PASSIVE)
	return 0;

    connectReq.opCode = htons(CMD_CONNECT_REQ);
    connectReq.reserved = 0;
    connectReq.capabilities = htonl(RECEIVER_CAPABILITIES);
    connectReq.rcvbuf = htonl(udpc_getRcvBuf(client_config->S_UCAST));
    if(haveServerAddress)
      return SSEND(connectReq);
    else
      return BCAST_CONTROL(client_config->S_UCAST, connectReq);
}

void udpc_setRcvBuf(int sock, unsigned int bufsize) {
    if(setsockopt(sock, SOL_SOCKET, SO_RCVBUF, 
		  (char*) &bufsize, sizeof(bufsize))< 0)
	{
	//TODO: FIXME
	//perror("Set receiver buffer");
	}
}

void udpc_copyFromMessage(struct sockaddr_in *dst, unsigned char *src) {
    memcpy((char *) &dst->sin_addr, src, sizeof(struct in_addr));
}

unsigned short udpc_getPort(struct sockaddr_in *addr) {
    return ntohs(((struct sockaddr_in *) addr)->sin_port);
}

int udpc_selectSock(int *socks, int nr, int startTimeout) {
    fd_set read_set;
    int r;
    int maxFd;
    struct timeval tv, *tvp;
    if(startTimeout) {
	tv.tv_sec = startTimeout;
	tv.tv_usec = 0;
	tvp = &tv;
    } else {
	tvp = NULL;
    }
    maxFd = prepareForSelect(socks, nr, &read_set);
    r = select(maxFd+1, &read_set, NULL, NULL, tvp);
    if(r < 0)
	return r;
    return udpc_getSelectedSock(socks, nr, &read_set);
}

void udpc_zeroSockArray(int *socks, int nr) {
    int i;

    for(i=0; i<nr; i++)
	socks[i] = -1;
}

unsigned char *getBlockSpace(struct clientState *clst)
{
    int pos;

    if(clst->localPos) {
	clst->localPos--;
	return clst->localBlockAddresses[clst->localPos];
    }

    pc_consume(clst->freeBlocks_pc, 1);
    pos = pc_getConsumerPosition(clst->freeBlocks_pc);
    pc_consumed(clst->freeBlocks_pc, 1);
    return clst->blockAddresses[pos];
}

void setNextBlock(struct clientState *clst)
{
    clst->nextBlock = getBlockSpace(clst);
}

int setupMessages(struct clientState *clst) {
    /* the messages received from the server */
    clst->data_iov[0].iov_base = (void *)&clst->Msg;
    clst->data_iov[0].iov_len = sizeof(clst->Msg);
    
    /* namelen set just before reception */
    clst->data_hdr.msg_iov = clst->data_iov;
    /* iovlen set just before reception */

    initMsgHdr(&clst->data_hdr);
    return 0;
}

struct slice *initSlice(struct clientState *clst, 
			       struct slice *slice,
			       int sliceNo)
{
    idbassert(slice->state == slice::SLICE_FREE || slice->state == slice::SLICE_RECEIVING);

    slice->magic = SLICEMAGIC;
    slice->state = slice::SLICE_RECEIVING;
    slice->blocksTransferred = 0;
    slice->dataBlocksTransferred = 0;
    BZERO(slice->retransmit);
    slice->freePos = 0;
    slice->bytes = 0;
    if(clst->currentSlice != NULL && !clst->currentSlice->bytesKnown) {
	//udpc_fatal(1, "Previous slice size not known\n");
    }
    if(clst->currentSliceNo != sliceNo-1) {
	//udpc_fatal(1, "Slice no mismatch %d <-> %d\n", sliceNo, clst->currentSliceNo);
    }
    slice->bytesKnown = 0;
    slice->sliceNo = sliceNo;

    BZERO(slice->missing_data_blocks);
#ifdef BB_FEATURE_UDPCAST_FEC
    BZERO(slice->fec_stripes);
    BZERO(slice->fec_blocks);
    BZERO(slice->fec_descs);
#endif
    clst->currentSlice = slice;
    clst->currentSliceNo = sliceNo;
    return slice;
}
struct slice *newSlice(struct clientState *clst, int sliceNo)
{
    struct slice *slice=NULL;
    int i;

    pc_consume(clst->free_slices_pc, 1);
    i = pc_getConsumerPosition(clst->free_slices_pc);
    pc_consumed(clst->free_slices_pc, 1);
    slice = &clst->slices[i];
    idbassert(slice->state == slice::SLICE_FREE);

    /* wait for free data memory */
    slice->base = pc_getConsumerPosition(clst->fifo->freeMemQueue);
    pc_consume(clst->fifo->freeMemQueue, 
	       clst->net_config->blockSize * MAX_SLICE_SIZE);
    initSlice(clst, slice, sliceNo);
    return slice;
}

#define ADR(x, bs) (fifo->dataBuffer + \
	(slice->base+(x)*bs) % fifo->dataBufSize)

void closeAllExcept(struct clientState *clst, int fd) {
    int i;
    int *socks = clst->client_config->socks;
    
    if(clst->selectedFd >= 0)
      return;

    clst->selectedFd = fd;
    for(i=1; i<NR_CLIENT_SOCKS; i++)
	if(socks[i] != -1 && socks[i] != fd)
	    udpc_closeSock(socks, NR_CLIENT_SOCKS, i);
}

struct slice *rcvFindSlice(struct clientState *clst, int sliceNo)
{
    if(! clst->currentSlice) {
	/* Streaming mode? */
	clst->currentSliceNo = sliceNo-1;
	return newSlice(clst, sliceNo);
    }
    if(sliceNo <= clst->currentSliceNo) {
	struct slice *slice = clst->currentSlice;
	int pos = slice - clst->slices;
	idbassert(slice == NULL || slice->magic == SLICEMAGIC);
	while(slice->sliceNo != sliceNo) {
	    if(slice->state == slice::SLICE_FREE)
		return NULL;
	    idbassert(slice->magic == SLICEMAGIC);
	    pos--;
	    if(pos < 0)
		pos += NR_SLICES;
	    slice = &clst->slices[pos];
	}
	return slice;
    }
    if(sliceNo != clst->currentSliceNo + 1) {
	//TODO: FIXME
	//udpc_flprintf("Slice %d skipped\n", sliceNo-1);
	//exit(1);
    }

    if((clst->net_config->flags & FLAG_STREAMING) &&
       sliceNo != clst->currentSliceNo) {
	return initSlice(clst, clst->currentSlice, sliceNo);	    
    }

    if(sliceNo > clst->receivedSliceNo + 2) {

	//TODO: FIXME
#if 0
	slice_t slice = rcvFindSlice(clst, clst->receivedSliceNo+1);
	udpc_flprintf("Dropped by server now=%d last=%d\n", sliceNo, clst->receivedSliceNo);
	if(slice != NULL)
	    printMissedBlockMap(clst, slice);
	exit(1);
#endif
    }
    return newSlice(clst, sliceNo);
}

void setSliceBytes(struct slice *slice, 
			  struct clientState *clst,
			  int bytes) {
    idbassert(slice->magic == SLICEMAGIC);
    if(slice->bytesKnown) {
	if(slice->bytes != bytes) {
		//TODO: FIXME
	    //udpc_fatal(1, "Byte number mismatch %d <-> %d\n", bytes, slice->bytes);
	}
    } else {
	slice->bytesKnown = 1;
	slice->bytes = bytes;
	if(bytes == 0)
	    clst->netEndReached=1;
    }
}

void advanceReceivedPointer(struct clientState *clst) {
    int pos = clst->receivedPtr;
    while(1) {	
	slice_t slice = &clst->slices[pos];
	if(
#ifdef BB_FEATURE_UDPCAST_FEC
	   slice->state != SLICE_FEC &&
	   slice->state != SLICE_FEC_DONE &&
#endif
	   slice->state != slice::SLICE_DONE)
	    break;
	pos++;
	clst->receivedSliceNo = slice->sliceNo;
	if(pos >= NR_SLICES)
	    pos -= NR_SLICES;
    }
    clst->receivedPtr = pos;
}

void receiverStatsAddBytes(receiver_stats_t rs, long bytes) {
    if(rs != NULL)
	rs->totalBytes += bytes;
}

void cleanupSlices(struct clientState *clst, unsigned int doneState)
{
    while(1) {	
	int pos = pc_getProducerPosition(clst->free_slices_pc);
	int bytes;
	slice_t slice = &clst->slices[pos];
	if(slice->state != (signed)doneState)
	    break;
	receiverStatsAddBytes(clst->stats, slice->bytes);
	bytes = slice->bytes;

	/* signal data received */
	if(bytes == 0) {
	    pc_produceEnd(clst->fifo->data);
	} else
	    pc_produce(clst->fifo->data, slice->bytes);

	/* free up slice structure */
	clst->slices[pos].state = slice::SLICE_FREE;
	pc_produce(clst->free_slices_pc, 1);
	
	/* if at end, exit this thread */
	if(!bytes) {
	    clst->endReached = 2;
	}
    }
}

void checkSliceComplete(struct clientState *clst,
			       struct slice *slice)
{
    int blocksInSlice;

    idbassert(slice->magic == SLICEMAGIC);
    if(slice->state != slice::SLICE_RECEIVING) 
	/* bad starting state */
	return; 

    /* is this slice ready ? */
    idbassert(clst->net_config->blockSize != 0);
    blocksInSlice = (slice->bytes + clst->net_config->blockSize - 1) / 
	clst->net_config->blockSize;
    if(blocksInSlice == slice->blocksTransferred) {
	pc_consumed(clst->fifo->freeMemQueue, slice->bytes);
	clst->net_config->flags &= ~FLAG_STREAMING;
	if(blocksInSlice == slice->dataBlocksTransferred)
	    slice->state = slice::SLICE_DONE;
	else {
#ifdef BB_FEATURE_UDPCAST_FEC
	    idbassert(clst->use_fec == 1);
	    slice->state = SLICE_FEC;
#else
	    idbassert(0);
#endif
	}
	advanceReceivedPointer(clst);
#ifdef BB_FEATURE_UDPCAST_FEC
	if(clst->use_fec) {
	    int n = pc_getProducerPosition(clst->fec_data_pc);
	    idbassert(slice->state == SLICE_DONE || slice->state == SLICE_FEC);
	    clst->fec_slices[n] = slice;
	    pc_produce(clst->fec_data_pc, 1);
	} else
#endif
	    cleanupSlices(clst, slice::SLICE_DONE);
    }
}

int processDataBlock(struct clientState *clst,
			    int sliceNo,
			    int blockNo,
			    int bytes)
{
//cerr << "processDataBlock(): " << sliceNo << ", " << blockNo << ", " << bytes << endl;
    struct fifo *fifo = clst->fifo;
    struct slice *slice = rcvFindSlice(clst, sliceNo);
    unsigned char *shouldAddress, *isAddress;

    idbassert(slice == NULL || slice->magic == SLICEMAGIC);

    if(slice == NULL || 
       slice->state == slice::SLICE_FREE ||
       slice->state == slice::SLICE_DONE
#ifdef BB_FEATURE_UDPCAST_FEC
       ||
       slice->state == SLICE_FEC ||
       slice->state == SLICE_FEC_DONE
#endif
       ) {
	/* an old slice. Ignore */
//cerr << "ignore" << endl;
	return 0;
    }

    if(sliceNo > clst->currentSliceNo+2)
	{
	//TODO: FIXME
	//udpc_fatal(1, "We have been dropped by sender\n");
	}

    if(BIT_ISSET(blockNo, slice->retransmit.map)) {
	/* we already have this packet, ignore */
#if 0
	flprintf("Packet %d:%d not for us\n", sliceNo, blockNo);
#endif
//cerr << "dup" << endl;
	return 0;
    }

    if(slice->base % clst->net_config->blockSize) {
	//TODO: FIXME
	//udpc_fatal(1, "Bad base %d, not multiple of block size %d\n", slice->base, clst->net_config->blockSize);
//cerr << "bad base" << endl;
    }
//cerr << "good slice" << endl;

    shouldAddress = ADR(blockNo, clst->net_config->blockSize);
    isAddress = (unsigned char*)clst->data_hdr.msg_iov[1].iov_base;
    if(shouldAddress != isAddress) {
	/* copy message to the correct place */
	memcpy(shouldAddress, isAddress,  clst->net_config->blockSize);
    }

    if(clst->client_config->sender_is_newgen && bytes != 0)
	setSliceBytes(slice, clst, bytes);
    if(clst->client_config->sender_is_newgen && bytes == 0)
	clst->netEndReached = 0;

    SET_BIT(blockNo, slice->retransmit.map);
#ifdef BB_FEATURE_UDPCAST_FEC
    if(slice->fec_stripes) {
	int stripe = blockNo % slice->fec_stripes;
	slice->missing_data_blocks[stripe]--;
	idbassert(slice->missing_data_blocks[stripe] >= 0);
	if(slice->missing_data_blocks[stripe] <
	   slice->fec_blocks[stripe]) {
	    int blockIdx;
	    /* FIXME: FEC block should be enqueued in local queue here...*/
	    slice->fec_blocks[stripe]--;
	    blockIdx = stripe+slice->fec_blocks[stripe]*slice->fec_stripes;
	    idbassert(slice->fec_descs[blockIdx].adr != 0);
	    clst->localBlockAddresses[clst->localPos++] = 
		slice->fec_descs[blockIdx].adr;
	    slice->fec_descs[blockIdx].adr=0;
	    slice->blocksTransferred--;
	}
    }
#endif
    slice->dataBlocksTransferred++;
    slice->blocksTransferred++;
    while(slice->freePos < MAX_SLICE_SIZE && 
	  BIT_ISSET(slice->freePos, slice->retransmit.map))
	slice->freePos++;
    checkSliceComplete(clst, slice);
    return 0;
}

int sendOk(struct client_config *client_config, unsigned int sliceNo)
{
    struct ok ok;
    ok.opCode = htons(CMD_OK);
    ok.reserved = 0;
    ok.sliceNo = htonl(sliceNo);
    return SSEND(ok);
}

int sendRetransmit(struct clientState *clst,
			  struct slice *slice,
			  int rxmit) {
    struct client_config *client_config = clst->client_config;

    idbassert(slice->magic == SLICEMAGIC);
    slice->retransmit.opCode = htons(CMD_RETRANSMIT);
    slice->retransmit.reserved = 0;
    slice->retransmit.sliceNo = htonl(slice->sliceNo);
    slice->retransmit.rxmit = htonl(rxmit);
    return SSEND(slice->retransmit);
}

int processReqAck(struct clientState *clst,
			 int sliceNo, int bytes, int rxmit)
{   
    struct slice *slice = rcvFindSlice(clst, sliceNo);
    int blocksInSlice;
    char *readySet = (char *) clst->data_hdr.msg_iov[1].iov_base;

    idbassert(slice == NULL || slice->magic == SLICEMAGIC);

    {
	struct timeval tv;
	gettimeofday(&tv, 0);
	/* usleep(1); DEBUG: FIXME */
    }
    if(BIT_ISSET(clst->client_config->clientNumber, readySet)) {
	/* not for us */
	return 0;
    }

    if(slice == NULL) {
	/* an old slice => send ok */
	return sendOk(clst->client_config, sliceNo);
    }

    setSliceBytes(slice, clst, bytes);
    idbassert(clst->net_config->blockSize != 0);
    blocksInSlice = (slice->bytes + clst->net_config->blockSize - 1) / 
	clst->net_config->blockSize;
    if (blocksInSlice == slice->blocksTransferred) {
	/* send ok */
	sendOk(clst->client_config, slice->sliceNo);
    } else {
	sendRetransmit(clst, slice, rxmit);
    }
    checkSliceComplete(clst, slice); /* needed for the final 0 sized slice */
    advanceReceivedPointer(clst);
#ifdef BB_FEATURE_UDPCAST_FEC
    if(!clst->use_fec)
	cleanupSlices(clst, SLICE_DONE);
#endif
    return 0;
}

int udpc_isAddressEqual(struct sockaddr_in *a, struct sockaddr_in *b) {
    return !memcmp((char *) a, (char *)b, 8);
}
int dispatchMessage(struct clientState *clst)
{
    int ret;
    struct sockaddr_in lserver;
    struct fifo *fifo = clst->fifo;
    int fd = -1;
    struct client_config *client_config = clst->client_config;

    /* set up message header */
    if (clst->currentSlice != NULL &&
	clst->currentSlice->freePos < MAX_SLICE_SIZE) {
	struct slice *slice = clst->currentSlice;
	idbassert(slice == NULL || slice->magic == SLICEMAGIC);
	clst->data_iov[1].iov_base = 
	    ADR(slice->freePos, clst->net_config->blockSize);
    } else {
	clst->data_iov[1].iov_base = clst->nextBlock;
    }

    clst->data_iov[1].iov_len = clst->net_config->blockSize;
    clst->data_hdr.msg_iovlen = 2;

    clst->data_hdr.msg_name = &lserver;
    clst->data_hdr.msg_namelen = sizeof(struct sockaddr_in);

    while(clst->endReached || clst->netEndReached) {
	int oldEndReached = clst->endReached;
	int nr_desc;
	struct timeval tv;
	fd_set read_set;

	int maxFd = prepareForSelect(client_config->socks,
				     NR_CLIENT_SOCKS, &read_set);

	tv.tv_sec = clst->net_config->exitWait / 1000;
	tv.tv_usec = (clst->net_config->exitWait % 1000)*1000;
	nr_desc = select(maxFd,  &read_set, 0, 0,  &tv);
	if(nr_desc < 0) {
	    break;
	}
	fd = udpc_getSelectedSock(client_config->socks, NR_CLIENT_SOCKS, &read_set);
	if(fd >= 0)
	  break;

	/* Timeout expired */
	if(oldEndReached >= 2) {
	    clst->endReached = 3;
	    return 0;
	}
    }

    if(fd < 0)
	fd = clst->selectedFd;

    if(fd < 0) {
	struct timeval tv, *tvp;
	fd_set read_set;
	int maxFd = prepareForSelect(client_config->socks,
				     NR_CLIENT_SOCKS, &read_set);
	clst->promptPrinted=1;

	if(clst->net_config->startTimeout == 0) {
	    tvp=NULL;
	} else {
	    tv.tv_sec = clst->net_config->startTimeout;
	    tv.tv_usec = 0;
	    tvp = &tv;
	}

//cerr << "waiting for data..." << endl;
	ret = selectWithoutConsole(maxFd+1, &read_set, tvp);
	if(ret < 0) {
	  perror("Select");
	  return 0;
	}
	if(ret == 0) {
		clst->endReached=3;
		clst->netEndReached=3;
		pc_produceEnd(clst->fifo->data);
		return 1;
	}
	fd = udpc_getSelectedSock(clst->client_config->socks,
			     NR_CLIENT_SOCKS, &read_set);
    }

#ifdef LOSSTEST
    loseRecvPacket(fd);
    ret=RecvMsg(fd, &clst->data_hdr, 0);
#else
    ret=recvmsg(fd, &clst->data_hdr, 0);
//cerr << "got some data on fd " << fd << endl;
#endif
    if (ret < 0) {
#if DEBUG
	flprintf("data recvfrom %d: %s\n", fd, strerror(errno));
#endif
	return -1;
    }

#if 0
    fprintf(stderr, "received packet for slice %d, block %d\n", 
	     ntohl(Msg.sliceNo), ntohs(db.blockNo));
#endif

    if(!udpc_isAddressEqual(&lserver, 
			    &clst->client_config->serverAddr)) {
	return -1;
    }

    switch(ntohs(clst->Msg.opCode)) {
	case CMD_DATA:
//cerr << "got CMD_DATA" << endl;
	    closeAllExcept(clst, fd);
	    //udpc_receiverStatsStartTimer(clst->stats);
	    clst->client_config->isStarted = 1;
	    return processDataBlock(clst,
				    ntohl(clst->Msg.dataBlock.sliceNo),
				    ntohs(clst->Msg.dataBlock.blockNo),
				    ntohl(clst->Msg.dataBlock.bytes));
#ifdef BB_FEATURE_UDPCAST_FEC
	case CMD_FEC:
//cerr << "got CMD_FEC" << endl;
	    closeAllExcept(clst, fd);
	    //receiverStatsStartTimer(clst->stats);
	    clst->client_config->isStarted = 1;
	    return processFecBlock(clst,
				   ntohs(clst->Msg.fecBlock.stripes),
				   ntohl(clst->Msg.fecBlock.sliceNo),
				   ntohs(clst->Msg.fecBlock.blockNo),
				   ntohl(clst->Msg.fecBlock.bytes));
#endif
	case CMD_REQACK:
//cerr << "got CMD_REQACK" << endl;
	    closeAllExcept(clst, fd);
	    //receiverStatsStartTimer(clst->stats);
	    clst->client_config->isStarted = 1;
	    return processReqAck(clst,
				 ntohl(clst->Msg.reqack.sliceNo),
				 ntohl(clst->Msg.reqack.bytes),
				 ntohl(clst->Msg.reqack.rxmit));
	case CMD_HELLO_STREAMING:
	case CMD_HELLO_NEW:
	case CMD_HELLO:
//cerr << "got CMD_HELLO" << endl;
	    /* retransmission of hello to find other participants ==> ignore */
	    return 0;
	default:
	    break;
    }

    return -1;
}

THREAD_RETURN netReceiverMain(void *args0)
{    
    struct clientState *clst = (struct clientState *) args0;

    clst->currentSliceNo = 0;
    setupMessages(clst);
    
    clst->currentSliceNo = -1;
    clst->currentSlice = NULL;
    clst->promptPrinted = 0;
    if(! (clst->net_config->flags & FLAG_STREAMING))
	newSlice(clst, 0);
    else {
	clst->currentSlice = NULL;
	clst->currentSliceNo = 0;
    }

    while(clst->endReached < 3) {
	dispatchMessage(clst);
    }
#ifdef BB_FEATURE_UDPCAST_FEC
    if(clst->use_fec)
	pthread_join(clst->fec_thread, NULL);
#endif
    return 0;
}

int spawnNetReceiver(struct fifo *fifo,
		     struct client_config *client_config,
		     struct net_config *net_config)
{
    int i;
    struct clientState  *clst = MALLOC(struct clientState);
    clst->fifo = fifo;
    clst->client_config = client_config;
    clst->net_config = net_config;
    //clst->stats = stats;
    clst->endReached = 0;
    clst->netEndReached = 0;
    clst->selectedFd = -1;

    clst->free_slices_pc = pc_makeProduconsum(NR_SLICES, "free slices");
    pc_produce(clst->free_slices_pc, NR_SLICES);
    for(i = 0; i <NR_SLICES; i++)
	clst->slices[i].state = slice::SLICE_FREE;
    clst->receivedPtr = 0;
    clst->receivedSliceNo = 0;

#ifdef BB_FEATURE_UDPCAST_FEC
    fec_init(); /* fec new involves memory
		 * allocation. Better do it here */
    clst->use_fec = 0;
    clst->fec_data_pc = pc_makeProduconsum(NR_SLICES, "fec data");
#endif

#define NR_BLOCKS 4096
    clst->freeBlocks_pc = pc_makeProduconsum(NR_BLOCKS, "free blocks");
    pc_produce(clst->freeBlocks_pc, NR_BLOCKS);
    clst->blockAddresses = (unsigned char**)calloc(NR_BLOCKS, sizeof(char *));
    clst->localBlockAddresses = (unsigned char**)calloc(NR_BLOCKS, sizeof(char *));
    clst->blockData = (unsigned char*)malloc(NR_BLOCKS * net_config->blockSize);
    for(i = 0; i < NR_BLOCKS; i++)
	clst->blockAddresses[i] = clst->blockData + i * net_config->blockSize;
    clst->localPos=0;

    setNextBlock(clst);
    return pthread_create(&client_config->thread, NULL, netReceiverMain, clst);
}

unsigned int pc_getSize(produconsum_t pc) {
    return pc->size;
}

int writer(struct fifo *fifo, SBS outbs) {
//cerr << "start appending to outbs" << endl;
	outbs->restart();
    int fifoSize = pc_getSize(fifo->data);
    while(1) {
	int pos=pc_getConsumerPosition(fifo->data);
	int bytes = pc_consumeContiguousMinAmount(fifo->data, BLOCKSIZE);
	if (bytes == 0) {
//cerr << "done appending to outbs: " << outbs->length() << endl;
	    return 0;
	}

	/*
	 * If we have more than blocksize, round down to nearest blocksize
	 * multiple
	 */
	if(pos + bytes != fifoSize && 
	   bytes > (pos + bytes) % BLOCKSIZE)
	    bytes -= (pos + bytes) % BLOCKSIZE;

	/* make sure we don't write to big a chunk... Better to
	 * liberate small chunks one by one rather than attempt to
	 * write out a bigger chunk and block reception for too
	 * long */
	if (bytes > 128 * 1024)
	    bytes = 64 * 1024;

	//bytes = write(outFile, fifo->dataBuffer + pos, bytes);
//cerr << "appending " << bytes << " bytes to outbs..." << endl;
	outbs->append(fifo->dataBuffer + pos, bytes);
	pc_consumed(fifo->data, bytes);
	pc_produce(fifo->freeMemQueue, bytes);
    }
}
}

namespace multicast
{

MulticastImpl::MulticastImpl(int min_receivers, const string& ifName, int portBase, int bufSize) :
	fIfName(ifName),
	fDb(0)
{
	udpc_clearIp(&fNet_config.dataMcastAddr);
	fNet_config.mcastRdv = 0;
	fNet_config.blockSize = 1024; //1456;
	fNet_config.sliceSize = 16;
	fNet_config.portBase = portBase;
	fNet_config.nrGovernors = 0;
	fNet_config.flags = FLAG_NOKBD;
	fNet_config.capabilities = 0;
	fNet_config.min_slice_size = 16;
	fNet_config.max_slice_size = 1024;
	fNet_config.default_slice_size = 0;
	fNet_config.ttl = 1;
	fNet_config.rexmit_hello_interval = 2000;
	fNet_config.autostart = 0;
	fNet_config.requestedBufSize = bufSize;

	fNet_config.min_receivers = min_receivers;
	fNet_config.max_receivers_wait=0;
	fNet_config.min_receivers_wait=0;

	fNet_config.retriesUntilDrop = 200;

	fNet_config.rehelloOffset = 50;

	fStat_config.log = 0;
	fStat_config.bwPeriod = 0;
	fStat_config.printUncompressedPos = -1;
	fStat_config.statPeriod = DEFLT_STAT_PERIOD;

	fNet_config.net_if = 0;

	//full-duplex
	fNet_config.flags |= FLAG_SN;

	if (fIfName.empty())
		fIfName = "eth0";

	fSock[0] = -1;
	fSock[1] = -1;
	fSock[2] = -1;
}

MulticastImpl::~MulticastImpl()
{
	delete fDb;

	for (int i=0; i<3; i++)
	{
		if (fSock[i] >= 0)
		{
			shutdown(fSock[i], SHUT_RDWR);
			close(fSock[i]);
		}
	}
}

void MulticastImpl::startSender()
{
    int tries;
    time_t firstConnected = 0;
    time_t *firstConnectedP;

    //participantsDb_t db;

    /* make the socket and print banner */
    //int fSock[3];
    int nr=0;
    int fd;
    int r;
    int j;

    fNet_config.net_if = udpc_getNetIf(fIfName.c_str());

    fSock[nr++] = udpc_makeSocket(ADDR_TYPE_UCAST,
			    fNet_config.net_if,
			    NULL,
			    SENDER_PORT(fNet_config.portBase));
//cerr << "sock[" << (nr-1) << "] = " << fSock[(nr-1)] << endl;

    if(! (fNet_config.flags & (FLAG_SN | FLAG_NOTSN)) ) {
      if(udpc_isFullDuplex(fSock[0], fNet_config.net_if->name) == 1) {
	fNet_config.flags |= FLAG_SN;
      }
    }
    
    fd = udpc_makeSocket(ADDR_TYPE_BCAST,
		    fNet_config.net_if,
		    NULL,
		    SENDER_PORT(fNet_config.portBase));
    if(fd >= 0)
	fSock[nr++] = fd;
//cerr << "sock[" << (nr-1) << "] = " << fSock[(nr-1)] << endl;

    if(fNet_config.requestedBufSize)
	udpc_setSendBuf(fSock[0], fNet_config.requestedBufSize);

    fNet_config.controlMcastAddr.sin_addr.s_addr =0;
    if(fNet_config.ttl == 1 && fNet_config.mcastRdv == NULL) {
	udpc_getBroadCastAddress(fNet_config.net_if,
			    &fNet_config.controlMcastAddr,
			    RECEIVER_PORT(fNet_config.portBase));
	udpc_setSocketToBroadcast(fSock[0]);
    } 

    if(fNet_config.controlMcastAddr.sin_addr.s_addr == 0) {
	udpc_getMcastAllAddress(&fNet_config.controlMcastAddr,
			   fNet_config.mcastRdv,
			   RECEIVER_PORT(fNet_config.portBase));
	/* Only do the following if controlMcastAddr is indeed an
	   mcast address ... */
	if(isMcastAddress(&fNet_config.controlMcastAddr)) {
	    udpc_setMcastDestination(fSock[0], fNet_config.net_if,
				&fNet_config.controlMcastAddr);
	    udpc_setTtl(fSock[0], fNet_config.ttl);
	    fSock[nr++] = udpc_makeSocket(ADDR_TYPE_MCAST,
				    fNet_config.net_if,
				    &fNet_config.controlMcastAddr,
				    SENDER_PORT(fNet_config.portBase));
//cerr << "sock[" << (nr-1) << "] = " << fSock[(nr-1)] << endl;
	}
    }

    if(!(fNet_config.flags & FLAG_POINTOPOINT) &&
       udpc_ipIsZero(&fNet_config.dataMcastAddr)) {
	udpc_getDefaultMcastAddress(fNet_config.net_if, 
			       &fNet_config.dataMcastAddr);
    }

    if(fNet_config.flags & FLAG_POINTOPOINT) {
	udpc_clearIp(&fNet_config.dataMcastAddr);
    }

    setPort(&fNet_config.dataMcastAddr, RECEIVER_PORT(fNet_config.portBase));

    fNet_config.capabilities = SENDER_CAPABILITIES;
    if(fNet_config.flags & FLAG_ASYNC)
	fNet_config.capabilities |= CAP_ASYNC;

    udpc_sendHello(&fNet_config, fSock[0], 0);

    fDb = udpc_makeParticipantsDb();
    tries = 0;

    if(fNet_config.min_receivers || fNet_config.min_receivers_wait ||
       fNet_config.max_receivers_wait)
	firstConnectedP = &firstConnected;
    else
	firstConnectedP = NULL;	

    while (!(r = mainDispatcher(fSock, nr, fDb, &fNet_config, &tries, firstConnectedP)))
        ;

    for(j=1; j<nr; j++)
      if(fSock[j] != fSock[0])
	closesocket(fSock[j]);

    if(r == 1) {
	int i;
	for(i=1; i<nr; i++)
	    udpc_closeSock(fSock, nr, i);
    }
	//doTransfer(fSock[0], fDb, &fNet_config, &fStat_config);
}

void MulticastImpl::doTransfer(const uint8_t* buf, uint32_t len)
{
    int i;
    int ret;
    struct fifo fifo;
    int isPtP = isPointToPoint(fDb, fNet_config.flags);

    fNet_config.rcvbuf=0;

    for(i=0; i<MAX_CLIENTS; i++)
	if(udpc_isParticipantValid(fDb, i)) {
	    unsigned int pRcvBuf = udpc_getParticipantRcvBuf(fDb, i);
	    if(isPtP)
		udpc_copyIpFrom(&fNet_config.dataMcastAddr, 
			   udpc_getParticipantIp(fDb,i));
	    fNet_config.capabilities &= 
		udpc_getParticipantCapabilities(fDb, i);
	    if(pRcvBuf != 0 && 
	       (fNet_config.rcvbuf == 0 || fNet_config.rcvbuf > pRcvBuf))
		fNet_config.rcvbuf = pRcvBuf;
	}

    if(isMcastAddress(&fNet_config.dataMcastAddr))
	udpc_setMcastDestination(fSock[0], fNet_config.net_if, 
			    &fNet_config.dataMcastAddr);

    if(! (fNet_config.capabilities & CAP_BIG_ENDIAN))
	{
		//TODO: FIXME
	//udpc_fatal(1, "Peer with incompatible endianness");
	}

    if(! (fNet_config.capabilities & CAP_NEW_GEN)) {
       fNet_config.dataMcastAddr = fNet_config.controlMcastAddr;
       fNet_config.flags &= ~(FLAG_SN | FLAG_ASYNC);
    }

    if(fNet_config.flags & FLAG_BCAST)
       fNet_config.dataMcastAddr = fNet_config.controlMcastAddr;

    udpc_initFifo(&fifo, fNet_config.blockSize);
    ret = spawnNetSender(&fifo, fSock[0], &fNet_config, fDb);
    localReader(&fifo, buf, len);

    pthread_join(fifo.thread, NULL);    

}

void MulticastImpl::startReceiver()
{
    union serverControlMsg Msg;
    int connectReqSent=0;
    struct sockaddr_in myIp;
    int haveServerAddress;

    fClient_config.sender_is_newgen = 0;

    fNet_config.net_if = udpc_getNetIf(fIfName.c_str());
{
fprintf(stderr, "net_if:\n\taddr = 0x%x\n\tbcast = 0x%x\n\tname = %s\n\tindex = %d\n", fNet_config.net_if->addr.s_addr, fNet_config.net_if->bcast.s_addr, fNet_config.net_if->name, fNet_config.net_if->index);
}

    udpc_zeroSockArray(fClient_config.socks, NR_CLIENT_SOCKS);

    fClient_config.S_UCAST = udpc_makeSocket(ADDR_TYPE_UCAST,
				       fNet_config.net_if,
				       0, RECEIVER_PORT(fNet_config.portBase));
//cerr << "S_UCAST = " << fClient_config.S_UCAST << endl;
    fClient_config.S_BCAST = udpc_makeSocket(ADDR_TYPE_BCAST,
				       fNet_config.net_if,
				       0, RECEIVER_PORT(fNet_config.portBase));
//cerr << "S_BCAST = " << fClient_config.S_BCAST << endl;

    if(fNet_config.ttl == 1 && fNet_config.mcastRdv == NULL) {
	udpc_getBroadCastAddress(fNet_config.net_if,
			    &fNet_config.controlMcastAddr,
			    SENDER_PORT(fNet_config.portBase));
	udpc_setSocketToBroadcast(fClient_config.S_UCAST);
    } else {
	udpc_getMcastAllAddress(&fNet_config.controlMcastAddr,
			   fNet_config.mcastRdv,
			   SENDER_PORT(fNet_config.portBase));
	if(isMcastAddress(&fNet_config.controlMcastAddr)) {
	    udpc_setMcastDestination(fClient_config.S_UCAST, fNet_config.net_if,
				&fNet_config.controlMcastAddr);
	    udpc_setTtl(fClient_config.S_UCAST, fNet_config.ttl);
	    
	    fClient_config.S_MCAST_CTRL =
		udpc_makeSocket(ADDR_TYPE_MCAST,
			   fNet_config.net_if,
			   &fNet_config.controlMcastAddr,
			   RECEIVER_PORT(fNet_config.portBase));
//cerr << "S_MCAST_CTRL = " << fClient_config.S_MCAST_CTRL << endl;
	    // TODO: subscribe address as receiver to!
	}
    }
    udpc_clearIp(&fNet_config.dataMcastAddr);

    connectReqSent = 0;
    haveServerAddress = 0;

    fClient_config.clientNumber= 0; /*default number for asynchronous transfer*/
    while(1) {
	// int len;
	int msglen;
	int sock;

	if (!connectReqSent) {
	    if (sendConnectReq(&fClient_config, &fNet_config,
			       haveServerAddress) < 0) {
		//TODO: FIXME
		//perror("sendto to locate server");
	    }
	    connectReqSent = 1;
	}

	haveServerAddress=0;

//cerr << "waiting for msg..." << flush << endl;
	sock = udpc_selectSock(fClient_config.socks, NR_CLIENT_SOCKS,
			       fNet_config.startTimeout);
//cerr << "got something" << endl;
	if(sock < 0) {
		//TODO: FIXME
	}

	// len = sizeof(server);
	msglen=RECV(sock, 
		    Msg, fClient_config.serverAddr, fNet_config.portBase);
	if (msglen < 0) {
		//TODO: FIXME
	    //perror("recvfrom to locate server");
	    //exit(1);
	}
	
	if(udpc_getPort(&fClient_config.serverAddr) != 
	   SENDER_PORT(fNet_config.portBase))
	    /* not from the right port */
	    continue;

	switch(ntohs(Msg.opCode)) {
	    case CMD_CONNECT_REPLY:
//cerr << "got conrep" << endl;
		fClient_config.clientNumber = ntohl(Msg.connectReply.clNr);
		fNet_config.blockSize = ntohl(Msg.connectReply.blockSize);

		if(ntohl(Msg.connectReply.capabilities) & CAP_NEW_GEN) {
		    fClient_config.sender_is_newgen = 1;
		    udpc_copyFromMessage(&fNet_config.dataMcastAddr,
				    Msg.connectReply.mcastAddr);
		}
		if (fClient_config.clientNumber == -1) {
		//TODO: FIXME
		    //udpc_fatal(1, "Too many clients already connected\n");
		}
		goto break_loop;

	    case CMD_HELLO_STREAMING:
	    case CMD_HELLO_NEW:
	    case CMD_HELLO:
//cerr << "got hello" << endl;
		connectReqSent = 0;
		if(ntohs(Msg.opCode) == CMD_HELLO_STREAMING)
			fNet_config.flags |= FLAG_STREAMING;
		if(ntohl(Msg.hello.capabilities) & CAP_NEW_GEN) {
		    fClient_config.sender_is_newgen = 1;
		    udpc_copyFromMessage(&fNet_config.dataMcastAddr,
				    Msg.hello.mcastAddr);
		    fNet_config.blockSize = ntohs(Msg.hello.blockSize);
		    if(ntohl(Msg.hello.capabilities) & CAP_ASYNC)
			fNet_config.flags |= FLAG_PASSIVE;
		    if(fNet_config.flags & FLAG_PASSIVE)
			goto break_loop;
		}
		haveServerAddress=1;
		continue;
	    case CMD_CONNECT_REQ:
	    case CMD_DATA:
	    case CMD_FEC:
		continue;
	    default:
		break;
	}

		//TODO: FIXME
	//udpc_fatal(1, "Bad server reply %04x. Other transfer in progress?\n", (unsigned short) ntohs(Msg.opCode));
    }

 break_loop:

    udpc_getMyAddress(fNet_config.net_if, &myIp);

    if(!udpc_ipIsZero(&fNet_config.dataMcastAddr)  &&
       !udpc_ipIsEqual(&fNet_config.dataMcastAddr, &myIp) &&
       (udpc_ipIsZero(&fNet_config.controlMcastAddr) ||
       !udpc_ipIsEqual(&fNet_config.dataMcastAddr, &fNet_config.controlMcastAddr)
	)) {
	fClient_config.S_MCAST_DATA = 
	  udpc_makeSocket(ADDR_TYPE_MCAST, fNet_config.net_if, 
		     &fNet_config.dataMcastAddr, 
		     RECEIVER_PORT(fNet_config.portBase));
//cerr << "S_MCAST_DATA = " << fClient_config.S_MCAST_DATA << endl;
    }


    if(fNet_config.requestedBufSize) {
      int i;
      for(i=0; i<NR_CLIENT_SOCKS; i++)
	if(fClient_config.socks[i] != -1)
	  udpc_setRcvBuf(fClient_config.socks[i],fNet_config.requestedBufSize);
    }

}

void MulticastImpl::receive(SBS outbs)
{
	struct fifo fifo;

	udpc_initFifo(&fifo, fNet_config.blockSize);
	fifo.data = pc_makeProduconsum(fifo.dataBufSize, "receive");
	fClient_config.isStarted = 0;

	spawnNetReceiver(&fifo, &fClient_config, &fNet_config);
	writer(&fifo, outbs);

	pthread_join(fClient_config.thread, NULL);
}

}

