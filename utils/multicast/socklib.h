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

#ifndef SOCKLIB_H
#define SOCKLIB_H

#ifndef UDPCAST_CONFIG_H
# define UDPCAST_CONFIG_H
# include "config.h"
#endif

#include <sys/types.h>
#include <string.h>

#ifdef HAVE_SYS_SOCKET_H
#include <sys/socket.h>
#endif

#ifdef HAVE_SYS_SELECT_H
#include <sys/select.h>
#endif

#ifdef HAVE_SYS_SOCKIO_H
#include <sys/sockio.h>
#endif

#ifdef HAVE_NETINET_IN_H
#include <netinet/in.h>
#endif

#ifdef HAVE_NETDB_H
#include <netdb.h>
#endif

#ifdef HAVE_WINSOCK2_H
#include <winsock2.h>
#endif

#ifdef HAVE_SYS_UIO_H
#include <sys/uio.h>
#endif

#ifdef __MINGW32__
#define WINDOWS
#undef USE_SYSLOG
#endif /* __MINGW32__ */

#ifdef __CYGWIN__
/* Untested so far ... */
#define WINDOWS
#endif

#define RECEIVER_PORT(x) (x)
#define SENDER_PORT(x) ((x)+1)

#define loseSendPacket udpc_loseSendPacket
#define loseRecvPacket udpc_loseRecvPacket
#define setWriteLoss udpc_setWriteLoss
#define setReadLoss udpc_setReadLoss
#define setReadSwap udpc_setReadSwap
#define srandomTime udpc_srandomTime
#define RecvMsg udpc_RecvMsg
#define doAutoRateLimit udpc_doAutoRateLimit
#define makeSockAddr udpc_makeSockAddr
#define printMyIp udpc_printMyIp
#define getSendBuf udpc_getSendBuf
#define setIpFromString udpc_setIpFromString
#define parseSize udpc_parseSize

#ifdef LOSSTEST
int loseSendPacket(void);
void loseRecvPacket(int s);
void setWriteLoss(char *l);
void setReadLoss(char *l);
void setReadSwap(char *l);
void srandomTime(int printSeed);
int RecvMsg(int s, struct msghdr *msg, int flags);
#endif

struct net_if {
    struct in_addr addr;
    struct in_addr bcast;
    const char *name;
#ifdef SIOCGIFINDEX
    int index;
#endif
};
typedef struct net_if net_if_t;

typedef enum addr_type_t {
  ADDR_TYPE_UCAST,
  ADDR_TYPE_MCAST,
  ADDR_TYPE_BCAST
} addr_type_t;

void doAutoRateLimit(int sock, int dir, int qsize, int size);

int makeSockAddr(char *hostname, short port, struct sockaddr_in *addr);

void printMyIp(net_if_t *net_if);

int getSendBuf(int sock);

#define SEND(s, msg, to) \
	doSend(s, &msg, sizeof(msg), &to)

#define RECV(s, msg, from, portBase ) \
	doReceive((s), &msg, sizeof(msg), &from, (portBase) )

#define BCAST_CONTROL(s, msg) \
	doSend(s, &msg, sizeof(msg), &net_config->controlMcastAddr)

void setIpFromString(struct sockaddr_in *addr, char *ip);

unsigned long parseSize(char *sizeString);

int udpc_socklibFatal(int code);

#ifdef __MINGW32__ /* __MINGW32__ */

struct iovec {
    void *iov_base;
    int iov_len;
};
struct msghdr {
    void *msg_name;
    int msg_namelen;
    struct iovec *msg_iov;
    int msg_iovlen;

};

ssize_t sendmsg(int s, const struct msghdr *msg, int flags);
ssize_t recvmsg (int fd, struct msghdr *msg, int flags);

#define usleep(x) Sleep((x)/1000)
#define sleep(x) Sleep(1000L*(x))
#endif /* __MINGW32__ */

static inline void initMsgHdr(struct msghdr *hdr) {
#ifndef WINDOWS
    hdr->msg_control = 0;
    hdr->msg_controllen = 0;
    hdr->msg_flags = 0;
#endif
}

#ifndef __MINGW32__
#undef closesocket
#define closesocket(x) close(x)
#endif

#ifndef HAVE_IN_ADDR_T
typedef unsigned long in_addr_t;
#endif

#endif
