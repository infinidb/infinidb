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

#ifndef UDPC_PROTOC_H
#define UDPC_PROTOC_H

#include "udpcast.h"

#define MAX_BLOCK_SIZE 1456
#define MAX_FEC_INTERLEAVE 256

/**
 * This file describes the UDPCast protocol
 */
enum opCode {    
    /* Receiver to sender */

    CMD_OK,	     /* all is ok, no need to retransmit anything */
    CMD_RETRANSMIT,  /* receiver asks for some data to be retransmitted */
    CMD_GO,	     /* receiver tells server to start */
    CMD_CONNECT_REQ, /* receiver tries to find out server's address */
    CMD_DISCONNECT,  /* receiver wants to disconnect itself */

    CMD_UNUSED,	     /* obsolete version of CMD_HELLO, dating back to the
		      * time when we had little endianness (PC). This
		      * opcode contained a long unnoticed bug with parsing of
		      * blocksize */

    /* Sender to receiver */
    CMD_REQACK,	     /* server request acknowledgments from receiver */
    CMD_CONNECT_REPLY, /* receiver tries to find out server's address */

    CMD_DATA,        /* a block of data */
    CMD_FEC,	     /* a forward-error-correction block */

    CMD_HELLO_NEW,	  /* sender says he's up */
    CMD_HELLO_STREAMING,  /* retransmitted hello during streaming mode */
};

/* Sender says he's up. This is not in the enum with the others,
 * because of some endianness Snafu in early versions. However,since
 * 2005-12-23, new receivers now understand a CMD_HELLO_NEW which is
 * in sequence. Once enough of those are out in the field, we'll send
 * CMD_HELLO_NEW by default, and then phase out the old variant. */
/* Tried to remove this on 2009-08-30, but noticed that receiver was printing
 * "unexpected opcode" on retransmitted hello */
#define CMD_HELLO 0x0500

    struct connectReq {
	unsigned short opCode;
	short reserved;
	int capabilities;
	unsigned int rcvbuf;
    };
    struct retransmit {
	unsigned short opCode;
	short reserved;
	int sliceNo;
	int rxmit;
	unsigned char map[MAX_SLICE_SIZE / BITS_PER_CHAR];
    };
    struct ok {
	unsigned short opCode;
	short reserved;
	int sliceNo;
    } ok;

union message {
    unsigned short opCode;
    struct ok ok;

    struct retransmit retransmit;

    struct connectReq connectReq;

    struct go {
	unsigned short opCode;
	short reserved;
    } go;

    struct disconnect {
	unsigned short opCode;
	short reserved;
    } disconnect;
};



struct connectReply {
    unsigned short opCode;
    short reserved;
    int clNr;
    int blockSize;
    int capabilities;
    unsigned char mcastAddr[16]; /* provide enough place for IPV6 */
};

struct hello {
    unsigned short opCode;
    short reserved;
    int capabilities;
    unsigned char mcastAddr[16]; /* provide enough place for IPV6 */
    short blockSize;
};

union serverControlMsg {
    unsigned short opCode;
    short reserved;
    struct hello hello;
    struct connectReply connectReply;

};


struct dataBlock {
    unsigned short opCode;
    short reserved;
    int sliceNo;
    unsigned short blockNo;
    unsigned short reserved2;
    int bytes;
};

struct fecBlock {
    unsigned short opCode;
    short stripes;
    int sliceNo;
    unsigned short blockNo;
    unsigned short reserved2;
    int bytes;
};

struct reqack {
    unsigned short opCode;
    short reserved;
    int sliceNo;
    int bytes;
    int rxmit;
};

union serverDataMsg {
    unsigned short opCode;
    struct reqack reqack;
    struct dataBlock dataBlock;
    struct fecBlock fecBlock;
};

/* ============================================
 * Capabilities
 */

/* Does the receiver support the new CMD_DATA command, which carries
 * capabilities mask?
 * "new generation" receiver:
 *   - capabilities word included in hello/connectReq commands
 *   - receiver multicast capable
 *   - receiver can receive ASYNC and SN
 */
#define CAP_NEW_GEN 0x0001

/* Use multicast instead of Broadcast for data */
/*#define CAP_MULTICAST 0x0002*/

#ifdef BB_FEATURE_UDPCAST_FEC
/* Forward error correction */
#define CAP_FEC 0x0004
#endif

/* Supports big endians (a.k.a. network) */
#define CAP_BIG_ENDIAN 0x0008

/* Support little endians (a.k.a. PC) ==> obsolete! */
#define CAP_LITTLE_ENDIAN 0x0010

/* This transmission is asynchronous (no receiver reply) */
#define CAP_ASYNC 0x0020

/* Sender currently supports CAPABILITIES and MULTICAST */
#define SENDER_CAPABILITIES ( \
	CAP_NEW_GEN | \
	CAP_BIG_ENDIAN)


#define RECEIVER_CAPABILITIES ( \
	CAP_NEW_GEN | \
	CAP_BIG_ENDIAN)


#endif
