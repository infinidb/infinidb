/* vim:ts=8:sts=8:sw=4:noai:noexpandtab
 *
 * global session ID helper functions.
 *
 * MD5 original source GNU C Library:
 * Includes functions to compute MD5 message digest of files or memory blocks
 * according to the definition of MD5 in RFC 1321 from April 1992.
 *
 * Copyright (C) 1995, 1996, 2001, 2003 Free Software Foundation, Inc.
 *
 * This file is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This file is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this file; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */

#include <errno.h>
#include <netdb.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <netinet/in.h>

#include <glib.h>

#include "pgm/gsi.h"

//#define GSI_DEBUG

#ifndef GSI_DEBUG
#define g_trace(...)		while (0)
#else
#define g_trace(...)		g_debug(__VA_ARGS__)
#endif


struct md5_ctx
{
	guint32 A;
	guint32 B;
	guint32 C;
	guint32 D;

	guint32 total[2];
	guint32 buflen;
	char buffer[128] __attribute__ ((__aligned__ (__alignof__ (guint32))));
};

/* globals */

/* This array contains the bytes used to pad the buffer to the next
 * 64-byte boundary.  (RFC 1321, 3.1: Step 1)  */
static const unsigned char fillbuf[64] = { 0x80, 0 /* , 0, 0, ...  */ };


#if __BYTE_ORDER == __BIG_ENDIAN
# define SWAP(n) \
	(((n) << 24) | (((n) & 0xff00) << 8) | (((n) >> 8) & 0xff00) | ((n) >> 24))
#else
#	define SWAP(n) (n)
#endif


/* Initialize structure containing state of computation.
   (RFC 1321, 3.3: Step 3)  */

static void
md5_init_ctx (
	struct md5_ctx*	ctx
	)
{
	ctx->A = 0x67452301;
	ctx->B = 0xefcdab89;
	ctx->C = 0x98badcfe;
	ctx->D = 0x10325476;

	ctx->total[0] = ctx->total[1] = 0;
	ctx->buflen = 0;
}

/* These are the four functions used in the four steps of the MD5 algorithm
   and defined in the RFC 1321.  The first function is a little bit optimized
   (as found in Colin Plumbs public domain implementation).  */
#define FF(b, c, d) (d ^ (b & (c ^ d)))
#define FG(b, c, d) FF (d, b, c)
#define FH(b, c, d) (b ^ c ^ d)
#define FI(b, c, d) (c ^ (b | ~d))

/* Process LEN bytes of BUFFER, accumulating context into CTX.
   It is assumed that LEN % 64 == 0.  */

static void
md5_process_block (
	const void*	buffer,
	size_t		len,
	struct md5_ctx*	ctx
	)
{
	guint32 correct_words[16];
	const guint32 *words = buffer;
	size_t nwords = len / sizeof (guint32);
	const guint32 *endp = words + nwords;
	guint32 A = ctx->A;
	guint32 B = ctx->B;
	guint32 C = ctx->C;
	guint32 D = ctx->D;

/* First increment the byte count.  RFC 1321 specifies the possible
   length of the file up to 2^64 bits.  Here we only compute the
   number of bytes.  Do a double word increment.  */
	ctx->total[0] += len;
	if (ctx->total[0] < len)
		++ctx->total[1];

/* Process all bytes in the buffer with 64 bytes in each round of
   the loop.  */
	while (words < endp)
	{
		guint32 *cwp = correct_words;
		guint32 A_save = A;
		guint32 B_save = B;
		guint32 C_save = C;
		guint32 D_save = D;

/* First round: using the given function, the context and a constant
   the next context is computed.  Because the algorithms processing
   unit is a 32-bit word and it is determined to work on words in
   little endian byte order we perhaps have to change the byte order
   before the computation.  To reduce the work for the next steps
   we store the swapped words in the array CORRECT_WORDS.  */

#define OP(a, b, c, d, s, T) \
	do \
	{ \
		a += FF (b, c, d) + (*cwp++ = SWAP (*words)) + T; \
		++words; \
		CYCLIC (a, s); \
		a += b; \
	} \
	while (0)

/* It is unfortunate that C does not provide an operator for
 * cyclic rotation.  Hope the C compiler is smart enough. */
#define CYCLIC(w, s) (w = (w << s) | (w >> (32 - s)))

/* Before we start, one word to the strange constants.
   They are defined in RFC 1321 as

   T[i] = (int) (4294967296.0 * fabs (sin (i))), i=1..64, or
   perl -e 'foreach(1..64){printf "0x%08x\n", int (4294967296 * abs (sin $_))}'
 */

/* Round 1.  */
		OP (A, B, C, D,  7, 0xd76aa478);
		OP (D, A, B, C, 12, 0xe8c7b756);
		OP (C, D, A, B, 17, 0x242070db);
		OP (B, C, D, A, 22, 0xc1bdceee);
		OP (A, B, C, D,  7, 0xf57c0faf);
		OP (D, A, B, C, 12, 0x4787c62a);
		OP (C, D, A, B, 17, 0xa8304613);
		OP (B, C, D, A, 22, 0xfd469501);
		OP (A, B, C, D,  7, 0x698098d8);
		OP (D, A, B, C, 12, 0x8b44f7af);
		OP (C, D, A, B, 17, 0xffff5bb1);
		OP (B, C, D, A, 22, 0x895cd7be);
		OP (A, B, C, D,  7, 0x6b901122);
		OP (D, A, B, C, 12, 0xfd987193);
		OP (C, D, A, B, 17, 0xa679438e);
		OP (B, C, D, A, 22, 0x49b40821);

/* For the second to fourth round we have the possibly swapped words
   in CORRECT_WORDS.  Redefine the macro to take an additional first
   argument specifying the function to use.  */
#undef OP
#define OP(f, a, b, c, d, k, s, T) \
	do \
	{ \
		a += f (b, c, d) + correct_words[k] + T; \
		CYCLIC (a, s); \
		a += b; \
	} \
	while (0)

/* Round 2.  */
		OP (FG, A, B, C, D,  1,  5, 0xf61e2562);
		OP (FG, D, A, B, C,  6,  9, 0xc040b340);
		OP (FG, C, D, A, B, 11, 14, 0x265e5a51);
		OP (FG, B, C, D, A,  0, 20, 0xe9b6c7aa);
		OP (FG, A, B, C, D,  5,  5, 0xd62f105d);
		OP (FG, D, A, B, C, 10,  9, 0x02441453);
		OP (FG, C, D, A, B, 15, 14, 0xd8a1e681);
		OP (FG, B, C, D, A,  4, 20, 0xe7d3fbc8);
		OP (FG, A, B, C, D,  9,  5, 0x21e1cde6);
		OP (FG, D, A, B, C, 14,  9, 0xc33707d6);
		OP (FG, C, D, A, B,  3, 14, 0xf4d50d87);
		OP (FG, B, C, D, A,  8, 20, 0x455a14ed);
		OP (FG, A, B, C, D, 13,  5, 0xa9e3e905);
		OP (FG, D, A, B, C,  2,  9, 0xfcefa3f8);
		OP (FG, C, D, A, B,  7, 14, 0x676f02d9);
		OP (FG, B, C, D, A, 12, 20, 0x8d2a4c8a);

/* Round 3.  */
		OP (FH, A, B, C, D,  5,  4, 0xfffa3942);
		OP (FH, D, A, B, C,  8, 11, 0x8771f681);
		OP (FH, C, D, A, B, 11, 16, 0x6d9d6122);
		OP (FH, B, C, D, A, 14, 23, 0xfde5380c);
		OP (FH, A, B, C, D,  1,  4, 0xa4beea44);
		OP (FH, D, A, B, C,  4, 11, 0x4bdecfa9);
		OP (FH, C, D, A, B,  7, 16, 0xf6bb4b60);
		OP (FH, B, C, D, A, 10, 23, 0xbebfbc70);
		OP (FH, A, B, C, D, 13,  4, 0x289b7ec6);
		OP (FH, D, A, B, C,  0, 11, 0xeaa127fa);
		OP (FH, C, D, A, B,  3, 16, 0xd4ef3085);
		OP (FH, B, C, D, A,  6, 23, 0x04881d05);
		OP (FH, A, B, C, D,  9,  4, 0xd9d4d039);
		OP (FH, D, A, B, C, 12, 11, 0xe6db99e5);
		OP (FH, C, D, A, B, 15, 16, 0x1fa27cf8);
		OP (FH, B, C, D, A,  2, 23, 0xc4ac5665);

/* Round 4.  */
		OP (FI, A, B, C, D,  0,  6, 0xf4292244);
		OP (FI, D, A, B, C,  7, 10, 0x432aff97);
		OP (FI, C, D, A, B, 14, 15, 0xab9423a7);
		OP (FI, B, C, D, A,  5, 21, 0xfc93a039);
		OP (FI, A, B, C, D, 12,  6, 0x655b59c3);
		OP (FI, D, A, B, C,  3, 10, 0x8f0ccc92);
		OP (FI, C, D, A, B, 10, 15, 0xffeff47d);
		OP (FI, B, C, D, A,  1, 21, 0x85845dd1);
		OP (FI, A, B, C, D,  8,  6, 0x6fa87e4f);
		OP (FI, D, A, B, C, 15, 10, 0xfe2ce6e0);
		OP (FI, C, D, A, B,  6, 15, 0xa3014314);
		OP (FI, B, C, D, A, 13, 21, 0x4e0811a1);
		OP (FI, A, B, C, D,  4,  6, 0xf7537e82);
		OP (FI, D, A, B, C, 11, 10, 0xbd3af235);
		OP (FI, C, D, A, B,  2, 15, 0x2ad7d2bb);
		OP (FI, B, C, D, A,  9, 21, 0xeb86d391);

/* Add the starting values of the context.  */
		A += A_save;
		B += B_save;
		C += C_save;
		D += D_save;
	}

/* Put checksum in context given as argument.  */
	ctx->A = A;
	ctx->B = B;
	ctx->C = C;
	ctx->D = D;
}

static void
md5_process_bytes (
	const void*	buffer,
	size_t		len,
	struct md5_ctx*	ctx
	)
{
	if (len >= 64)
	{
#if !_STRING_ARCH_unaligned
/* To check alignment gcc has an appropriate operator.  Other
   compilers don't.  */
#	if __GNUC__ >= 2
#		define UNALIGNED_P(p) (((uintptr_t) p) % __alignof__ (guint32) != 0)
#	else
#		define UNALIGNED_P(p) (((uintptr_t) p) % sizeof (guint32) != 0)
#	endif
		if (UNALIGNED_P (buffer))
			while (len > 64)
			{
				md5_process_block (memcpy (ctx->buffer, buffer, 64), 64, ctx);
				buffer = (const char *) buffer + 64;
				len -= 64;
			}
		else
#endif
		{
			md5_process_block (buffer, len & ~63, ctx);
			buffer = (const char *) buffer + (len & ~63);
			len &= 63;
		}
	}

/* Move remaining bytes in internal buffer.  */
	if (len > 0)
	{
		size_t left_over = ctx->buflen;

		memcpy (&ctx->buffer[left_over], buffer, len);
		left_over += len;
		if (left_over >= 64)
		{
			md5_process_block (ctx->buffer, 64, ctx);
			left_over -= 64;
			memcpy (ctx->buffer, &ctx->buffer[64], left_over);
		}
		ctx->buflen = left_over;
	}
}

/* Put result from CTX in first 16 bytes following RESBUF.  The result
   must be in little endian byte order.

   IMPORTANT: On some systems it is required that RESBUF is correctly
   aligned for a 32 bits value.  */

static void *
md5_read_ctx (
	const struct md5_ctx	*ctx,
	void*			resbuf
	)
{
	((guint32*)resbuf)[0] = SWAP (ctx->A);
	((guint32*)resbuf)[1] = SWAP (ctx->B);
	((guint32*)resbuf)[2] = SWAP (ctx->C);
	((guint32*)resbuf)[3] = SWAP (ctx->D);

	return resbuf;
}

/* Process the remaining bytes in the internal buffer and the usual
   prolog according to the standard and write the result to RESBUF.

   IMPORTANT: On some systems it is required that RESBUF is correctly
   aligned for a 32 bits value.  */

static void *
md5_finish_ctx (
	struct md5_ctx*	ctx,
	void*		resbuf
	)
{
/* Take yet unprocessed bytes into account.  */
	guint32 bytes = ctx->buflen;
	size_t pad;

/* Now count remaining bytes.  */
	ctx->total[0] += bytes;
	if (ctx->total[0] < bytes)
		++ctx->total[1];

	pad = bytes >= 56 ? 64 + 56 - bytes : 56 - bytes;
	memcpy (&ctx->buffer[bytes], fillbuf, pad);

	/* Put the 64-bit file length in *bits* at the end of the buffer.  */
	*(guint32*) &ctx->buffer[bytes + pad] = SWAP (ctx->total[0] << 3);
	*(guint32*) &ctx->buffer[bytes + pad + 4] = SWAP ((ctx->total[1] << 3) |
								(ctx->total[0] >> 29));

/* Process last bytes.  */
	md5_process_block (ctx->buffer, bytes + pad + 8, ctx);

	return md5_read_ctx (ctx, resbuf);
}

/* create a global session ID as recommended by the PGM draft specification using
 * low order 48 bits of md5 of the hostname.
 *
 * could easily be swapped for an OpenSSL accelerated varient.
 */

int
pgm_create_md5_gsi (
	pgm_gsi_t*	gsi
	)
{
	g_return_val_if_fail (gsi != NULL, -EINVAL);

	int retval = 0;
	struct md5_ctx ctx;
	char hostname[NI_MAXHOST];
	char resblock[16];

	if ((retval = gethostname (hostname, sizeof(hostname))) != 0) {
		goto out;
	}

	md5_init_ctx (&ctx);
	md5_process_bytes (hostname, strlen(hostname), &ctx);
	md5_finish_ctx (&ctx, resblock);

	memcpy (gsi, resblock + 10, 6);

out:
	return retval;
}

/* create a global session ID based on the IP address.
 *
 * GLib random API will need warming up before g_random_int_range returns
 * numbers that actually vary.
 */

int
pgm_create_ipv4_gsi (
	pgm_gsi_t*	gsi
	)
{
	g_return_val_if_fail (gsi != NULL, -EINVAL);
	int retval = 0;
	char hostname[NI_MAXHOST];
	struct addrinfo hints, *res = NULL;

	if ((retval = gethostname (hostname, sizeof(hostname))) != 0) {
		goto out;
	}

	memset (&hints, 0, sizeof(hints));
	hints.ai_family = AF_INET;
	hints.ai_flags = AI_ADDRCONFIG;
	if ((retval = getaddrinfo (hostname, NULL, &hints, &res)) != 0) {
		goto out;
	}
	memcpy (gsi, &((struct sockaddr_in*)(res->ai_addr))->sin_addr, sizeof(struct in_addr));
	freeaddrinfo (res);
	guint16 random = g_random_int_range (0, UINT16_MAX);
	memcpy ((guint8*)gsi + sizeof(struct in_addr), &random, sizeof(random));

out:
	return retval;
}

/* re-entrant form of pgm_print_gsi()
 */
int
pgm_print_gsi_r (
	const pgm_gsi_t*	gsi_,
	char*			buf,
	gsize			bufsize
	)
{
	g_return_val_if_fail (gsi_ != NULL, -EINVAL);
	g_return_val_if_fail (buf != NULL, -EINVAL);

	const guint8* gsi = (const guint8*)gsi_;
	snprintf(buf, bufsize, "%i.%i.%i.%i.%i.%i",
		gsi[0], gsi[1], gsi[2], gsi[3], gsi[4], gsi[5]);
	return 0;
}

/* transform GSI to ASCII string form.
 *
 * on success, returns pointer to ASCII string.  on error, returns NULL.
 */
gchar*
pgm_print_gsi (
	const pgm_gsi_t*	gsi
	)
{
	g_return_val_if_fail (gsi != NULL, NULL);

	static char buf[PGM_GSISTRLEN];
	pgm_print_gsi_r (gsi, buf, sizeof(buf));
	return buf;
}

/* compare two global session identifier GSI values and return TRUE if they are equal
 */

gint
pgm_gsi_equal (
        gconstpointer   v,
        gconstpointer   v2
        )
{
        return memcmp (v, v2, 6 * sizeof(guint8)) == 0;
}


/* eof */
