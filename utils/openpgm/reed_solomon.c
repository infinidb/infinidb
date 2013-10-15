/* vim:ts=8:sts=8:sw=4:noai:noexpandtab
 *
 * Reed-Solomon forward error correction based on Vandermonde matrices.
 *
 * Output is incompatible with BCH style Reed-Solomon encoding.
 *
 * draft-ietf-rmt-bb-fec-rs-05.txt
 * + rfc5052
 *
 * Copyright (c) 2006-2008 Miru Limited.
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
#include <string.h>

#include <glib.h>

#include "pgm/galois.h"


struct rs_t {
	guint		n, k;		/* RS(n, k) */
	gf8_t*		GM;		/* Generator Matrix */
	gf8_t*		RM;		/* Recovery Matrix */
};

typedef struct rs_t rs_t;


/* globals */

int pgm_rs_create (rs_t**, guint, guint);
int pgm_rs_destroy (rs_t*);
int pgm_rs_encode (rs_t*, const gf8_t**, guint, gf8_t*, gsize);
int pgm_rs_decode_parity_inline (rs_t*, gf8_t**, guint*, gsize);
int pgm_rs_decode_parity_appended (rs_t*, gf8_t**, guint*, gsize);


/* Vector GF(2⁸) plus-equals multiplication.
 *
 * d[] += b • s[]
 */

static void
gf_vec_addmul (
	gf8_t*			d,
	const gf8_t		b,
	const gf8_t*		s,
	guint			len		/* length of vectors */
	)
{
	unsigned i;
	unsigned count8;

	if (G_UNLIKELY(b == 0))
		return;

#ifdef CONFIG_GALOIS_MUL_LUT
        const gf8_t* gfmul_b = &gftable[ (guint16)b << 8 ];
#endif

	i = 0;
	count8 = len >> 3;		/* 8-way unrolls */
	if (count8)
	{
		while (count8--) {
#ifdef CONFIG_GALOIS_MUL_LUT
			d[i  ] ^= gfmul_b[ s[i  ] ];
			d[i+1] ^= gfmul_b[ s[i+1] ];
			d[i+2] ^= gfmul_b[ s[i+2] ];
			d[i+3] ^= gfmul_b[ s[i+3] ];
			d[i+4] ^= gfmul_b[ s[i+4] ];
			d[i+5] ^= gfmul_b[ s[i+5] ];
			d[i+6] ^= gfmul_b[ s[i+6] ];
			d[i+7] ^= gfmul_b[ s[i+7] ];
#else
			d[i  ] ^= gfmul( b, s[i  ] );
			d[i+1] ^= gfmul( b, s[i+1] );
			d[i+2] ^= gfmul( b, s[i+2] );
			d[i+3] ^= gfmul( b, s[i+3] );
			d[i+4] ^= gfmul( b, s[i+4] );
			d[i+5] ^= gfmul( b, s[i+5] );
			d[i+6] ^= gfmul( b, s[i+6] );
			d[i+7] ^= gfmul( b, s[i+7] );
#endif
			i += 8;
		}

/* remaining */
		len %= 8;
	}

	while (len--) {
#ifdef CONFIG_GALOIS_MUL_LUT
		d[i] ^= gfmul_b[ s[i] ];
#else
		d[i] ^= gfmul( b, s[i] );
#endif
		i++;
	}
}

/* Basic matrix multiplication.
 *
 * C = AB
 *         n
 * c_i,j = ∑  a_i,j × b_r,j = a_i,1 × b_1,j + a_i,2 × b_2,j + ⋯ + a_i,n × b_n,j
 *        r=1
 */

static void
matmul (
	const gf8_t*		a,	/* m-by-n */
	const gf8_t*		b,	/* n-by-p */
	gf8_t*			c,	/* ∴ m-by-p */
	const guint		m,
	const guint		n,
	const guint		p
	)
{
	for (guint j = 0; j < m; j++)
	{
		for (guint i = 0; i < p; i++)
		{
			gf8_t sum = 0;

			for (guint k = 0; k < n; k++)
			{
				sum ^= gfmul( a[ (j * n) + k ], b[ (k * p) + i ] );
			}

			c[ (j * p) + i ] = sum;
		}
	}
}

/* Generic square matrix inversion
 */

#ifdef CONFIG_XOR_SWAP
#define SWAP(a, b)	(((a) ^= (b)), ((b) ^= (a)), ((a) ^= (b)))
#else
#define SWAP(a, b)	do { gf8_t _t = (b); (b) = (a); (a) = _t; } while (0)
#endif

static void
matinv (
	gf8_t*			M,		/* is n-by-n */
	const guint		n
	)
{
	guint pivot_rows[ n ];
	guint pivot_cols[ n ];
	gboolean pivots[ n ];
	memset (pivots, 0, sizeof(pivots));

	gf8_t identity[ n ];
	memset (identity, 0, sizeof(identity));

	for (guint i = 0; i < n; i++)
	{
		guint row = 0, col = 0;

/* check diagonal for new pivot */
		if (!pivots[ i ] &&
			M[ (i * n) + i ])
		{
			row = col = i;
		}
		else
		{
			for (guint j = 0; j < n; j++)
			{
				if (pivots[ j ]) continue;

				for (guint x = 0; x < n; x++)
				{
					if (!pivots[ x ] &&
						M[ (j * n) + x ])
					{
						row = j;
						col = x;
						goto found;
					}
				}
			}
		}

found:
		pivots[ col ] = TRUE;

/* pivot */
		if (row != col)
		{
			for (guint x = 0; x < n; x++)
			{
				gf8_t *pivot_row = &M[ (row * n) + x ],
				      *pivot_col = &M[ (col * n) + x ];
				SWAP( *pivot_row, *pivot_col );
			}
		}

/* save location */
		pivot_rows[ i ] = row;
		pivot_cols[ i ] = col;

/* divide row by pivot element */
		if (M[ (col * n) + col ] != 1)
		{
			gf8_t c = M[ (col * n) + col ];
			M[ (col * n) + col ] = 1;

			for (guint x = 0; x < n; x++)
			{
				M[ (col * n) + x ] = gfdiv( M[ (col * n) + x ], c );
			}
		}

/* reduce if not an identity row */
		identity[ col ] = 1;
		if (memcmp (&M[ (col * n) ], identity, n * sizeof(gf8_t)))
		{
			for (	guint y = 0, x = 0;
				x < n;
				x++, y++ )
			{
				if (x == col) continue;

				gf8_t c = M[ (y * n) + col ];
				M[ (y * n) + col ] = 0;

				gf_vec_addmul (&M[ y * n ], c, &M[ col * n ], n);
			}
		}
		identity[ col ] = 0;
	}

/* revert all pivots */
	for (int i = ((int)n - 1); i >= 0; i--)
	{
		if (pivot_rows[ i ] != pivot_cols[ i ])
		{
			for (guint j = 0; j < n; j++)
			{
				gf8_t *pivot_row = &M[ (j * n) + pivot_rows[ i ] ],
				      *pivot_col = &M[ (j * n) + pivot_cols[ i ] ];
				SWAP( *pivot_row, *pivot_col );
			}
		}
	}
}

/* Gauss–Jordan elimination optimised for Vandermonde matrices
 *
 * matrix = matrix⁻¹
 *
 * A Vandermonde matrix exhibits geometric progression in each row:
 *
 *     ⎡  1  α₁  α₁² ⋯ α₁^^(n-1) ⎤
 * V = ⎢  1  α₂  α₂² ⋯ α₂^^(n-1) ⎥
 *     ⎣  1  α₃  α₃² ⋯ α₃^^(n-1) ⎦
 *
 * First column is actually α_m⁰, second column is α_m¹.
 *
 * As only the second column is actually unique so optimise from that.
 */

static void
matinv_vandermonde (
	gf8_t*			V,		/* is n-by-n */
	const guint		n
	)
{
/* trivial cases */
	if (n == 1) return;

/* P_j(α) is polynomial of degree n - 1 defined by
 *
 *          n 
 * P_j(α) = ∏ (α - α_m)
 *         m=1
 *
 * 1: Work out coefficients.
 */

	gf8_t P[ n ];
	memset (P, 0, sizeof(P));

/* copy across second row, i.e. j = 2 */
	for (guint i = 0; i < n; i++)
	{
		P[ i ] = V[ (i * n) + 1 ];
	}

	gf8_t alpha[ n ];
	memset (alpha, 0, sizeof(alpha));

	alpha[ n - 1 ] = P[ 0 ];
	for (guint i = 1; i < n; i++)
	{
		for (guint j = (n - i); j < (n - 1); j++)
		{
			alpha[ j ] ^= gfmul( P[ i ], alpha[ j + 1 ] );
		}
		alpha[ n - 1 ] ^= P[ i ];
	}

/* 2: Obtain numberators and denominators by synthetic division.
 */

	gf8_t b[ n ];
	b[ n - 1 ] = 1;
	for (guint j = 0; j < n; j++)
	{
		gf8_t xx = P[ j ];
		gf8_t t = 1;

/* skip first iteration */
		for (int i = ((int)n - 2); i >= 0; i--)
		{
			b[ i ] = alpha[ i + 1 ] ^ gfmul( xx, b[ i + 1 ] );
			t = gfmul( xx, t ) ^ b[ i ];
		}

		for (guint i = 0; i < n; i++)
		{
			V[ (i * n) + j ] = gfdiv ( b[ i ], t );
		}
	}
}

/* create the generator matrix of a reed-solomon code.
 *
 *          s             GM            e
 *   ⎧  ⎡   s₀  ⎤   ⎡ 1 0     0 ⎤   ⎡   e₀  ⎤  ⎫
 *   ⎪  ⎢   ⋮   ⎥   ⎢ 0 1       ⎥ = ⎢   ⋮   ⎥  ⎬ n
 * k ⎨  ⎢   ⋮   ⎥ × ⎢     ⋱     ⎥   ⎣e_{n-1}⎦  ⎭
 *   ⎪  ⎢   ⋮   ⎥   ⎢       ⋱   ⎥
 *   ⎩  ⎣s_{k-1}⎦   ⎣ 0 0     1 ⎦
 *
 * e = s × GM
 */

int
pgm_rs_create (
	rs_t**			rs_,
	guint			n,
	guint			k
	)
{
	g_return_val_if_fail (rs_ != NULL, -EINVAL);

	rs_t* rs = g_malloc0 (sizeof(rs_t));

	rs->n	= n;
	rs->k	= k;
	rs->GM	= g_malloc0 (n * k * sizeof(gf8_t));
	rs->RM	= g_malloc0 (k * k * sizeof(gf8_t));

/* alpha = root of primitive polynomial of degree m
 *                 ( 1 + x² + x³ + x⁴ + x⁸ )
 *
 * V = Vandermonde matrix of k rows and n columns.
 *
 * Be careful, Harry!
 */
	gf8_t* V = g_malloc0 (n * k * sizeof(gf8_t));
	gf8_t* p = V + k;
	V[0] = 1;
	for (guint j = 0; j < (n - 1); j++)
	{
		for (guint i = 0; i < k; i++)
		{
/* the {i, j} entry of V_{k,n} is v_{i,j} = α^^(i×j),
 * where 0 <= i <= k - 1 and 0 <= j <= n - 1.
 */
			*p++ = gfantilog[ ( i * j ) % GF_MAX ];
		}
	}

/* This generator matrix would create a Maximum Distance Separable (MDS)
 * matrix, a systematic result is required, i.e. original data is left
 * unchanged.
 *
 * GM = V_{k,k}⁻¹ × V_{k,n}
 *
 * 1: matrix V_{k,k} formed by the first k columns of V_{k,n}
 */
	gf8_t* V_kk = V;
	gf8_t* V_kn = V + (k * k);

/* 2: invert it
 */
	matinv_vandermonde (V_kk, k);

/* 3: multiply by V_{k,n}
 */
	matmul (V_kn, V_kk, rs->GM + (k * k), n - k, k, k);

	g_free (V);

/* 4: set identity matrix for original data
 */
	p = rs->GM;
	for (guint i = 0; i < k; i++)
	{
		p[ (i * k) + i ] = 1;
	}

	*rs_	= rs;

	return 0;
}

int
pgm_rs_destroy (
	rs_t*		rs
	)
{
	g_return_val_if_fail (rs != NULL, -EINVAL);

	if (rs->RM) {
		g_free (rs->RM);
		rs->RM = NULL;
	}

	if (rs->GM) {
		g_free (rs->GM);
		rs->GM = NULL;
	}

	g_free (rs);

	return 0;
}

/* create a parity packet from a vector of original data packets and
 * FEC block packet offset.
 */
int
pgm_rs_encode (
	rs_t*		rs,
	const gf8_t*	src[],		/* length rs_t::k */
	guint		offset,
	gf8_t*		dst,
	gsize		len
	)
{
	g_assert (offset >= rs->k && offset < rs->n);	/* parity packet */

	memset (dst, 0, len);
	for (guint i = 0; i < rs->k; i++)
	{
		gf8_t c = rs->GM[ (offset * rs->k) + i ];
		gf_vec_addmul (dst, c, src[i], len);
	}

	return 0;
}

/* original data block of packets with missing packet entries replaced
 * with on-demand parity packets.
 */
int
pgm_rs_decode_parity_inline (
	rs_t*		rs,
	gf8_t*		block[],	/* length rs_t::k */
	guint		offsets[],	/* offsets within FEC block, 0 < offset < n */
	gsize		len		/* packet length */
	)
{
/* create new recovery matrix from generator
 */
	for (guint i = 0; i < rs->k; i++)
	{
		if (offsets[i] < rs->k) {
			memset (&rs->RM[ i * rs->k ], 0, rs->k * sizeof(gf8_t));
			rs->RM[ (i * rs->k) + i ] = 1;
			continue;
		}
		memcpy (&rs->RM[ i * rs->k ], &rs->GM[ offsets[ i ] * rs->k ], rs->k * sizeof(gf8_t));
	}

/* invert */
	matinv (rs->RM, rs->k);

	gf8_t* repairs[ rs->k ];

/* multiply out, through the length of erasures[] */
	for (guint j = 0; j < rs->k; j++)
	{
		if (offsets[ j ] < rs->k)
			continue;

		gf8_t* erasure = repairs[ j ] = g_slice_alloc0 (len);
		for (guint i = 0; i < rs->k; i++)
		{
			gf8_t* src = block[ i ];
			gf8_t c = rs->RM[ (j * rs->k) + i ];
			gf_vec_addmul (erasure, c, src, len);
		}
	}

/* move repaired over parity packets */
	for (guint j = 0; j < rs->k; j++)
	{
		if (offsets[ j ] < rs->k)
			continue;

		memcpy (block[ j ], repairs[ j ], len * sizeof(gf8_t));
		g_slice_free1 (len, repairs[ j ]);
	}

	return 0;
}

/* entire FEC block of original data and parity packets.
 *
 * erased packet buffers must be zeroed.
 */
int
pgm_rs_decode_parity_appended (
	rs_t*		rs,
	gf8_t*		block[],	/* length rs_t::n, the FEC block */
	guint		offsets[],	/* ordered index of packets */
	gsize		len		/* packet length */
	)
{
/* create new recovery matrix from generator
 */
	for (guint i = 0; i < rs->k; i++)
	{
		if (offsets[i] < rs->k) {
			memset (&rs->RM[ i * rs->k ], 0, rs->k * sizeof(gf8_t));
			rs->RM[ (i * rs->k) + i ] = 1;
			continue;
		}
		memcpy (&rs->RM[ i * rs->k ], &rs->GM[ offsets[ i ] * rs->k ], rs->k * sizeof(gf8_t));
	}

/* invert */
	matinv (rs->RM, rs->k);

/* multiply out, through the length of erasures[] */
	for (guint j = 0; j < rs->k; j++)
	{
		if (offsets[ j ] < rs->k)
			continue;

		int p = rs->k;
		gf8_t* erasure = block[ j ];
		for (guint i = 0; i < rs->k; i++)
		{
			gf8_t* src;
			if (offsets[ i ] < rs->k)
				src = block[ i ];
			else
				src = block[ p++ ];
			gf8_t c = rs->RM[ (j * rs->k) + i ];
			gf_vec_addmul (erasure, c, src, len);
		}
	}

	return 0;
}

/* eof */
