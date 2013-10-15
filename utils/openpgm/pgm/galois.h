/*
 * Galois field maths.
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

#ifndef __PGM_GALOIS_H__
#define __PGM_GALOIS_H__

#include <glib.h>


/* 8 bit wide galois field integer: GF(2⁸) */
typedef guint8 gf8_t;

/* E denotes the encoding symbol length in bytes.
 * S denotes the symbol size in units of m-bit elements.  When m = 8,
 * then S and E are equal.
 */
#define GF_ELEMENT_BYTES	sizeof(gf8_t)

/* m defines the length of the elements in the finite field, in bits.
 * m belongs to {2..16}.
 */
#define GF_ELEMENT_BITS		( 8 * GF_ELEMENT_BYTES )

/* q defines the number of elements in the finite field.
 */
#define GF_NO_ELEMENTS		( 1 << GF_ELEMENT_BITS )
#define GF_MAX			( GF_NO_ELEMENTS - 1 )


extern const gf8_t gflog[GF_NO_ELEMENTS];
extern const gf8_t gfantilog[GF_NO_ELEMENTS];

#ifdef CONFIG_GALOIS_MUL_LUT
extern const gf8_t gftable[GF_NO_ELEMENTS * GF_NO_ELEMENTS];
#endif


G_BEGIN_DECLS


/* In a finite field with characteristic 2, addition and subtraction are
 * identical, and are accomplished using the XOR operator. 
 */
static inline gf8_t
gfadd (
	gf8_t		a,
	gf8_t		b
	)
{
	return a ^ b;
}

static inline gf8_t
gfadd_equals (
	gf8_t		a,
	gf8_t		b
	)
{
	return a ^= b;
}

static inline gf8_t
gfsub (
	gf8_t		a,
	gf8_t		b
	)
{
	return gfadd (a, b);
}

static inline gf8_t
gfsub_equals (
	gf8_t		a,
	gf8_t		b
	)
{
	return gfadd_equals (a, b);
}

static inline gf8_t
gfmul (
        gf8_t           a,
        gf8_t           b
        )
{
	if (G_UNLIKELY( !(a && b) )) {
		return 0;
	}

#ifdef CONFIG_GALOIS_MUL_LUT
	return gftable[ (guint16)a << 8 | (guint16)b ];
#else
	guint sum = gflog[ a ] + gflog[ b ];
	return sum >= GF_MAX ? gfantilog[ sum - GF_MAX ] : gfantilog[ sum ];
#endif
}

static inline gf8_t
gfdiv (
        gf8_t           a,
        gf8_t           b
        )
{
	if (G_UNLIKELY( !a )) {
		return 0;
	}

	gint sum = gflog[ a ] - gflog[ b ];
	return sum < 0 ? gfantilog[ sum + GF_MAX ] : gfantilog[ sum ];
}

G_END_DECLS

#endif /* __PGM_GALOIS_H__ */
