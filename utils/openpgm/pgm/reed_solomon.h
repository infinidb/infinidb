/*
 * Reed-Solomon forward error correction based on Vandermonde matrices
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

#ifndef __PGM_REED_SOLOMON_H__
#define __PGM_REED_SOLOMON_H__

#include <glib.h>

G_BEGIN_DECLS

#define PGM_RS_DEFAULT_N	255


int pgm_rs_create (gpointer*, guint, guint);
int pgm_rs_destroy (gpointer);

int pgm_rs_encode (gpointer, const void**, guint, void*, gsize);
int pgm_rs_decode_parity_inline (gpointer, void**, guint*, gsize);
int pgm_rs_decode_parity_appended (gpointer, void**, guint*, gsize);

#if 0
int pgm_rs_bch_create (gpointer*, int, int);
int pgm_rs_bch_destroy (gpointer);

int pgm_rs_bch_encode (gpointer, const void**, void**, int);
int pgm_rs_bch_decode_parity_inline (gpointer, void*, int*, int);
int pgm_rs_bch_decode_parity_appended (gpointer, void*, int*, int);
#endif


G_END_DECLS

#endif /* __PGM_REED_SOLOMON_H__ */
