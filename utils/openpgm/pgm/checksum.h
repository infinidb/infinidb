/* vim:ts=8:sts=4:sw=4:noai:noexpandtab
 * 
 * PGM checksum routines
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

#ifndef __PGM_CHECKSUM_H__
#define __PGM_CHECKSUM_H__

#include <glib.h>


G_BEGIN_DECLS

guint16 pgm_inet_checksum (const void*, guint, int);

guint16 pgm_csum_fold (guint32);
guint32 pgm_csum_block_add (guint32, guint32, guint);

guint32 pgm_compat_csum_partial (const void*, guint, guint32);
guint32 pgm_compat_csum_partial_copy (const void*, void*, guint, guint32);

#ifdef CONFIG_CKSUM_COPY

#ifdef __x86_64__

static inline unsigned add32_with_carry (unsigned a, unsigned b)
{
    asm("addl %2,%0\n\t"
	    "adcl $0,%0"
	    : "=r" (a)
	    : "0" (a), "r" (b));
    return a;
}

unsigned pgm_asm64_csum_partial(const unsigned char *buff, unsigned len);

static inline guint32 pgm_csum_partial(const void *buff, guint len, guint32 sum)
{
    return (guint32)add32_with_carry(pgm_asm64_csum_partial(buff, len), sum);
}

#else

guint32 pgm_csum_partial(const void *buff, guint len, guint32 sum);

#endif /* __x86_64__ */

guint32 pgm_csum_partial_copy_generic (const unsigned char *src, const unsigned char *dst,
					unsigned len,
					unsigned sum, 
					int *src_err_ptr, int *dst_err_ptr);

static inline guint32 pgm_csum_partial_copy (const void *src, void *dst, unsigned len, unsigned sum)
{
    return pgm_csum_partial_copy_generic (src, dst, len, sum, NULL, NULL);
}

#else

#   define pgm_csum_partial            pgm_compat_csum_partial
#   define pgm_csum_partial_copy       pgm_compat_csum_partial_copy

#endif /* CONFIG_CKSUM_COPY */

G_END_DECLS

#endif /* __PGM_CHECKSUM_H__ */

