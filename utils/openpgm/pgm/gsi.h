/* vim:ts=8:sts=4:sw=4:noai:noexpandtab
 * 
 * global session ID helper functions
 *
 * Copyright (c) 2006-2007 Miru Limited.
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

#ifndef __PGM_GSI_H__
#define __PGM_GSI_H__

#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>

#include <glib.h>


#define PGM_GSISTRLEN		(sizeof("000.000.000.000.000.000"))


typedef struct pgm_gsi_t pgm_gsi_t;

struct pgm_gsi_t {
	guint8	identifier[6];
};

G_BEGIN_DECLS

int pgm_create_md5_gsi (pgm_gsi_t*);
int pgm_create_ipv4_gsi (pgm_gsi_t*);

int pgm_print_gsi_r (const pgm_gsi_t*, char*, gsize);
gchar* pgm_print_gsi (const pgm_gsi_t*);
gint pgm_gsi_equal (gconstpointer, gconstpointer);


G_END_DECLS

#endif /* __PGM_GSI_H__ */
