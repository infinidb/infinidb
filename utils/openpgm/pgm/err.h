/* vim:ts=8:sts=8:sw=4:noai:noexpandtab
 * 
 * PGM error structure ala MSG_ERRQUEUE
 *
 * Copyright (c) 2006-2009 Miru Limited.
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

#ifndef __PGM_ERR_H__
#define __PGM_ERR_H__

#pragma pack(push, 1)

struct pgm_sock_err_t {
#ifdef __PGM_TRANSPORT_H__
	pgm_tsi_t	tsi;
#else
	char		identifier[8];		/* TSI */
#endif
	guint32		lost_count;
};

typedef struct pgm_sock_err_t pgm_sock_err_t;

#pragma pack(pop)


#endif /* __PGM_ERR_H__ */
