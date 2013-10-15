/* vim:ts=8:sts=4:sw=4:noai:noexpandtab
 * 
 * Re-entrant safe signal handling.
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

#ifndef __PGM_SIGNAL_H__
#define __PGM_SIGNAL_H__

#include <signal.h>

#include <glib.h>

#ifdef CONFIG_HAVE_SIGHANDLER_T
#	define pgm_sighandler_t		sighandler_t
#else
typedef void (*pgm_sighandler_t)(int);
#endif

G_BEGIN_DECLS

pgm_sighandler_t pgm_signal_install (int, pgm_sighandler_t);

G_END_DECLS

#endif /* __PGM_SIGNAL_H__ */
