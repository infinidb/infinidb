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

#ifndef UTIL_H
#define UTIL_H

#include <stdlib.h>

#define MALLOC(type) ((type*)calloc(1, sizeof(type)))

/* bitmap manipulation */
#define BITS_PER_ITEM(map) (sizeof(map[0])*8)
#define MASK(pos,map) (1 << ((pos) % (BITS_PER_ITEM(map))))
#define POS(pos,map)  ((pos) / BITS_PER_ITEM(map))
#define SET_BIT(x, map) (map[POS(x,map)] |= MASK(x,map))
#define CLR_BIT(x, map) (map[POS(x,map)] &= ~MASK(x,map))
#define BIT_ISSET(x, map) (map[POS(x,map)] & MASK(x,map))


#endif
