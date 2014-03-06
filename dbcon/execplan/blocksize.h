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

// $Id: blocksize.h 9210 2013-01-21 14:10:42Z rdempsey $
#ifndef EXECPLAN_BLOCKSIZE_H_
#define EXECPLAN_BLOCKSIZE_H_

#include <stdint.h>

// # of bytes in a block
const uint64_t BLOCK_SIZE = 8192;

// lobgical_block_rids is the # of rows 1-byter-column in a block
// its value is the same as block_size, but different unit
const uint64_t LOGICAL_BLOCK_RIDS = BLOCK_SIZE;

#endif

