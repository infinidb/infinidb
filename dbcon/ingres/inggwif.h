/* Copyright (C) 2013 Calpont Corp.

   This library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation;
   version 2.1 of the License.

   This library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. */

// $Id: inggwif.h 9210 2013-01-21 14:10:42Z rdempsey $
#ifndef INGGWIF_H__
#define INGGWIF_H__

#ifdef __cplusplus
extern "C" {
#endif

void* ing_getqctx();
int ing_sendplan(void* qctx);
void* ing_opentbl(void* qctx, const char* sn, const char* tn);
int ing_getrows(void* qctx, void* tctx, char* buf, unsigned rowlen, unsigned nrows);
int ing_closetbl(void* qctx, void* tctx);
int ing_relqctx(void* qctx);
void ing_debugfcn(void* data);

#ifdef __cplusplus
}
#endif

#endif

