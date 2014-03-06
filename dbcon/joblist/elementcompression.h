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

/******************************************************************************
 * $Id: elementcompression.h 9655 2013-06-25 23:08:13Z xlou $
 *
 *****************************************************************************/

/** @file */

#ifndef ELEMENTCOMPRESSION_H_
#define ELEMENTCOMPRESSION_H_

#include <fstream>
#include <stdexcept>
#include <stdint.h>
#include <vector>

#include "elementtype.h"

#ifndef __GNUC__
#  ifndef __attribute__
#    define __attribute__(x)
#  endif
#endif

namespace joblist
{

//------------------------------------------------------------------------------
// Structs used for storing compressed element types externally to disk.
// Note the use of "packed" to keep the compiler from padding the struct.
// This is done so that a vector of these element types will be contiguous,
// and can thus be read and written with a single block read/write.
//------------------------------------------------------------------------------
struct CompElement64Rid32Val
{
    uint64_t first;
    uint32_t second;
} __attribute__((__packed__));

struct CompElement32Rid64Val
{
    uint32_t first;
    uint64_t second;
} __attribute__((__packed__));

struct CompElement32Rid32Val
{
    uint32_t first;
    uint32_t second;
};

struct CompElement32RidOnly
{
	uint32_t first;
};

//------------------------------------------------------------------------------
/** @brief Utilities to compress/expand various element type datalists.
 *
 * Purpose of utiltiies is to compress elementtypes as they are saved to disk,
 * and conversely, to expand them when they are read back from disk.
 */
//------------------------------------------------------------------------------
class ElementCompression
{
	public:
		//
		//...Utilities to compress from 64 bit to 32 bit for RID and/or Value
		//
		template <typename DestType>
		static void compress(
			std::vector<ElementType>&             vIn,
			std::vector<DestType>&                vOut);
		static void compress(
			std::vector<ElementType>&             vIn,
			std::vector<CompElement32RidOnly>&    vOut)
			{ throw std::logic_error(
				"Compression of ElementType to 32RidOnly not supported"); }

		static void compress(
			std::vector<StringElementType>&       vIn,
			std::vector<CompElement64Rid32Val>&   vOut)
			{ throw std::logic_error(
				"Compression of StringElementType to 64/32 not supported"); }
		static void compress(
			std::vector<StringElementType>&       vIn,
			std::vector<CompElement32Rid64Val>&   vOut)
			{ throw std::logic_error(
				"Compression of StringElementType to 32/64 not supported"); }
		static void compress(
			std::vector<StringElementType>&       vIn,
			std::vector<CompElement32Rid32Val>&   vOut)
			{ throw std::logic_error(
				"Compression of StringElementType to 32/32 not supported"); }
		static void compress(
			std::vector<StringElementType>&       vIn,
			std::vector<CompElement32RidOnly>&    vOut)
			{ throw std::logic_error(
				"Compression of StringElementType to 32RidOnly not supported");}

		static void compress(
			std::vector<DoubleElementType>&       vIn,
			std::vector<CompElement64Rid32Val>&   vOut)
			{ throw std::logic_error(
				"Compression of DoubleElementType to 64/32 not supported"); }
		static void compress(
			std::vector<DoubleElementType>&       vIn,
			std::vector<CompElement32Rid64Val>&   vOut)
			{ throw std::logic_error(
				"Compression of DoubleElementType to 32/64 not supported"); }
		static void compress(
			std::vector<DoubleElementType>&       vIn,
			std::vector<CompElement32Rid32Val>&   vOut)
			{ throw std::logic_error(
				"Compression of DoubleElementType to 32/32 not supported"); }
		static void compress(
			std::vector<DoubleElementType>&       vIn,
			std::vector<CompElement32RidOnly>&    vOut)
			{ throw std::logic_error(
				"Compression of DoubleElementType to 32RidOnly not supported");}

		static void compress(
			std::vector<RIDElementType>&          vIn,
			std::vector<CompElement64Rid32Val>&   vOut)
			{ throw std::logic_error(
				"Compression of RIDElementType to 64/32 not supported"); }
		static void compress(
			std::vector<RIDElementType>&          vIn,
			std::vector<CompElement32Rid64Val>&   vOut)
			{ throw std::logic_error(
				"Compression of RIDElementType to 32/64 not supported"); }
		static void compress(
			std::vector<RIDElementType>&          vIn,
			std::vector<CompElement32Rid32Val>&   vOut)
			{ throw std::logic_error(
				"Compression of RIDElementType to 32/32 not supported"); }
		static void compress(
			std::vector<RIDElementType>&          vIn,
			std::vector<CompElement32RidOnly>&    vOut);

	    static void compress(
			std::vector<TupleType>&          vIn,
			std::vector<CompElement64Rid32Val>&   vOut)
			{ throw std::logic_error(
				"Compression of TupleType to 64/32 not supported"); }
		static void compress(
			std::vector<TupleType>&          vIn,
			std::vector<CompElement32Rid64Val>&   vOut)
			{ throw std::logic_error(
				"Compression of TupleType to 32/64 not supported"); }
		static void compress(
			std::vector<TupleType>&          vIn,
			std::vector<CompElement32Rid32Val>&   vOut)
			{ throw std::logic_error(
				"Compression of TupleType to 32/32 not supported"); }
		static void compress(
			std::vector<TupleType>&          vIn,
			std::vector<CompElement32RidOnly>&    vOut)
			{ throw std::logic_error(
				"Compression of TupleType to 32/32 not supported"); }

		//
		//...Utilities to expand from 32 bit to 64 bit for RID and/or Value
		//
		template <typename SrcType>
		static void expand(
			std::vector<SrcType>&                 vIn,
			ElementType*                          vOut);
		static void expand(
			std::vector<CompElement32RidOnly>&    vIn,
			ElementType*                          vOut)
			{ throw std::logic_error(
				"Expansion to ElementType from 32RidOnly not supported");}

		static void expand(
			std::vector<CompElement64Rid32Val>&   vIn,
			StringElementType*                    vOut)
			{ throw std::logic_error(
				"Expansion to StringElementType from 64/32 not supported"); }
		static void expand(
			std::vector<CompElement32Rid64Val>&   vIn,
			StringElementType*                    vOut)
			{ throw std::logic_error(
				"Expansion to StringElementType from 32/64 not supported"); }
		static void expand(
			std::vector<CompElement32Rid32Val>&   vIn,
			StringElementType*                    vOut)
			{ throw std::logic_error(
				"Expansion to StringElementType from 32/32 not supported"); }
		static void expand(
			std::vector<CompElement32RidOnly>&    vIn,
			StringElementType*                    vOut)
			{ throw std::logic_error(
				"Expansion to StringElementType from 32RidOnly not supported");}

		static void expand(
			std::vector<CompElement64Rid32Val>&   vIn,
			DoubleElementType*                    vOut)
			{ throw std::logic_error(
				"Expansion to DoubleElementType from 64/32 not supported"); }
		static void expand(
			std::vector<CompElement32Rid64Val>&   vIn,
			DoubleElementType*                    vOut)
			{ throw std::logic_error(
				"Expansion to DoubleElementType from 32/64 not supported"); }
		static void expand(
			std::vector<CompElement32Rid32Val>&   vIn,
			DoubleElementType*                    vOut)
			{ throw std::logic_error(
				"Expansion to DoubleElementType from 32/32 not supported"); }
		static void expand(
			std::vector<CompElement32RidOnly>&    vIn,
			DoubleElementType*                    vOut)
			{ throw std::logic_error(
				"Expansion to DoubleElementType from 32RidOnly not supported");}

		static void expand(
			std::vector<CompElement64Rid32Val>&   vIn,
			RIDElementType*                       vOut)
			{ throw std::logic_error(
				"Expansion to RIDElementType from 64/32 not supported"); }
		static void expand(
			std::vector<CompElement32Rid64Val>&   vIn,
			RIDElementType*                       vOut)
			{ throw std::logic_error(
				"Expansion to RIDElementType from 32/64 not supported"); }
		static void expand(
			std::vector<CompElement32Rid32Val>&   vIn,
			RIDElementType*                       vOut)
			{ throw std::logic_error(
				"Expansion to RIDElementType from 32/32 not supported"); }
		static void expand(
			std::vector<CompElement32RidOnly>&    vIn,
			RIDElementType*                       vOut);

		static void expand(
			std::vector<CompElement64Rid32Val>&   vIn,
			TupleType*                       vOut)
			{ throw std::logic_error(
				"Expansion to TupleType from 64/32 not supported"); }
		static void expand(
			std::vector<CompElement32Rid64Val>&   vIn,
			TupleType*                       vOut)
			{ throw std::logic_error(
				"Expansion to TupleType from 32/64 not supported"); }
		static void expand(
			std::vector<CompElement32Rid32Val>&   vIn,
			TupleType*                       vOut)
			{ throw std::logic_error(
				"Expansion to TupleType from 32/32 not supported"); }
		static void expand(
			std::vector<CompElement32RidOnly>&    vIn,
			TupleType*                       vOut)
			{ throw std::logic_error(
				"Expansion to TupleType from 32/32 not supported"); }

		//
		//...Utilities to write a single element with a compressed 32 bit RID.
		//
		static void writeWith32Rid(
			const ElementType& e,
			std::fstream&      fFile);
		static void writeWith32Rid(
			const DoubleElementType& e,
			std::fstream&            fFile)
			{ throw std::logic_error(
				"Compress/Write of 32 RID DoubleElementType not supported"); }
		static void writeWith32Rid(
			const StringElementType& e,
			std::fstream&            fFile);
		static void writeWith32Rid(
			const RIDElementType& e,
			std::fstream&         fFile);
	    static void writeWith32Rid(
			const TupleType& e,
		    std::fstream&         fFile)
		{throw std::logic_error(
				"Compress/Write of 32 RID TupleType not supported");}

		//
		//...Utilities to read a single element with a compressed 32 bit RID.
		//
		static void readWith32Rid(
			ElementType&  e,
			std::fstream& fFile);
		static void readWith32Rid(
			DoubleElementType& e,
			std::fstream&      fFile)
			{ throw std::logic_error(
				"Read/Expand of 32 RID DoubleElementType not supported"); }
		static void readWith32Rid(
			StringElementType& e,
			std::fstream&      fFile);
		static void readWith32Rid(
			RIDElementType& e,
			std::fstream&   fFile);
	    static void readWith32Rid(
			TupleType& e,
		    std::fstream&   fFile)
		    {throw std::logic_error(
				"Read/Expand of 32 RID TupleType not supported");}
};

//------------------------------------------------------------------------------
// Inline utilities to compress from 64 bit to 32 bit for RID and/or Value
//------------------------------------------------------------------------------

//
//...Compress RID/Value to 64 bit RID, 32 bit value
//...Compress RID/Value to 32 bit RID, 64 bit value
//...Compress RID/Value to 32 bit RID, 32 bit value
//
template <typename DestType>
/* static */ inline void
ElementCompression::compress(
	std::vector<ElementType>&          vIn,
	std::vector<DestType>&             vOut)
{
	uint64_t count = vIn.size();
	for (unsigned int i=0; i<count; i++)
	{
		vOut[i].first = vIn[i].first;
		vOut[i].second= vIn[i].second;
	}
}

//
//...Compress RID only to 32 bit RID
//...This method has to be defined, rather than relying on the template ver-
//...sion of compress() because this version does not access or copy the
//..."second" data member.
//
/* static */ inline void
ElementCompression::compress(
	std::vector<RIDElementType>&       vIn,
	std::vector<CompElement32RidOnly>& vOut)
{
	uint64_t count = vIn.size();
	for (unsigned int i=0; i<count; i++)
	{
		vOut[i].first = vIn[i].first;
	}
}

//------------------------------------------------------------------------------
// Inline utilities to expand from 32 bit to 64 bit for RID and/or Value
//------------------------------------------------------------------------------

//
//...Expand RID/Value from 64 bit RID, 32 bit value
//...Expand RID/Value from 32 bit RID, 64 bit value
//...Expand RID/Value from 32 bit RID, 32 bit value
//
template <typename SrcType>
/* static */ inline void
ElementCompression::expand(
	std::vector<SrcType>&              vIn,
	ElementType*                       vOut)
{
	uint64_t count = vIn.size();
	for (unsigned int i=0; i<count; i++)
	{
		vOut[i].first = vIn[i].first;
		vOut[i].second= vIn[i].second;
	}
}

//
//...Expand RID only from 32 bit RID
//...This method has to be defined, rather than relying on the template ver-
//...sion of expand() because this version does not access or copy the
//..."second" data member.
//
/* static */ inline void
ElementCompression::expand(
	std::vector<CompElement32RidOnly>& vIn,
	RIDElementType*                    vOut)
{
	uint64_t count = vIn.size();
	for (unsigned int i=0; i<count; i++)
	{
		vOut[i].first = vIn[i].first;
	}
}

//------------------------------------------------------------------------------
// Inline utilities to write a single element with a compressed 32 bit RID.
//------------------------------------------------------------------------------

/* static */ inline void
ElementCompression::writeWith32Rid(
	const ElementType& e,
	std::fstream&      fFile)
{
	CompElement32Rid64Val eCompressed;
	eCompressed.first  = e.first;
	eCompressed.second = e.second;
	fFile.write((char *) &eCompressed, sizeof(CompElement32Rid64Val));
}

/* static */ inline void
ElementCompression::writeWith32Rid(
	const StringElementType& e,
	std::fstream&            fFile)
{
	uint32_t rid  = e.first;
	uint16_t dlen = e.second.length();

	fFile.write((char*)&rid, sizeof(rid) );
	fFile.write((char*)&dlen,sizeof(dlen));
	fFile.write( e.second.c_str(),  dlen );
}

/* static */ inline void
ElementCompression::writeWith32Rid(
	const RIDElementType& e,
	std::fstream&         fFile)
{
	CompElement32RidOnly eCompressed;
	eCompressed.first  = e.first;
	fFile.write((char *) &eCompressed, sizeof(CompElement32RidOnly));
}

//------------------------------------------------------------------------------
// Inline utilities to read a single element with a compressed 32 bit RID.
//------------------------------------------------------------------------------

/* static */ inline void
ElementCompression::readWith32Rid(
	ElementType&  e,
	std::fstream& fFile)
{
	CompElement32Rid64Val eCompressed;
	fFile.read((char *) &eCompressed, sizeof(CompElement32Rid64Val));
	e.first  = eCompressed.first;
	e.second = eCompressed.second;
}

/* static */ inline void
ElementCompression::readWith32Rid(
	StringElementType& e,
	std::fstream&      fFile)
{
	uint32_t rid  = 0;
	uint16_t dlen = 0;
	char d[32768];

	fFile.read((char*)&rid, sizeof(rid) );
	fFile.read((char*)&dlen,sizeof(dlen));
	fFile.read( d,  dlen );

	e.first  = rid;
	e.second.assign(d, dlen);
}

/* static */ inline void
ElementCompression::readWith32Rid(
	RIDElementType& e,
	std::fstream&   fFile)
{
	CompElement32RidOnly eCompressed;
	fFile.read((char *) &eCompressed, sizeof(CompElement32RidOnly));
	e.first  = eCompressed.first;
}

} // end of joblist namespace

#endif
