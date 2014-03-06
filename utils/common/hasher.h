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
 * $Id: hasher.h 3843 2013-05-31 13:46:24Z pleblanc $
 *
 *****************************************************************************/

/** @file 
 * class Hasher interface
 */

#ifndef UTILS_HASHER_H
#define UTILS_HASHER_H

#include <stdint.h>
#include <string.h>

namespace utils
{
/** @brief class Hasher
 *  As of 10/16/12, this implements the Murmur3 hash algorithm.
 */

inline uint32_t rotl32(uint32_t x, int8_t r)
{
	return (x << r) | (x >> (32 - r));
}

inline uint32_t fmix(uint32_t h)
{
	h ^= h >> 16;
	h *= 0x85ebca6b;
	h ^= h >> 13;
	h *= 0xc2b2ae35;
	h ^= h >> 16;

	return h;
}

inline uint64_t fmix(uint64_t k)
{
	k ^= k >> 33;
	k *= 0xff51afd7ed558ccdULL;
	k ^= k >> 33;
	k *= 0xc4ceb9fe1a85ec53ULL;
	k ^= k >> 33;

	return k;
}

inline uint64_t rotl64(uint64_t x, int8_t r)
{
	return (x << r) | (x >> (64 - r));
}

class Hasher {
public:
	inline uint32_t operator()(const std::string &s) const
	{
		return operator()(s.data(), s.length());
	}

	inline uint32_t operator()(const char *data, uint64_t len) const
	{
		const int nblocks = len / 4;

		uint32_t h1 = 0;

		const uint32_t c1 = 0xcc9e2d51;
		const uint32_t c2 = 0x1b873593;

		//----------
		// body

		const uint32_t * blocks = (const uint32_t *) (data + nblocks * 4);

		for (int i = -nblocks; i; i++) {
			uint32_t k1 = blocks[i];

			k1 *= c1;
			k1 = rotl32(k1, 15);
			k1 *= c2;

			h1 ^= k1;
			h1 = rotl32(h1, 13);
			h1 = h1 * 5 + 0xe6546b64;
		}

		//----------
		// tail

		const uint8_t * tail = (const uint8_t*) (data + nblocks * 4);

		uint32_t k1 = 0;

		switch (len & 3) {
		case 3:	k1 ^= tail[2] << 16;
		case 2:	k1 ^= tail[1] << 8;
		case 1:	k1 ^= tail[0];
			k1 *= c1;
			k1 = rotl32(k1, 15);
			k1 *= c2;
			h1 ^= k1;
		};

		//----------
		// finalization

		h1 ^= len;
		h1 = fmix(h1);
		return h1;
	}
};

class Hasher_r {
public:
	inline uint32_t operator()(const char *data, uint64_t len, uint32_t seed) const
	{
		const int nblocks = len / 4;

		uint32_t h1 = seed;

		const uint32_t c1 = 0xcc9e2d51;
		const uint32_t c2 = 0x1b873593;

		//----------
		// body

		const uint32_t * blocks = (const uint32_t *) (data + nblocks * 4);

		for (int i = -nblocks; i; i++) {
			uint32_t k1 = blocks[i];

			k1 *= c1;
			k1 = rotl32(k1, 15);
			k1 *= c2;

			h1 ^= k1;
			h1 = rotl32(h1, 13);
			h1 = h1 * 5 + 0xe6546b64;
		}

		//----------
		// tail

		const uint8_t * tail = (const uint8_t*) (data + nblocks * 4);

		uint32_t k1 = 0;

		switch (len & 3) {
		case 3:	k1 ^= tail[2] << 16;
		case 2:	k1 ^= tail[1] << 8;
		case 1:	k1 ^= tail[0];
			k1 *= c1;
			k1 = rotl32(k1, 15);
			k1 *= c2;
			h1 ^= k1;
		};

		return h1;
	}
	
	inline uint32_t finalize(uint32_t seed, uint32_t len) const {
		seed ^= len;
		seed = fmix(seed);
		return seed;
	}
};

class Hasher128 {
public:
	inline uint64_t operator()(const char *data, uint64_t len) const
	{
		const int nblocks = len / 16;

		uint64_t h1 = 0;
		uint64_t h2 = 0;

		const uint64_t c1 = 0x87c37b91114253d5ULL;
		const uint64_t c2 = 0x4cf5ad432745937fULL;

		//----------
		// body

		const uint64_t * blocks = (const uint64_t *) (data);

		for (int i = 0; i < nblocks; i++) {
			uint64_t k1 = blocks[i * 2 + 0];
			uint64_t k2 = blocks[i * 2 + 1];

			k1 *= c1;
			k1 = rotl64(k1, 31);
			k1 *= c2;
			h1 ^= k1;

			h1 = rotl64(h1, 27);
			h1 += h2;
			h1 = h1 * 5 + 0x52dce729;

			k2 *= c2;
			k2 = rotl64(k2, 33);
			k2 *= c1;
			h2 ^= k2;

			h2 = rotl64(h2, 31);
			h2 += h1;
			h2 = h2 * 5 + 0x38495ab5;
		}

		//----------
		// tail

		const uint8_t * tail = (const uint8_t*) (data + nblocks * 16);

		uint64_t k1 = 0;
		uint64_t k2 = 0;

		switch (len & 15) {
		case 15: k2 ^= uint64_t(tail[14]) << 48;
		case 14: k2 ^= uint64_t(tail[13]) << 40;
		case 13: k2 ^= uint64_t(tail[12]) << 32;
		case 12: k2 ^= uint64_t(tail[11]) << 24;
		case 11: k2 ^= uint64_t(tail[10]) << 16;
		case 10: k2 ^= uint64_t(tail[9]) << 8;
		case 9:	k2 ^= uint64_t(tail[8]) << 0;
			k2 *= c2;
			k2 = rotl64(k2, 33);
			k2 *= c1;
			h2 ^= k2;
		case 8:	k1 ^= uint64_t(tail[7]) << 56;
		case 7:	k1 ^= uint64_t(tail[6]) << 48;
		case 6:	k1 ^= uint64_t(tail[5]) << 40;
		case 5:	k1 ^= uint64_t(tail[4]) << 32;
		case 4:	k1 ^= uint64_t(tail[3]) << 24;
		case 3:	k1 ^= uint64_t(tail[2]) << 16;
		case 2:	k1 ^= uint64_t(tail[1]) << 8;
		case 1:	k1 ^= uint64_t(tail[0]) << 0;
			k1 *= c1;
			k1 = rotl64(k1, 31);
			k1 *= c2;
			h1 ^= k1;
		};

		//----------
		// finalization

		h1 ^= len;
		h2 ^= len;

		h1 += h2;
		h2 += h1;

		h1 = fmix(h1);
		h2 = fmix(h2);

		h1 += h2;
		h2 += h1;

		return h1;
	}
};

//------------------------------------------------------------------------------
/** @brief class TupleHasher
 *
 */
//------------------------------------------------------------------------------
class TupleHasher
{
    public:
        TupleHasher(uint32_t len) : fHashLen(len) {}
        inline uint64_t operator()(const uint8_t* hashKey) const
        {
            return fHasher(reinterpret_cast<const char*>(hashKey), fHashLen);
        }
    private:
        Hasher   fHasher;
        uint32_t fHashLen;
};


//------------------------------------------------------------------------------
/** @brief class TupleComparator
 *
 */
//------------------------------------------------------------------------------
class TupleComparator
{
    public:
        TupleComparator(uint32_t len) : fCmpLen(len) {}
        inline bool operator()(const uint8_t* hashKey1, const uint8_t* hashKey2) const
        {
            return (memcmp(hashKey1, hashKey2, fCmpLen) == 0);
        }
    private:
        uint32_t fCmpLen;
};


}

#endif  // UTILS_HASHER_H
