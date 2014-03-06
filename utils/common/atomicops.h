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

/** @file */

#ifndef COMMON_ATOMICOPS_H__
#define COMMON_ATOMICOPS_H__

#include <unistd.h>
#if defined(__linux__)
#include <stdint.h>
#endif
#if !defined(_MSC_VER)
#include <sched.h>
#endif

/*
This is an attempt to wrap the differneces between Windows and Linux around atomic ops.
Boost has something in interprocess::ipcdetail, but it doesn't have 64-bit API's.
*/

namespace atomicops
{

//Returns the resulting, incremented value
template <typename T>
inline T atomicInc(volatile T* mem)
{
#ifdef _MSC_VER
	switch (sizeof(T))
	{
	case 4:
	default:
		return InterlockedIncrement(reinterpret_cast<volatile LONG*>(mem));

	case 8:
		return InterlockedIncrement64(reinterpret_cast<volatile LONGLONG*>(mem));
	}
#else
	return __sync_add_and_fetch(mem, 1);
#endif
}

//decrements, but returns the pre-decrement value
template <typename T>
inline T atomicDec(volatile T* mem)
{
#ifdef _MSC_VER
	switch (sizeof(T))
	{
	case 4:
	default:
		return InterlockedDecrement(reinterpret_cast<volatile LONG*>(mem))+1;

	case 8:
		return InterlockedDecrement64(reinterpret_cast<volatile LONGLONG*>(mem))+1;
	}
#else
	return __sync_fetch_and_add(mem, -1);
#endif
}

//Returns the resulting value (but doesn't need to yet)
template <typename T>
inline T atomicAdd(volatile T* mem, T val)
{
#ifdef _MSC_VER
	switch (sizeof(T))
	{
	case 4:
	default:
		InterlockedExchangeAdd(reinterpret_cast<volatile LONG*>(mem), val);
		break;

	case 8:
		InterlockedExchangeAdd64(reinterpret_cast<volatile LONGLONG*>(mem), val);
		break;
	}
	return *mem;
#else
	return __sync_add_and_fetch(mem, val);
#endif
}

//Returns the resulting value
template <typename T>
inline T atomicSub(volatile T* mem, T val)
{
#ifdef _MSC_VER
	switch (sizeof(T))
	{
	case 4:
	default:
		InterlockedExchangeAdd(reinterpret_cast<volatile LONG*>(mem), -(static_cast<LONG>(val)));
		break;
	case 8:
		InterlockedExchangeAdd64(reinterpret_cast<volatile LONGLONG*>(mem), -(static_cast<LONGLONG>(val)));
		break;
	}
	return *mem;
#else
	return __sync_sub_and_fetch(mem, val);
#endif
}

//Implements a memory barrier
inline void atomicMb()
{
#ifdef _MSC_VER
	MemoryBarrier();
#else
	__sync_synchronize();
#endif
}

//Returns true iff the CAS took place, that is
// if (*mem == comp) {
//   *mem = swap;
//   return true;
// } else {
//   return false;
// }
template <typename T>
inline bool atomicCAS(volatile T* mem, T comp, T swap)
{
#ifdef _MSC_VER
	switch (sizeof(T))
	{
	case 4:
	default:
		//The function returns the initial value of the mem parameter
		return (InterlockedCompareExchange(reinterpret_cast<volatile LONG*>(mem), swap, comp) == comp);

	case 8:
		return (InterlockedCompareExchange64(reinterpret_cast<volatile LONGLONG*>(mem), swap, comp) == comp);
	}
#else
	//If the current value of *mem is comp, then write swap into *comp. Return true if the comparison is successful and swap was written.
	return __sync_bool_compare_and_swap(mem, comp, swap);
#endif
}

//Implements a scheduler yield
inline void atomicYield()
{
#ifdef _MSC_VER
	SwitchToThread();
#else
	sched_yield();
#endif
}

}

#endif
