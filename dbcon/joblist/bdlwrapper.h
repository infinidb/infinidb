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

// $Id: bdlwrapper.h 8436 2012-04-04 18:18:21Z rdempsey $
#ifndef JOBLISTDLWRAPPER_H
#define JOBLISTDLWRAPPER_H

//
// C++ Interface: dlwrapper
//
// Description: 
//
//
// Author: Jason Rodriguez <jrodriguez@calpont.com>
// Calpont (C) 2007
//
// Copyright: See COPYING file that comes with this distribution
//
//

/** @file */

#include "bucketdl.h"

namespace joblist {

/**
Wrapper class for DataList abstract baseclass

	@author Jason Rodriguez <jrodriguez@calpont.com>
*/

// TEMP: types for construction.
// should be support each type to support for Hash Join
// 
// TODO: is there an abstract class for column types
// or some other class to use here.

/** @brief class BDLWrapper
 *
 */
template <typename element_t>
class BDLWrapper {
	
	typedef BucketDL<element_t>		BucketDL_t;

public:

	BDLWrapper();
	BDLWrapper(BucketDL_t* bdlptr);

    ~BDLWrapper();

	void init();
	uint getIterator(uint n) const {return fBDLintPtr->getIterator(n);}
	
	bool next(const uint b, const uint i, element_t* v) const;

	uint size() const;
	uint size(uint n) const;

	uint bucketCount() const;

	void insert(element_t val) {fBDLintPtr->insert(val);}

	void dump() const ;
	void dump(uint n) const ;
	BucketDL_t* getBDL() const { return fBDLintPtr; }

private:

	BucketDL_t* fBDLintPtr;	

}; // BDLWrapper

template <typename element_t>
bool BDLWrapper<element_t>::next(const uint b, const uint i, element_t* v) const
{
	bool ret=false;
	ret = fBDLintPtr->next(b, i, v);
	return ret;
}

template <typename element_t>
BDLWrapper<element_t>::BDLWrapper()
{
	fBDLintPtr = NULL;
	return;
}

template <typename element_t>
BDLWrapper<element_t>::BDLWrapper(BucketDL_t* bdlptr)
{
	fBDLintPtr = bdlptr;
}

template <typename element_t>
BDLWrapper<element_t>::~BDLWrapper() {
}

template <typename element_t>
void BDLWrapper<element_t>::init() {
	return;
}

template <typename element_t>
uint BDLWrapper<element_t>::size() const
{
	if (fBDLintPtr==NULL)
		return 0;

	uint hSize=0;
	uint bCount = fBDLintPtr->bucketCount();
	
	for (int idx=0; idx < (int)bCount; idx++)
		hSize += fBDLintPtr->size(idx);
	
	return hSize;
}

template <typename element_t>
uint BDLWrapper<element_t>::bucketCount() const
{
	if (fBDLintPtr==NULL)
		return 0;

	return fBDLintPtr->bucketCount();

}

template <typename element_t>
uint BDLWrapper<element_t>::size(uint n) const
{
	if (fBDLintPtr==NULL)
		return 0;

	return fBDLintPtr->size(n);

}

template <typename element_t>
void BDLWrapper<element_t>::dump(uint n) const
{

	if (fBDLintPtr==NULL)
		return;

	int bSize = fBDLintPtr->size(n);
	element_t element;
	uint bucketIter;
	bool moreElems=true;

	bucketIter = fBDLintPtr->getIterator(n);
	std::cout << "::dump(n) bucket# " << n <<  " " << fBDLintPtr->size(n) << std::endl;

	for (; fBDLintPtr->size(n) && moreElems; )
	{
		moreElems = fBDLintPtr->next(n, bucketIter, &element);
		if (moreElems)
		std::cout << "Bkt " << n << " Idx " << bucketIter << " val "
			<< element.getValue() << std::endl;
	}

	std::cout << "---" <<  std::endl;
	bSize=0;
}

template <typename element_t>
void BDLWrapper<element_t>::dump() const
{
	if (fBDLintPtr==NULL)
		return;

	int bSize = fBDLintPtr->bucketCount();

	std::cout << "dump() bucket count " << bSize << std::endl;

	for(int idx=0; idx<bSize; idx++)
		dump(idx);

}

} // namespace joblist
#endif
