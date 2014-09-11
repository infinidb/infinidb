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
 * $Id: filteroperation.cpp 7409 2011-02-08 14:38:50Z rdempsey $
 *
 *****************************************************************************/

#include "filteroperation.h"
#include "constantdatalist.h"
#include "iostream"
#include "timeset.h"

using namespace std;
namespace joblist
{

template<typename element_t>
void FilterOperation::filter(int8_t COP, FIFO<RowWrapper<element_t> >& dl1, 
	FIFO<RowWrapper<element_t> >& dl2, FIFO<RowWrapper<element_t> >& dlOut, uint64_t & resultCount, TimeSet& ts)
{

	bool constantDL = false;
	// Validate that if a ConstantDataList is being used that it's the second one passed in.
	if(typeid(ConstantDataList<element_t>) == typeid(dl1)) {
		throw(std::runtime_error("FilterOperation::filter(): first DataList argument cannot be a ConstantDataList."));
	}
	if(typeid(ConstantDataList<element_t>) == typeid(dl2)) {
		RowWrapper<element_t> rw;
		int id = dl2.getIterator();
		if(!dl2.next(id, &rw)) {
			throw(std::runtime_error("FilterOperation::filter(): ConstantDataList passed does not contain any values."));
		}
		constantDL = true;
		
	}

	// Iterate over the data lists, perform the comparison, and load the output datalist with the qualifying
	// RIDs and values.
	RowWrapper<element_t> rw1, rw2, rw3;
	int id1, id2;
	id1 = dl1.getIterator();
	id2 = dl2.getIterator();		
	ts.setTimer(filterCompare);
	if(constantDL) {
		dl2.next(id2, &rw2);
		typename element_t::second_type c = rw2.et[0].second;
		while(dl1.next(id1, &rw1)) {
			for (uint64_t i = 0; i < rw1.count; ++i) {
				if(compare(COP, rw1.et[i].second, c)) {
					resultCount++;
					rw3.et[rw3.count++] = rw1.et[i];
					if (rw3.count == rw3.ElementsPerGroup) {
						ts.setTimer(filterInsert);
						dlOut.insert(rw3);
						rw3.count = 0;
						ts.holdTimer(filterInsert);
					}
				}
			}
		}
		dlOut.endOfInput();
		dlOut.totalSize(resultCount);
		ts.setTimer(filterCompare, false);
	}
	else {
		bool more1 = dl1.next(id1, &rw1);
		bool more2 = dl2.next(id2, &rw2);
		uint64_t i=0, j=0;

		while(more1 && more2) {
			while (i < rw1.count && j < rw2.count) {
				
				//cout << "IN1: " << rw1.et[i].first << " : " << rw1.et[i].second  << "    IN2: " << rw2.et[i].first << " : " <<rw2.et[i].second << endl;
				if(rw1.et[i].first > rw2.et[j].first) {					
					++j;
				}
				else if (rw1.et[i].first < rw2.et[j].first) {
					++i;
				}
				else {				
					if(compare(COP, rw1.et[i].second, rw2.et[j].second)) {
						resultCount++;
						rw3.et[rw3.count++] = rw1.et[i];
						if (rw3.count == rw3.ElementsPerGroup) {
							ts.setTimer(filterInsert);
							dlOut.insert(rw3);
							rw3.count = 0;
							ts.holdTimer(filterInsert);
						}
					}
					++i;
					++j;
				}
			}
			
			if(i == rw1.count) {
				more1 = dl1.next(id1, &rw1);
				i = 0;
			}
			if(j == rw2.count) {
				more2 = dl2.next(id2, &rw2);
				j = 0;
			}
		}
		
		if (rw3.count > 0)
		{
			dlOut.insert(rw3);		
		}
		ts.setTimer(filterCompare, false);

		dlOut.endOfInput();
		dlOut.totalSize(resultCount);

		ts.setTimer(filterFinish);
		while ( more1 )
		{
			more1 = dl1.next(id1, &rw1);
		}
		while ( more2 )
		{
			more2 = dl2.next(id2, &rw2);
		}
		ts.setTimer(filterFinish, false);
	}
	
}


template<typename element_t>
void FilterOperation::filter(int8_t COP, FIFO<RowWrapper<element_t> >& dl1, 
	FIFO<RowWrapper<element_t> >& dl2, DataList<element_t>& dlOut, uint64_t & resultCount, TimeSet& ts)
{

	bool constantDL = false;
      int ridcount = 0;
	// Validate that if a ConstantDataList is being used that it's the second one passed in.
	if(typeid(ConstantDataList<element_t>) == typeid(dl1)) {
		throw(std::runtime_error("FilterOperation::filter(): first DataList argument cannot be a ConstantDataList."));
	}
	if(typeid(ConstantDataList<element_t>) == typeid(dl2)) {
		RowWrapper<element_t> rw;
		int id = dl2.getIterator();
		if(!dl2.next(id, &rw)) {
			throw(std::runtime_error("FilterOperation::filter(): ConstantDataList passed does not contain any values."));
		}
		constantDL = true;
		
	}

	// Iterate over the data lists, perform the comparison, and load the output datalist with the qualifying
	// RIDs and values.
	RowWrapper<element_t> rw1, rw2;
	int id1, id2;
	id1 = dl1.getIterator();
	id2 = dl2.getIterator();		

	ts.setTimer(filterCompare);
	if(constantDL) {
		dl2.next(id2, &rw2);
		typename element_t::second_type c = rw2.et[0].second;
		while(dl1.next(id1, &rw1)) {
			for (uint64_t i = 0; i < rw1.count; ++i) {
				if(compare(COP, rw1.et[i].second, c)) {
					resultCount++;
					dlOut.insert(rw1.et[i]);				
				}
			}
		}
		dlOut.endOfInput();
		ts.setTimer(filterCompare, false);
	}
	else {
		bool more1 = dl1.next(id1, &rw1);
		bool more2 = dl2.next(id2, &rw2);
		uint64_t i=0, j=0;
		
		while(more1 && more2) {
			while (i < rw1.count && j < rw2.count) {
				if(rw1.et[i].first > rw2.et[j].first) {
					++j;
				}
				else if (rw1.et[i].first < rw2.et[j].first) {
					++i;
				}
				else {
					if(compare(COP, rw1.et[i].second, rw2.et[j].second)) {
						resultCount++;
						dlOut.insert(rw1.et[i]);
						ridcount++;
					}
					++i;
					++j;
				}
			}

			if(i == rw1.count) {
				more1 = dl1.next(id1, &rw1);
				i = 0;
			}
			if(j == rw2.count) {
				more2 = dl2.next(id2, &rw2);
				j = 0;
			}
		}
		dlOut.endOfInput();
		ts.setTimer(filterCompare, false);
	
		ts.setTimer(filterFinish);
		while ( more1 )
		{
			more1 = dl1.next(id1, &rw1);
		}
		while ( more2 )
		{
			more2 = dl2.next(id2, &rw2);
		}
		ts.setTimer(filterFinish, false);
	}
}



template<typename datavalue_t>
bool FilterOperation::compare(const int8_t COP, const datavalue_t& v1, const datavalue_t& v2) {
	switch(COP) {
		case COMPARE_GT:
			return v1 > v2;
			break;
		case COMPARE_LT:
			return v1 < v2;
			break;
		case COMPARE_EQ:
			return v1 == v2;
			break;
		case COMPARE_GE:
			return v1 >= v2;
			break;
		case COMPARE_LE:
			return v1 <= v2;
			break;
		case COMPARE_NE:
			return v1 != v2;
			break;
		default:
			return false;
			break;
	}				
}


template void FilterOperation::filter(int8_t COP,
	FIFO<RowWrapper<ElementType> >& dl1,
	FIFO<RowWrapper<ElementType> >& dl2,
	FIFO<RowWrapper<ElementType> >& dlOut,
	uint64_t & resultCount, TimeSet& ts );

template void FilterOperation::filter(int8_t COP,
	FIFO<RowWrapper<ElementType> >& dl1,
	FIFO<RowWrapper<ElementType> >& dl2,
	DataList<ElementType>& dlOut, 
	uint64_t & resultCount, TimeSet& ts);

template void FilterOperation::filter(int8_t COP,
	FIFO<RowWrapper<StringElementType> >& dl1,
	FIFO<RowWrapper<StringElementType> >& dl2,
	FIFO<RowWrapper<StringElementType> >& dlOut,
	uint64_t & resultCount, TimeSet& ts);

template void FilterOperation::filter(int8_t COP,
	FIFO<RowWrapper<StringElementType> >& dl1,
	FIFO<RowWrapper<StringElementType> >& dl2,
	DataList<StringElementType>& dlOut,
	uint64_t & resultCount, TimeSet& ts);

template void FilterOperation::filter(int8_t COP,
	FIFO<RowWrapper<DoubleElementType> >& dl1,
	FIFO<RowWrapper<DoubleElementType> >& dl2,
	FIFO<RowWrapper<DoubleElementType> >& dlOut,
	uint64_t & resultCount, TimeSet& ts);

template void FilterOperation::filter(int8_t COP,
	FIFO<RowWrapper<DoubleElementType> >& dl1,
	FIFO<RowWrapper<DoubleElementType> >& dl2,
	DataList<DoubleElementType>& dlOut,
	uint64_t & resultCount, TimeSet& ts);

}  // namespace

