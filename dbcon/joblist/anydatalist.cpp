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

// $Id: anydatalist.cpp 9655 2013-06-25 23:08:13Z xlou $

#include "elementtype.h"

namespace joblist
{
//
//...Create an index where we store a manipulator flag (for each stream)
//...that controls the inclusion of a datalist's OID in stream operations.
//
static const int showOidInDataList_Index = std::ios_base::xalloc();

/*static*/
AnyDataList::DataListTypes AnyDataList::dlType(const DataList_t* dl)
{
	if (dl == 0) return UNKNOWN_DATALIST;
//	if (typeid(*dl) == typeid(BandedDataList)) return BANDED_DATALIST;
//	if (typeid(*dl) == typeid(WorkingSetDataList)) return WORKING_SET_DATALIST;
	if (typeid(*dl) == typeid(FifoDataList)) return FIFO_DATALIST;
//	if (typeid(*dl) == typeid(BucketDataList)) return BUCKET_DATALIST;
//	if (typeid(*dl) == typeid(ConstantDataList_t)) return CONSTANT_DATALIST;
//	if (typeid(*dl) == typeid(SortedWSDL)) return SORTED_WORKING_SET_DATALIST;
//	if (typeid(*dl) == typeid(ZonedDL)) return ZONED_DATALIST;
//	if (typeid(*dl) == typeid(DeliveryWSDL)) return DELIVERYWSDL;
	if (typeid(*dl) == typeid(RowGroupDL)) return ROWGROUP_DATALIST;
	return UNKNOWN_DATALIST;
}

AnyDataList::DataListTypes AnyDataList::strDlType(const StrDataList* dl)
{
	if (dl == 0) return UNKNOWN_DATALIST;
//	if (typeid(*dl) == typeid(StringDataList)) return STRINGBANDED_DATALIST;
//	if (typeid(*dl) == typeid(StringFifoDataList)) return STRINGFIFO_DATALIST;
//	if (typeid(*dl) == typeid(StringBucketDataList)) return STRINGBUCKET_DATALIST;
	if (typeid(*dl) == typeid(StrDataList)) return STRING_DATALIST;
//	if (typeid(*dl) == typeid(StringConstantDataList_t)) return STRINGCONSTANT_DATALIST;
//	if (typeid(*dl) == typeid(StringSortedWSDL)) return STRINGSORTED_WORKING_SET_DATALIST;
//	if (typeid(*dl) == typeid(StringZonedDL)) return STRINGZONED_DATALIST;
	return UNKNOWN_DATALIST;
}

//AnyDataList::DataListTypes AnyDataList::tupleDlType(const TupleDataList* dl)
//{
//    if (dl == 0) return UNKNOWN_DATALIST;
//    if (typeid(*dl) == typeid(TupleBucketDataList)) return TUPLEBUCKET_DATALIST;
//    return UNKNOWN_DATALIST;
//}

std::ostream& operator<<(std::ostream& oss, const AnyDataListSPtr& dl)
{
	DataList_t          * dle = NULL;
	StrDataList         * dls = NULL;
//	DoubleDataList      * dld = NULL;
//	TupleBucketDataList * dlt = NULL;
	bool withOid = (oss.iword(showOidInDataList_Index) != 0);

	if ((dle = dl->dataList()) != NULL)
	{
		if (withOid)
			oss << dle->OID() << " ";

		//...If this datalist is saved to disk, then include the saved
		//...element size in the printed information.
		std::ostringstream elemSizeStr;
		if ( dle->useDisk() )
		{
			elemSizeStr << "(" << dle->getDiskElemSize1st() << "," <<
				dle->getDiskElemSize2nd() << ")";
		}

		oss << "(0x"
			<< std::hex << (ptrdiff_t)dle << std::dec << "[" <<
			AnyDataList::dlType(dle) << "]" << elemSizeStr.str() << ")";
	}
	else if ((dls = dl->stringDataList()) != NULL)
	{
		if (withOid)
			oss << dls->OID() << " ";

		//...If this datalist is saved to disk, then include the saved
		//...element size in the printed information.
		std::ostringstream elemSizeStr;
		if ( dls->useDisk() )
		{
			elemSizeStr << "(" << dls->getDiskElemSize1st() << "," <<
				dls->getDiskElemSize2nd() << ")";
		}

		oss << "(0x"
			<< std::hex << (ptrdiff_t)dls << std::dec << "[" <<
			AnyDataList::strDlType(dls) << "]" << elemSizeStr.str() << ")";
	}
//	else if ((dld = dl->doubleDL()) != NULL)
//	{
//		if (withOid)
//			oss << dld->OID() << " ";
//
//		//...If this datalist is saved to disk, then include the saved
//		//...element size in the printed information.
//		std::ostringstream elemSizeStr;
//		if ( dld->useDisk() )
//		{
//			elemSizeStr << "(" << dld->getDiskElemSize1st() << "," <<
//				dld->getDiskElemSize2nd() << ")";
//		}
//
//		oss << "(0x"
//			<< std::hex << (ptrdiff_t)dld << std::dec << "[" <<
//			AnyDataList::DOUBLE_DATALIST << "])";
//	}
//	else if ((dlt = dl->tupleBucketDL()) != NULL)
//	{
//		oss << dlt->OID() << " (0x";
//		oss << std::hex << (ptrdiff_t)dlt << std::dec << "[" << AnyDataList::TUPLEBUCKET_DATALIST << "]), ";
//	}
	else
	{
		oss << "0 (0x0000 [0])";
	}

    return oss;
}

//
//...showOidInDL is a manipulator that enables the inclusion of the data-
//...list's OID in subsequent invocations of the AnyDataListSPtr output
//...stream operator.
//
std::ostream& showOidInDL(std::ostream& strm)
{
	strm.iword(showOidInDataList_Index) = true;
	return strm;
}

//
//...omitOidInDL is a manipulator that disables the inclusion of the data-
//...list's OID in subsequent invocations of the AnyDataListSPtr output
//...stream operator.
//
std::ostream& omitOidInDL(std::ostream& strm)
{
	strm.iword(showOidInDataList_Index) = false;
	return strm;
}


}
// vim:ts=4 sw=4:

