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

/*
 * $Id: elementtype.h 9655 2013-06-25 23:08:13Z xlou $
 */
/** @file */

#ifndef JOBLIST_ELEMENTTYPE_H_
#define JOBLIST_ELEMENTTYPE_H_

#include <iostream>
#include <utility>
#include <string>
#include <stdexcept>
#include <boost/shared_array.hpp>
#include <stdint.h>
#include <rowgroup.h>

#ifndef __GNUC__
#  ifndef __attribute__
#    define __attribute__(x)
#  endif
#endif

namespace joblist
{

/** @brief struct ElementType
 *
 */

struct ElementType
{
	typedef uint64_t first_type;
	typedef uint64_t second_type;
	uint64_t first;
	uint64_t second;

	ElementType() : first(static_cast<uint64_t>(-1)),
		second(static_cast<uint64_t>(-1))
		{ };
	ElementType(uint64_t f, uint64_t s) : first(f), second(s) { };

	const char * getHashString(uint64_t mode, uint64_t *len) const {
		switch (mode) {
			case 0:
				*len = 8;
				return (char *) &first;
			case 1:
				*len = 8;
				return (char *) &second;
			default:
				throw std::logic_error("ElementType: invalid mode in getHashString().");
		}
	}
	inline bool operator<(const ElementType &e) const { return (first < e.first); }
	const std::string toString() const;
};

/** @brief struct StringElementType
 *
 */
struct StringElementType
{
	typedef uint64_t first_type;
	typedef std::string second_type;

	uint64_t first;
	std::string second;

	StringElementType();
	StringElementType(uint64_t f, const std::string &s);

	const char * getHashString(uint64_t mode, uint64_t *len) const {
		switch (mode) {
			case 0:
				*len = sizeof(first);
				return (char *) &first;
			case 1:
				*len = second.size();
				return (char *) second.data();
			default:
				throw std::logic_error("StringElementType: invalid mode in getHashString().");
		}
	}
	inline bool operator<(const StringElementType &e) const { return (first < e.first); }

};

/** @brief struct DoubleElementType
 *
 */
struct DoubleElementType
{

	uint64_t first;
	double second;

	DoubleElementType();
	DoubleElementType(uint64_t f, double s);

	typedef double second_type;
	const char * getHashString(uint64_t mode, uint64_t *len) const {
		switch (mode) {
			case 0:
				*len = sizeof(first);
				return (char *) &first;
			case 1:
				*len = sizeof(second);
				return (char *) &second;
			default:
				throw std::logic_error("StringElementType: invalid mode in getHashString().");
		}
	}
	inline bool operator<(const DoubleElementType &e) const { return (first < e.first); }
};

template<typename element_t>
struct RowWrapper
{
	uint64_t count;
	static const uint64_t ElementsPerGroup = 8192;
	element_t et[8192];

	RowWrapper():count(0)
	{
	}

	inline RowWrapper(const RowWrapper &rg) : count(rg.count)
	{
		for (uint32_t i = 0; i < count; ++i)
			et[i] = rg.et[i];
	}

	~RowWrapper()
	{
	}

	inline RowWrapper & operator=(const RowWrapper &rg)
	{
		count = rg.count;
		for (uint32_t i = 0; i < count; ++i)
			et[i] = rg.et[i];
		return *this;
	}
};

/** @brief struct RIDElementType
 *
 */
struct RIDElementType
{
	uint64_t first;

	RIDElementType();
	RIDElementType(uint64_t f);

	const char * getHashString(uint64_t mode, uint64_t *len) const {
		*len = 8;
		return (char *) &first;
	}

	bool operator<(const RIDElementType &e) const { return (first < e.first); }
};

/** @brief struct TupleType
 *
 * first: rid
 * second: data value in unstructured format
 */

struct TupleType
{
    uint64_t first;
    char* second;
    TupleType() {}
    TupleType (uint64_t f, char* s):first(f), second(s){}

    /** @brief delete a tuple
     *
     * this function should be called by the tuple user outside
     * the datalist if necessary
     */
    void deleter()
    {
        delete [] second;
    }

    /** @brief get hash string
     * @note params mode and len are ignored here. they are carried
     * just to keep a consistent interface with the other element type
     */
    const char * getHashString(uint64_t mode, uint64_t *len) const
    {
        return (char*)second;
    }

    bool operator<(const TupleType &e) const { return (first < e.first); }
};

typedef RowWrapper<ElementType> UintRowGroup;
typedef RowWrapper<StringElementType> StringRowGroup;
typedef RowWrapper<DoubleElementType> DoubleRowGroup;

extern std::istream& operator>>(std::istream& in, ElementType& rhs);
extern std::ostream& operator<<(std::ostream& out, const ElementType& rhs);
extern std::istream& operator>>(std::istream& in, StringElementType& rhs);
extern std::ostream& operator<<(std::ostream& out, const StringElementType& rhs);
extern std::istream& operator>>(std::istream& in, DoubleElementType& rhs);
extern std::ostream& operator<<(std::ostream& out, const DoubleElementType& rhs);
extern std::istream& operator>>(std::istream& in, RIDElementType& rhs);
extern std::ostream& operator<<(std::ostream& out, const RIDElementType& rhs);
extern std::istream& operator>>(std::istream& in, TupleType& rhs);
extern std::ostream& operator<<(std::ostream& out, const TupleType& rhs);
}

#ifndef NO_DATALISTS

//#include "bandeddl.h"
//#include "wsdl.h"
#include "fifo.h"
//#include "bucketdl.h"
//#include "constantdatalist.h"
//#include "swsdl.h"
//#include "zdl.h"
//#include "deliverywsdl.h"

namespace joblist {

///** @brief type BandedDataList
// *
// */
//typedef BandedDL<ElementType> BandedDataList;
///** @brief type StringDataList
// *
// */
//typedef BandedDL<StringElementType> StringDataList;
/** @brief type StringFifoDataList
 *
 */
//typedef FIFO<StringElementType> StringFifoDataList;
typedef FIFO<StringRowGroup> StringFifoDataList;
///** @brief type StringBucketDataList
// *
// */
//typedef BucketDL<StringElementType> StringBucketDataList;
///** @brief type WorkingSetDataList
// *
// */
//typedef WSDL<ElementType> WorkingSetDataList;
/** @brief type FifoDataList
 *
 */
//typedef FIFO<ElementType> FifoDataList;
typedef FIFO<UintRowGroup> FifoDataList;
///** @brief type BucketDataList
// *
// */
//typedef BucketDL<ElementType> BucketDataList;
///** @brief type ConstantDataList_t
// *
// */
//typedef ConstantDataList<ElementType> ConstantDataList_t;
///** @brief type StringConstantDataList_t
// *
// */
//typedef ConstantDataList<StringElementType> StringConstantDataList_t;
/** @brief type DataList_t
 *
 */
typedef DataList<ElementType> DataList_t;
/** @brief type StrDataList
 *
 */
typedef DataList<StringElementType> StrDataList;
///** @brief type DoubleDataList
// *
// */
//typedef DataList<DoubleElementType> DoubleDataList;
///** @brief type TupleDataList
// *
// */
//typedef DataList<TupleType> TupleDataList;
///** @brief type SortedWSDL
// *
// */
//typedef SWSDL<ElementType> SortedWSDL;
///** @brief type StringSortedWSDL
// *
// */
//typedef SWSDL<StringElementType> StringSortedWSDL;
///** @brief type ZonedDL
// *
// */
//typedef ZDL<ElementType> ZonedDL;
///** @brief type StringZonedDL
// *
// */
//typedef ZDL<StringElementType> StringZonedDL;
//
///** @brief type TupleBucketDL
// *
// */
//typedef BucketDL<TupleType> TupleBucketDataList;

typedef FIFO<rowgroup::RGData> RowGroupDL;

}

#include <vector>
#include <boost/shared_ptr.hpp>

namespace joblist
{
/** @brief class AnyDataList
 *
 */
class AnyDataList
{
public:
	AnyDataList() : fDl3(0), fDl6(0), fDl9(0), fDisown(false) { }
	~AnyDataList() { if (!fDisown) { delete fDl3; delete fDl6; delete fDl9; } }

//	AnyDataList() : fDl1(0), fDl2(0), fDl3(0), fDl4(0), fDl5(0), fDl6(0), fDl7(0), fDl8(0), fDl9(0),
//		fDl10(0), fDl11(0), fDl12(0), fDl13(0), fDl14(0), fDl15(0), fDl16(0), fDl17(0), fDl18(0),
//		fDl19(0), fDl20(0), fDisown(false) { }
//	~AnyDataList() { if (!fDisown) { delete fDl1; delete fDl2; delete fDl3; delete fDl4;
//		delete fDl5; delete fDl6; delete fDl7; delete fDl8; delete fDl9; delete fDl10; delete fDl11;
//		delete fDl12; delete fDl13; delete fDl14; delete fDl15; delete fDl16; delete fDl17;
//		delete fDl18; delete fDl19; delete fDl20; } }

	// disown() fixes the problem of multiple ownership of a single DL,
	// or one on the stack

	//In the world of bad ideas these are at the top. The whole point of this class is to manage
	// dynamically allocated data in an automatic way. These 2 methods circumvent this, and they
	// are not necessary in any event, because you can safely share AnyDataList's via a AnyDataListSPtr.
	inline void disown() __attribute__ ((deprecated)) { fDisown = true; }
	inline void posess() __attribute__ ((deprecated)) { fDisown = false; }

//	inline void bandedDL(BandedDataList* dl) { fDl1 = dl; }
//	inline BandedDataList* bandedDL() { return fDl1; }
//	inline const BandedDataList* bandedDL() const { return fDl1; }
//
//	inline void workingSetDL(WorkingSetDataList* dl) { fDl2 = dl; }
//	inline WorkingSetDataList* workingSetDL() { return fDl2; }
//	inline const WorkingSetDataList* workingSetDL() const { return fDl2; }
//
	inline void fifoDL(FifoDataList* dl) { fDl3 = dl; }
	inline FifoDataList* fifoDL() { return fDl3; }
	inline const FifoDataList* fifoDL() const { return fDl3; }
//
//	inline void bucketDL(BucketDataList* dl) { fDl4 = dl; }
//	inline BucketDataList* bucketDL() { return fDl4; }
//	inline const BucketDataList* bucketDL() const { return fDl4; }
//
//	inline void constantDL(ConstantDataList_t* dl) { fDl5 = dl; }
//	inline ConstantDataList_t* constantDL() { return fDl5; }
//	inline const ConstantDataList_t* constantDL() const { return fDl5; }
//
//	inline void sortedWSDL(SortedWSDL* dl) { fDl13 = dl; }
//	inline SortedWSDL* sortedWSDL() { return fDl13; }
//	inline const SortedWSDL* sortedWSDL() const { return fDl13; }
//
//	inline void zonedDL(ZonedDL* dl) { fDl15 = dl; }
//	inline ZonedDL* zonedDL() { return fDl15; }
//	inline const ZonedDL* zonedDL() const { return fDl15; }
//
	inline void stringDL(StringFifoDataList* dl) { fDl6 = dl; }
	inline StringFifoDataList* stringDL() { return fDl6; }
	inline const StringFifoDataList* stringDL() const { return fDl6; }
//
//	inline void stringBandedDL(StringDataList* dl) { fDl10 = dl; }
//	inline StringDataList* stringBandedDL() { return fDl10; }
//	inline const StringDataList* stringBandedDL() const { return fDl10; }
//
//	inline void stringBucketDL(StringBucketDataList* dl) { fDl11 = dl; }
//	inline StringBucketDataList* stringBucketDL() { return fDl11; }
//	inline const StringBucketDataList* stringBucketDL() const { return fDl11; }
//
//	inline void stringConstantDL(StringConstantDataList_t* dl) { fDl12 = dl; }
//	inline StringConstantDataList_t* stringConstantDL() { return fDl12; }
//	inline const StringConstantDataList_t* stringConstantDL() const { return fDl12; }
//
//	inline void stringSortedWSDL(StringSortedWSDL* dl) { fDl14 = dl; }
//	inline StringSortedWSDL* stringSortedWSDL() { return fDl14; }
//	inline const StringSortedWSDL* stringSortedWSDL() const { return fDl14; }
//
//	inline void stringZonedDL(StringZonedDL* dl) { fDl16 = dl; }
//	inline StringZonedDL* stringZonedDL() { return fDl16; }
//	inline const StringZonedDL* stringZonedDL() const { return fDl16; }
//
//	inline void tupleBucketDL(TupleBucketDataList* dl) { fDl18 = dl; }
//	inline TupleBucketDataList* tupleBucketDL() { return fDl18; }
//	inline const TupleBucketDataList* tupleBucketDL() const { return fDl18; }
//
//	inline void deliveryWSDL(DeliveryWSDL *dl) { fDl19 = dl; }
//	inline DeliveryWSDL * deliveryWSDL() { return fDl19; }
//	inline const DeliveryWSDL * deliveryWSDL() const { return fDl19; }

	inline void rowGroupDL(boost::shared_ptr<RowGroupDL> dl) { fDl20 = dl; }
	inline void rowGroupDL(RowGroupDL *dl) { fDl20.reset(dl); }
	inline RowGroupDL * rowGroupDL() { return fDl20.get(); }
	inline const RowGroupDL * rowGroupDL() const { return fDl20.get(); }

	DataList_t* dataList() {
		if (fDl3 != NULL) return reinterpret_cast<DataList_t*>(fDl3);
		else if (fDl9 != NULL) return fDl9;
		return reinterpret_cast<DataList_t*>(fDl20.get());
//		if (fDl1 != NULL) return fDl1;
//		else if (fDl2 != NULL) return fDl2;
//		else if (fDl3 != NULL) return reinterpret_cast<DataList_t*>(fDl3);
//		else if (fDl4 != NULL) return fDl4;
//		else if (fDl9 != NULL) return fDl9;
//		else if (fDl13 != NULL) return fDl13;
//		else if (fDl15 != NULL) return fDl15;
//		else if (fDl19 != NULL) return fDl19;
//		else if (fDl20 != NULL) return reinterpret_cast<DataList_t*>(fDl20);
//		else return fDl5;
	}
//
	StrDataList* stringDataList() {
//		if (fDl6 != NULL) return reinterpret_cast<StrDataList*>(fDl6);
//		else if (fDl10 != NULL) return fDl10;
//		else if (fDl11 != NULL) return fDl11;
//		else if (fDl12 != NULL) return fDl12;
//		else if (fDl14 != NULL) return fDl14;
//		else if (fDl16 != NULL) return fDl16;
//		return fDl8;
		return reinterpret_cast<StrDataList*>(fDl6);
	}
//
//	TupleDataList* tupleDataList() {
//		if (fDl18 != NULL) return fDl18;
//		return fDl17;
//	}
//
//	/* fDl{7,8} store base class pointers.  For consistency, maybe strDataList
//	   should consider fDl6 also. */
//	inline StrDataList * strDataList()
//	{ return fDl8; }
//
//	inline void strDataList(StrDataList *d)
//	{ fDl8 = d; }
//
//	inline DoubleDataList * doubleDL()
//	{ return fDl7; }
//
//	inline void doubleDL(DoubleDataList *d)
//	{ fDl7 = d; }

	enum DataListTypes
	{
		UNKNOWN_DATALIST,                   /*!<  0 Unknown DataList */
		BANDED_DATALIST,                    /*!<  1 Banded DataList */
		WORKING_SET_DATALIST,               /*!<  2 WSDL */
		FIFO_DATALIST,                      /*!<  3 FIFO */
		BUCKET_DATALIST,                    /*!<  4 Bucket */
		CONSTANT_DATALIST,                  /*!<  5 Constant */
		STRING_DATALIST,                    /*!<  6 String */
		DOUBLE_DATALIST,                    /*!<  7 Double */
		STRINGFIFO_DATALIST,                /*!<  8 String FIFO */
		STRINGBANDED_DATALIST,              /*!<  9 String Banded */
		STRINGBUCKET_DATALIST,              /*!< 10 String Bucket */
		STRINGCONSTANT_DATALIST,            /*!< 11 String Constant */
		SORTED_WORKING_SET_DATALIST,        /*!< 12 Sorted WSDL */
		STRINGSORTED_WORKING_SET_DATALIST,  /*!< 13 String Sorted WSDL */
		ZONED_DATALIST,                     /*!< 14 Zoned Datalist */
		STRINGZONED_DATALIST,               /*!< 15 String Zoned Datalist */
		TUPLEBUCKET_DATALIST,               /*!< 16 Tuple Bucket Datalist */
		TUPLE_DATALIST,                     /*!< 17 Tuple Datalist */
		DELIVERYWSDL,                       /*!< 18 Delivery WSDL */
		ROWGROUP_DATALIST
	};

	static DataListTypes dlType(const DataList_t* dl);
	static DataListTypes strDlType(const StrDataList* dl);
//	static DataListTypes tupleDlType(const TupleDataList* dl);
	uint32_t getNumConsumers()
	{
//	    if (fDl1 != NULL) return fDl1->getNumConsumers();
//		else if (fDl2 != NULL) return fDl2->getNumConsumers();
//		else if (fDl3 != NULL) return fDl3->getNumConsumers();
//		else if (fDl6 != NULL) return fDl6->getNumConsumers();
//		else if (fDl10 != NULL) return fDl10->getNumConsumers();
//		else if (fDl13 != NULL) return fDl13->getNumConsumers();
//		else if (fDl14 != NULL) return fDl14->getNumConsumers();
//		else if (fDl15 != NULL) return fDl15->getNumConsumers();
//		else if (fDl16 != NULL) return fDl16->getNumConsumers();
//		else if (fDl4 != NULL) return 1;
//		else if (fDl11 != NULL) return 1;
//		else if (fDl18 != NULL) return 1;
//		else if (fDl19 != NULL) return fDl19->getNumConsumers();
//		else if (fDl20 != NULL) return 1;
//		else return 0;

		if (fDl20) return 1;
		else if (fDl3 != NULL) return fDl3->getNumConsumers();
		else if (fDl6 != NULL) return fDl6->getNumConsumers();
		return 0;
	}

	//There is no operator==() because 2 AnyDataList's are equal if they point to the same DL, but the only way
	// that could be is if they are the _same_ AnyDatalist, since, by convention, AnyDataList's are only
	// moved around as shared_ptr's (AnyDataListSPtr). Indeed, it is an error if two different AnyDataList
	// objects point to the same DL.
	// bool operator==(const AnyDataList& rhs);

private:
	AnyDataList(const AnyDataList& rhs);
	AnyDataList& operator=(const AnyDataList& rhs);

//	BandedDataList* fDl1;
//	WorkingSetDataList* fDl2;
	FifoDataList* fDl3;
//	BucketDataList* fDl4;
//	ConstantDataList_t* fDl5;
	StringFifoDataList* fDl6;
//	DoubleDataList* fDl7;
//	StrDataList* fDl8;
	DataList_t* fDl9;
//	StringDataList* fDl10;
//	StringBucketDataList* fDl11;
//	StringConstantDataList_t* fDl12;
//	SortedWSDL* fDl13;
//	StringSortedWSDL* fDl14;
//	ZonedDL* fDl15;
//	StringZonedDL* fDl16;
//	TupleDataList* fDl17;
//	TupleBucketDataList *fDl18;
//	DeliveryWSDL *fDl19;
	boost::shared_ptr<RowGroupDL> fDl20;
	bool fDisown;

};

/** @brief type AnyDataListSPtr
 *
 */
typedef boost::shared_ptr<AnyDataList> AnyDataListSPtr;
/** @brief type DataListVec
 *
 */
typedef std::vector<AnyDataListSPtr> DataListVec;

extern std::ostream& operator<<(std::ostream& os, const AnyDataListSPtr& dl);

//
//...Manipulators for controlling the inclusion of the datalist's
//...OID in the AnyDataListSPtr's output stream operator.
//
extern std::ostream& showOidInDL ( std::ostream& strm );
extern std::ostream& omitOidInDL ( std::ostream& strm );

}

#endif

#endif
// vim:ts=4 sw=4:

