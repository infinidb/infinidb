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
 * $Id: tablecolumn.h 9655 2013-06-25 23:08:13Z xlou $
 *
 *****************************************************************************/

/** @file */

#ifndef _TABLECOLUMN_H_
#define _TABLECOLUMN_H_

#include <vector>
#include <boost/any.hpp>

#include "calpontsystemcatalog.h"
#include "bytestream.h"
#include "datalist.h"
#include "elementtype.h"

//#define TC_CHECK_RIDS 1

#if defined(_MSC_VER) && defined(JOBLIST_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace joblist
{

/** @brief create a JobList object from a CalpontExecutionPlan object
 *
 * Class TableColumn contains a column and it's values.  TableColumn objects are contained in a
 * TableBand object and used to deliver a band of rows from ExeMgr to the front end.
 */
class TableColumn {
public:

	/** @brief enum with the supported value types.
	*/
	enum supportedType
	{
		UINT8,
		UINT16,
		UINT32,
		UINT64,
		STRING,
		UNDEFINED
	};

	/** @brief constructor
	*/
	EXPORT TableColumn(const execplan::CalpontSystemCatalog::OID columnOID, const supportedType columnType);

	EXPORT TableColumn();

	/** @brief getter for the column's OID.
	*/
	inline execplan::CalpontSystemCatalog::OID getColumnOID() const { return fColumnOID; }

	/** @brief getter for the column's values.
	*/
	inline const boost::shared_ptr<std::vector<uint64_t> > getIntValues() { return fIntValues; }

	inline const boost::shared_ptr<std::vector<std::string> > getStrValues() { return fStrValues; }

	inline bool isNullColumn() const { return fIsNullColumn; }

	// pre-build the bytestream to be returned
	EXPORT void serialize();

	/** @brief serializes the object into the passed byte stream.
	*/
	EXPORT void serialize(messageqcpp::ByteStream& b);

	/** @brief inflates the object from the passed byte stream.
	*/
	EXPORT void unserialize(messageqcpp::ByteStream& b);

	/** @brief adds the column and it's values to the passed NJLSysDataList or appends the values if the column is already included in the NJLSysDataList.
	*/
	EXPORT void addToSysDataList(execplan::CalpontSystemCatalog::NJLSysDataList& sysDataList, const std::vector<uint64_t>& rids);

#if 0
	EXPORT void addToSysDataRids(execplan::CalpontSystemCatalog::NJLSysDataList& sysDataList, const std::vector<uint64_t>& rids);
#endif
	inline void setIntValues(boost::shared_ptr<std::vector<uint64_t> > sv)
	{
		fIntValues = sv;
		fIsNullColumn = fIntValues->empty();
	}

	inline void setStrValues(boost::shared_ptr<std::vector<std::string> > sv)
	{
		fStrValues = sv;
		fIsNullColumn = fStrValues->empty();
	}

	inline supportedType getColumnType() { return fColumnType; }

#ifdef TC_CHECK_RIDS
	const std::vector<uint64_t>& rids() const { return fRids; }
#endif

private:
	execplan::CalpontSystemCatalog::OID fColumnOID;
	boost::shared_ptr<std::vector <uint64_t> > fIntValues;
	boost::shared_ptr<std::vector <std::string> > fStrValues;
	bool fIsNullColumn;
	supportedType fColumnType;
	boost::shared_ptr<messageqcpp::ByteStream> preserialized;
#ifdef TC_CHECK_RIDS
	std::vector<uint64_t> fRids;
#endif

	// defaults okay
	//TableColumn(const TableColumn& rhs); 			// no copies
	//TableColumn& operator=(const TableColumn& rhs); 	// no assignments
};

#undef EXPORT

}  // namespace

#endif

