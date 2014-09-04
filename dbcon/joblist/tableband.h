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
 * $Id: tableband.h 8436 2012-04-04 18:18:21Z rdempsey $
 *
 *****************************************************************************/

/** @file */

#ifndef JOBLIST_TABLEBAND_H_
#define JOBLIST_TABLEBAND_H_

#include <string>
#include <vector>
#include <stdexcept>
#include <iosfwd>
#include <boost/shared_ptr.hpp>

#include "calpontsystemcatalog.h"
#include "bytestream.h"
#include "datalist.h"
#include "elementtype.h"
#include "tablecolumn.h"

//#define TB_CHECK_RIDS 1
//#define TB_CHECK_STR_RIDS 1

#if defined(_MSC_VER) && defined(TABLEBAND_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace joblist
{

/** @brief used during the delivery step to project a band of rows to the front end.
 *
 * Class TableBand is used during the delivery step to project a band of rows to the front end.  
 */
class TableBand {
public:

	typedef std::vector<boost::shared_ptr<TableColumn> > VBA;

	/** @brief Constructor - used on the ExeMgr side to create a new band of rows.
	* 
	* @param shemaOwner the schema owner of the table 
	* @param tableName the table name.
	*/
	EXPORT explicit TableBand(execplan::CalpontSystemCatalog::OID tableOID=0);

	// @bug 710 - fifo read is moved here because sorting is not needed in deliverystep
	/** @brief addColumn
	*/
	EXPORT void addColumn(FifoDataList *fifo, uint it, uint32_t flushInterval, bool firstColumn, bool isExeMgr);

	/** @brief addColumn
	*/
	EXPORT void addColumn(StringFifoDataList *fifo, uint it, uint32_t flushInterval, bool firstColumn, bool isExeMgr);

	/** @brief addNullColumn
	*/
	EXPORT void addNullColumn(execplan::CalpontSystemCatalog::OID columnOID);

	/** @brief Getter for the Table OID
	*/
	EXPORT execplan::CalpontSystemCatalog::OID tableOID() const { return fTableOID; }

	/** @brief Getter for the Table OID
	*/
	EXPORT void tableOID(execplan::CalpontSystemCatalog::OID tableOID) { fTableOID = tableOID; }

	/** @brief Getter for the vector of TableColumn<kind> objects.
	*   
	*    	  The any is actually a TableColumn.  Since the data types (kind) can be different for each
	*	  column, they had to be typed as any.
	*/
	EXPORT const VBA& getColumns() const { return fColumns; }

	/** @brief Getter for the number of columns.
	*/
	EXPORT VBA::size_type getColumnCount() const { return fColumns.size(); }

	/** @brief Getter for the number of rows.
	*/
	EXPORT VBA::size_type getRowCount() const { return fRowCount; }

	/** @brief Getter for status.
	*/
	EXPORT uint16_t getStatus() const { return fStatus; }

	/** @brief Serializes the object into the passed bytestream.
	*/
	EXPORT void serialize(messageqcpp::ByteStream& b) const;

	/** @brief Inflates the object from the passed bytestream.
	*/
	EXPORT void unserialize(messageqcpp::ByteStream& b);
	
	/** @brief Converts the tableband to the passed SysDataList.
	*/
	EXPORT void convertToSysDataList(execplan::CalpontSystemCatalog::NJLSysDataList& sysDataList,
		execplan::CalpontSystemCatalog *csc);
#if 0
	EXPORT void convertToSysDataRids(execplan::CalpontSystemCatalog::NJLSysDataList& sysDataList);
#endif
	/** @brief Clears out the rows.
	*/
	EXPORT void clearRows();

	/** @brief print the band metadata to cout
	*/
	EXPORT void toString() const;

	/** @brief find a column OID in this band
	*
	* Returns the index of OID in this band. Returns -1 if the OID was not found.
	*/
	EXPORT int find(execplan::CalpontSystemCatalog::OID OID) const;

	/** @brief format a band into a string
	*
	* Formats a band of data out to a stream in ASCII using sep as the column separator. This is generally
	* suitable for import back into the system via cpimport (perhaps with some final post-processing).
	*/
	EXPORT std::ostream& formatToCSV(std::ostream& os, char sep) const;

	EXPORT std::vector<uint64_t>* getRidList() { return &fRids; }

private:
	// defaults okay
	//TableBand(const TableBand& rhs); 		// no copies
	//TableBand& operator=(const TableBand& rhs); 	// no assignments

	execplan::CalpontSystemCatalog::OID fTableOID;
	VBA fColumns;
	uint fRowCount;
	std::vector<uint64_t> fRids;
	uint numColumns;
	uint16_t fStatus;
};

#undef EXPORT

}  // namespace

#endif
