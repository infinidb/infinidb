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

/***********************************************************************
*   $Id: calpontsystemcatalog.h 9210 2013-01-21 14:10:42Z rdempsey $
*
*
***********************************************************************/
/** @file */

#ifndef DBCON_CALPONTSYSTEMCATALOG_H
#define DBCON_CALPONTSYSTEMCATALOG_H

namespace dbcon {

class CalpontSystemCatalog
{
public:
	/** looks up a column's TCN in the System Catalog
	 *
	 * For a unique table_name.column_name return the internal TCN
	 */
	TCN lookupTCN(const TableColName& tableColName) const;

	/** returns the colunm type attribute(s) for a column
	 *
	 * return the various colunm attributes for a given TCN:
	 *    dictionary/native
	 *    DDN
	 */
	ColType colType(const TCN& tcn) const;

	/** return the current SCN
	 *
	 * returns the current System Change Number (for versioning support)
	 */
	SCN scn(void) const;
};

} //namespace dbcon

#endif //DBCON_CALPONTSYSTEMCATALOG_H

