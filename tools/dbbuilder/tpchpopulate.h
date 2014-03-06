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

/******************************************************************************************
* $Id: tpchpopulate.h 2101 2013-01-21 14:12:52Z rdempsey $
*
******************************************************************************************/
/**
 * @file
 */
#ifndef TPCHPOPULATE_H
#define TPCHPOPULATE_H

class TpchPopulate
{

public:
    void populate_part();
	void populate_customer();
	void populate_tpch();
	void populateFromFile(std::string tableName, std::string& fileName);
protected:


private:
    void insert(std::string insertStmt);

};

#endif //TPCHPOPULATE_H
